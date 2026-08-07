package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
	asynctask "github.com/syncgo/syncgo/pkg/async_task"
	"github.com/syncgo/syncgo/pkg/postgresql"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	standbyMessageTimeout = time.Second * 10
)

type RowBatcher interface {
	Add(item bulk_transformer.Data)
	CommitN(n int)
	RollbackN(n int)
}

type LogicalRepicationConfig struct {
	PublicationName string
	SlotName        string
	IDColumn        string
	DB              postgresql.Config
}

type LogicalReplicationConn struct {
	wg sync.WaitGroup
	mu sync.Mutex

	conn     *pgconn.PgConn
	slotName string

	plugin     string
	pluginArgs []string

	xLogPos  pglogrepl.LSN
	txn      txnTracker
	idColumn string

	batcher RowBatcher

	standbyMessageTimeout time.Duration
	standbyMessageTask    *asynctask.AsyncTask
}

func New(ctx context.Context, cfg LogicalRepicationConfig, conn *pgconn.PgConn, batcher RowBatcher) (*LogicalReplicationConn, error) {
	replicationConn := &LogicalReplicationConn{
		wg:                    sync.WaitGroup{},
		conn:                  conn,
		slotName:              cfg.SlotName,
		batcher:               batcher,
		standbyMessageTimeout: standbyMessageTimeout,
	}

	if cfg.IDColumn == "" {
		replicationConn.idColumn = "id"
	}

	if err := replicationConn.validateReplicationSetup(ctx, cfg); err != nil {
		return nil, err
	}

	// if err := replicationConn.dropPublication(ctx); err != nil {
	// 	return nil, err
	// }
	//
	// if err := replicationConn.createPublication(ctx); err != nil {
	// 	return nil, err
	// }

	replicationConn.plugin = "pgoutput"
	replicationConn.pluginArgs = replicationConn.buildPluginArgs()

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		slog.Error("failed to identify system", slog.String("err", err.Error()))
		return nil, err
	}
	replicationConn.xLogPos = sysident.XLogPos

	slog.Info("successfully indentify system",
		slog.String("sysetmID", sysident.SystemID),
		slog.Int64("timeline", int64(sysident.Timeline)),
		slog.String("xLogPos", sysident.XLogPos.String()),
		slog.String("dbName", sysident.DBName),
	)

	replicationConn.standbyMessageTask = asynctask.New(standbyMessageTimeout, func(ctx context.Context) {
		if err := replicationConn.sendStandbyStatusUpdate(ctx); err != nil {
			slog.Error("failed to send standby status update", slog.String("err", err.Error()))
		}
	})
	replicationConn.standbyMessageTask.Start(ctx)

	return replicationConn, nil
}

func (c *LogicalReplicationConn) buildPluginArgs() []string {
	return []string{
		"proto_version '1'",
		"publication_names 'pglogrepl_demo'",
		"messages 'false'",
	}
}

func (c *LogicalReplicationConn) StartReplication(ctx context.Context) error {
	if err := c.startReplication(ctx, c.slotName, c.xLogPos, c.pluginArgs); err != nil {
		return err
	}

	relations := map[uint32]*pglogrepl.RelationMessage{}
	typeMap := pgtype.NewMap()

	c.wg.Go(func() {
		if err := c.readReplicationMessages(ctx, relations, typeMap); err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error("failed to read replication messages", slog.String("err", err.Error()))
			}
		}
	})

	return nil
}

func (c *LogicalReplicationConn) Close() error {
	c.wg.Wait()

	return c.conn.Close(context.Background())
}

//func (c *LogicalReplicationConn) createPublication(ctx context.Context) error {
//	result := c.conn.Exec(ctx, "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;")
//	if _, err := result.ReadAll(); err != nil {
//		slog.Error("failed to create publication", slog.String("err", err.Error()))
//		return err
//	}
//
//	return nil
//}

//func (c *LogicalReplicationConn) dropPublication(ctx context.Context) error {
//	result := c.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS pglogrepl_demo;")
//	if _, err := result.ReadAll(); err != nil {
//		slog.Error("failed to create publication", slog.String("err", err.Error()))
//		return err
//	}
//
//	return nil
//}

func (c *LogicalReplicationConn) createReplicationSlot(ctx context.Context, slotName string, temporary bool) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, c.conn, slotName, c.plugin, pglogrepl.CreateReplicationSlotOptions{Temporary: temporary})
	if err != nil {
		slog.Error("failed to create slot", slog.String("err", err.Error()))
		return err
	}

	slog.Info("created replication slot", slog.String("name", slotName), slog.Bool("temporary", temporary))

	return nil
}

func (c *LogicalReplicationConn) startReplication(ctx context.Context, slotName string, xLogPos pglogrepl.LSN, pluginArgs []string) error {
	if err := pglogrepl.StartReplication(ctx, c.conn, slotName, xLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		slog.Error("failed to start replication", slog.String("err", err.Error()))
		return err
	}

	slog.Info("logical replication started", slog.String("slotName", slotName))

	return nil
}

// func (c *LogicalReplicationConn) stopReplication(ctx context.Context, slotName string) error {
// 	if err := pglogrepl.StopReplication(ctx, c.conn, slotName, pglogrepl.StopReplicationOptions{}); err != nil {
// 		log.Fatalln("StopReplication failed:", err)
// 		slog.Error("failed to stop replication", slog.String("err", err.Error()))
// 		return err
// 	}

// 	return nil
// }

func (c *LogicalReplicationConn) readReplicationMessages(ctx context.Context, relations map[uint32]*pglogrepl.RelationMessage, typeMap *pgtype.Map) error {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			slog.Error("context done, stopping read replication messages")
			return ctx.Err()
		default:
			rawMsg, err := c.conn.ReceiveMessage(ctx)
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				slog.Error("failed to receive message", slog.String("err", err.Error()))
				return err
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				slog.Error("received Postgres WAL error", slog.Any("error", errMsg))
				return fmt.Errorf("postgres WAL error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				slog.Warn("received unexpected message", slog.String("type", fmt.Sprintf("%T", rawMsg)))
				slog.Debug("received message", slog.String("message", fmt.Sprintf("%+v", rawMsg)))
				continue
			}

			slog.Debug("received copy data message", slog.String("byte", string(msg.Data[0])))
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					slog.Error("failed to parse primary keepalive message", slog.String("err", err.Error()))
					return err
				}
				slog.Debug("primary keepalive message",
					slog.String("serverWALEnd", pkm.ServerWALEnd.String()),
					slog.Time("serverTime", pkm.ServerTime),
					slog.Bool("replyRequested", pkm.ReplyRequested),
				)

				c.mu.Lock()
				clientXLogPos := c.xLogPos
				c.mu.Unlock()

				if pkm.ServerWALEnd > clientXLogPos {
					c.mu.Lock()
					c.xLogPos = pkm.ServerWALEnd
					c.mu.Unlock()
				}

				if pkm.ReplyRequested {
					if err := c.sendStandbyStatusUpdate(ctx); err != nil {
						return err
					}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					slog.Error("failed to parse xlog data", slog.String("err", err.Error()))
					return err
				}

				slog.Debug("xlog data",
					slog.String("walStart", xld.WALStart.String()),
					slog.String("serverWALEnd", xld.ServerWALEnd.String()),
					slog.Time("serverTime", xld.ServerTime),
				)

				c.process(xld.WALData, relations, typeMap)

				c.mu.Lock()
				clientXLogPos := c.xLogPos
				c.mu.Unlock()

				if xld.WALStart > clientXLogPos {
					c.mu.Lock()
					c.xLogPos = xld.WALStart
					c.mu.Unlock()
				}
			}
		}
	}

	return ctx.Err()
}

func (c *LogicalReplicationConn) sendStandbyStatusUpdate(ctx context.Context) error {
	c.mu.Lock()
	clientXLogPos := c.xLogPos
	c.mu.Unlock()

	if err := pglogrepl.SendStandbyStatusUpdate(ctx, c.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos}); err != nil {
		slog.Error("failed to send standby status update", slog.String("err", err.Error()))
		return err
	}

	slog.Debug("sent standby status update", slog.Any("walWritePosition", clientXLogPos))

	return nil
}

func (c *LogicalReplicationConn) validateReplicationSetup(ctx context.Context, cfg LogicalRepicationConfig) error {
	adminConn, err := postgresql.NewStandard(ctx, cfg.DB)
	if err != nil {
		return fmt.Errorf("failed to connect for replication setup validation: %w", err)
	}
	defer func() {
		if closeErr := adminConn.Close(ctx); closeErr != nil {
			slog.Error("failed to close admin connection", slog.String("err", closeErr.Error()))
		}
	}()

	slotExists, err := postgresql.ReplicationSlotExists(ctx, adminConn, cfg.SlotName)
	if err != nil {
		return fmt.Errorf("failed to check replication slot %q: %w", cfg.SlotName, err)
	}
	if !slotExists {
		slog.Debug("replication slot does not exist", slog.String("slotName", cfg.SlotName))
		if err = c.createReplicationSlot(ctx, cfg.SlotName, true); err != nil {
			return err
		}
	}

	// pubExists, err := postgresql.PublicationExists(ctx, adminConn, cfg.PublicationName)
	// if err != nil {
	// 	return fmt.Errorf("failed to check publication %q: %w", cfg.PublicationName, err)
	// }
	// if !pubExists {
	// 	slog.Debug("publication does not exist", slog.String("publicationName", cfg.PublicationName))
	// 	if err = c.createPublication(ctx); err != nil {
	// 		return err
	// 	}
	// 	return fmt.Errorf("publication %q does not exist", cfg.PublicationName)
	// }

	return nil
}
