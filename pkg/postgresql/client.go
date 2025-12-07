package postgresql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type Config struct {
	User     string
	Password string
	Host     string
	Port     string
	Database string
}

type LogicalReplicationConn struct {
	wg sync.WaitGroup

	conn     *pgconn.PgConn
	slotName string

	plugin     string
	pluginArgs []string
	inStream   bool
	v2         bool

	xLogPos               pglogrepl.LSN
	standbyMessageTimeout time.Duration
}

const (
	pgOutputPlugin = "pgoutput"
)

func New(ctx context.Context, cfg Config) (*LogicalReplicationConn, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?replication=database",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	pgCfg, err := pgconn.ParseConfig(url)
	if err != nil {
		slog.Error("failed to parse postgres config", slog.String("error", err.Error()))

		return nil, err
	}

	conn, err := pgconn.ConnectConfig(ctx, pgCfg)
	if err != nil {
		slog.Error("pool constructor failed", slog.String("error", err.Error()))

		return nil, err
	}

	replicationConn := &LogicalReplicationConn{
		wg:                    sync.WaitGroup{},
		conn:                  conn,
		slotName:              "pglogrepl_demo",
		plugin:                pgOutputPlugin,
		v2:                    true,
		inStream:              false,
		standbyMessageTimeout: time.Second * 10,
	}

	// if err := replicationConn.dropPublication(ctx); err != nil {
	// 	return nil, err
	// }
	//
	// if err := replicationConn.createPublication(ctx); err != nil {
	// 	return nil, err
	// }

	switch replicationConn.plugin {
	case pgOutputPlugin:
		replicationConn.pluginArgs = []string{
			"proto_version '2'",
			"publication_names 'pglogrepl_demo'",
			"messages 'true'",
			"streaming 'true'",
		}
	}

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

	if err := replicationConn.createReplicationSlot(ctx, replicationConn.slotName, true); err != nil {
		return nil, err
	}

	return replicationConn, nil
}

func (c *LogicalReplicationConn) StartReplication(ctx context.Context) error {
	if err := c.startReplication(ctx, c.slotName, c.xLogPos, c.pluginArgs); err != nil {
		return err
	}

	clientXLogPos := c.xLogPos
	relations := map[uint32]*pglogrepl.RelationMessage{}
	relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
	typeMap := pgtype.NewMap()

	c.wg.Go(func() {
		if err := c.readReplicationMessages(ctx, clientXLogPos, relations, relationsV2, typeMap); err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error("failed to read replication messages", slog.String("err", err.Error()))
			}
		}
	})

	return nil
}

func (c *LogicalReplicationConn) Close() error {
	c.wg.Wait()

	if err := c.conn.Close(context.Background()); err != nil {
		slog.Error("failed to close connection", slog.String("err", err.Error()))
		return err
	}

	return nil
}

func (c *LogicalReplicationConn) createPublication(ctx context.Context) error {
	result := c.conn.Exec(ctx, "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;")
	if _, err := result.ReadAll(); err != nil {
		slog.Error("failed to create publication", slog.String("err", err.Error()))
		return err
	}

	return nil
}

func (c *LogicalReplicationConn) dropPublication(ctx context.Context) error {
	result := c.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS pglogrepl_demo;")
	if _, err := result.ReadAll(); err != nil {
		slog.Error("failed to create publication", slog.String("err", err.Error()))
		return err
	}

	return nil
}

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

func (c *LogicalReplicationConn) readReplicationMessages(ctx context.Context, clientXLogPos pglogrepl.LSN, relations map[uint32]*pglogrepl.RelationMessage, relationsV2 map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map) error {
	standbyStatusTicker := time.NewTicker(c.standbyMessageTimeout)
	defer standbyStatusTicker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			slog.Error("context done, stopping read replication messages")
			return ctx.Err()
		case <-standbyStatusTicker.C:
			if err := c.sendStandbyStatusUpdate(ctx, clientXLogPos); err != nil {
				slog.Error("failed to send standby status update", slog.String("err", err.Error()))
				return err
			}
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
				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					if err := c.sendStandbyStatusUpdate(ctx, clientXLogPos); err != nil {
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
				if c.v2 {
					processV2(xld.WALData, relationsV2, typeMap, &c.inStream)
				} else {
					processV1(xld.WALData, relations, typeMap)
				}

				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}
			}
		}
	}

	return ctx.Err()
}

func (c *LogicalReplicationConn) sendStandbyStatusUpdate(ctx context.Context, clientXLogPos pglogrepl.LSN) error {
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, c.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos}); err != nil {
		slog.Error("failed to send standby status update", slog.String("err", err.Error()))
		return err
	}

	slog.Debug("sent standby status update", slog.Any("walWritePosition", clientXLogPos))

	return nil
}

func processV2(walData []byte, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, inStream *bool) {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		slog.Error("failed to parse logical replication message", slog.String("err", err.Error()))
		return
	}
	slog.Debug("received logical replication message", slog.String("type", logicalMsg.Type().String()))
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessageV2:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			slog.Error("unknown relation ID", slog.Uint64("relationID", uint64(logicalMsg.RelationID)))
			return
		}
		values := map[string]any{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					slog.Error("error decoding column data", slog.String("err", err.Error()))
					return
				}
				values[colName] = val
			}
		}
		slog.Info("insert operation",
			slog.Uint64("xid", uint64(logicalMsg.Xid)),
			slog.String("namespace", rel.Namespace),
			slog.String("relationName", rel.RelationName),
			slog.Any("values", values),
		)

	case *pglogrepl.UpdateMessageV2:
		slog.Info("update operation", slog.Uint64("xid", uint64(logicalMsg.Xid)))
		// ...
	case *pglogrepl.DeleteMessageV2:
		slog.Info("delete operation", slog.Uint64("xid", uint64(logicalMsg.Xid)))
		// ...
	case *pglogrepl.TruncateMessageV2:
		slog.Info("truncate operation", slog.Uint64("xid", uint64(logicalMsg.Xid)))
		// ...

	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessageV2:
		slog.Info("logical decoding message",
			slog.String("prefix", logicalMsg.Prefix),
			slog.String("content", string(logicalMsg.Content)),
			slog.Uint64("xid", uint64(logicalMsg.Xid)),
		)

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		slog.Debug("stream start message",
			slog.Uint64("xid", uint64(logicalMsg.Xid)),
			slog.Bool("firstSegment", logicalMsg.FirstSegment == 1),
		)
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		slog.Debug("stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		slog.Debug("stream commit message", slog.Uint64("xid", uint64(logicalMsg.Xid)))
	case *pglogrepl.StreamAbortMessageV2:
		slog.Debug("stream abort message", slog.Uint64("xid", uint64(logicalMsg.Xid)))
	default:
		slog.Warn("unknown message type in pgoutput stream", slog.String("type", fmt.Sprintf("%T", logicalMsg)))
	}
}

func processV1(walData []byte, relations map[uint32]*pglogrepl.RelationMessage, typeMap *pgtype.Map) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		slog.Error("failed to parse logical replication message", slog.String("err", err.Error()))
		return
	}
	slog.Debug("received logical replication message", slog.String("type", logicalMsg.Type().String()))
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			slog.Error("unknown relation ID", slog.Uint64("relationID", uint64(logicalMsg.RelationID)))
			return
		}
		values := map[string]any{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					slog.Error("error decoding column data", slog.String("err", err.Error()))
					return
				}
				values[colName] = val
			}
		}
		slog.Info("insert operation",
			slog.String("namespace", rel.Namespace),
			slog.String("relationName", rel.RelationName),
			slog.Any("values", values),
		)

		// todo bulk insert into elasticsearch

	case *pglogrepl.UpdateMessage:
		// todo bulk update into elasticsearch
	case *pglogrepl.DeleteMessage:
		// todo bulk delete into elasticsearch
	case *pglogrepl.TruncateMessage:
		// todo maybe bulk delete into elasticsearch

	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessage:
		slog.Info("logical decoding message",
			slog.String("prefix", logicalMsg.Prefix),
			slog.String("content", string(logicalMsg.Content)),
		)

	case *pglogrepl.StreamStartMessageV2:
		slog.Debug("stream start message",
			slog.Uint64("xid", uint64(logicalMsg.Xid)),
			slog.Bool("firstSegment", logicalMsg.FirstSegment == 1),
		)
	case *pglogrepl.StreamStopMessageV2:
		slog.Debug("stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		slog.Debug("stream commit message", slog.Uint64("xid", uint64(logicalMsg.Xid)))
	case *pglogrepl.StreamAbortMessageV2:
		slog.Debug("stream abort message", slog.Uint64("xid", uint64(logicalMsg.Xid)))
	default:
		slog.Warn("unknown message type in pgoutput stream", slog.String("type", fmt.Sprintf("%T", logicalMsg)))
	}
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
