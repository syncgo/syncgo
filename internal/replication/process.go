package replication

import (
	"fmt"
	"log/slog"

	"github.com/syncgo/syncgo/internal/bulk_transformer"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func (c *LogicalReplicationConn) process(walData []byte, relations map[uint32]*pglogrepl.RelationMessage, typeMap *pgtype.Map) {
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
		c.txn.begin()

	case *pglogrepl.CommitMessage:
		c.txn.commit(c.batcher)
		c.incTxn()

	case *pglogrepl.InsertMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			slog.Error("unknown relation ID", slog.Uint64("relationID", uint64(logicalMsg.RelationID)))
			c.incErr("unknown_relation")
			return
		}
		values := tupleToValues(rel, logicalMsg.Tuple, typeMap)
		slog.Debug("insert data", slog.Any("values", values))
		id, ok := documentID(c.idColumn, values)
		if !ok {
			slog.Error("missing document id column",
				slog.String("column", c.idColumn),
				slog.String("relation", rel.RelationName),
			)
			c.incErr("missing_id_column")
			return
		}
		item, ok := valuesToData(id, bulk_transformer.Index, values)
		if !ok {
			c.incErr("marshal_row")
			return
		}
		c.enqueueRow(item)
		c.incEvent("insert")
		slog.Debug("insert operation",
			slog.String("namespace", rel.Namespace),
			slog.String("relationName", rel.RelationName),
			slog.String("id", id),
		)

	case *pglogrepl.UpdateMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			slog.Error("unknown relation ID", slog.Uint64("relationID", uint64(logicalMsg.RelationID)))
			c.incErr("unknown_relation")
			return
		}
		keyValues := tupleToValues(rel, logicalMsg.OldTuple, typeMap)
		if len(keyValues) == 0 {
			keyValues = tupleToValues(rel, logicalMsg.NewTuple, typeMap)
		}
		slog.Debug("update old data", slog.Any("values", keyValues))
		id, ok := documentID(c.idColumn, keyValues)
		if !ok {
			slog.Error("missing document id column for update",
				slog.String("column", c.idColumn),
				slog.String("relation", rel.RelationName),
			)
			c.incErr("missing_id_column")
			return
		}
		newValues := tupleToValues(rel, logicalMsg.NewTuple, typeMap)
		slog.Debug("update new data", slog.Any("values", keyValues))
		item, ok := valuesToData(id, bulk_transformer.Update, newValues)
		if !ok {
			c.incErr("marshal_row")
			return
		}
		c.enqueueRow(item)
		c.incEvent("update")
		slog.Debug("update operation",
			slog.String("namespace", rel.Namespace),
			slog.String("relationName", rel.RelationName),
			slog.String("id", id),
		)

	case *pglogrepl.DeleteMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			slog.Error("unknown relation ID", slog.Uint64("relationID", uint64(logicalMsg.RelationID)))
			c.incErr("unknown_relation")
			return
		}
		values := tupleToValues(rel, logicalMsg.OldTuple, typeMap)
		slog.Debug("delete data", slog.Any("values", values))
		id, ok := documentID(c.idColumn, values)
		if !ok {
			slog.Error("missing document id column for delete",
				slog.String("column", c.idColumn),
				slog.String("relation", rel.RelationName),
			)
			c.incErr("missing_id_column")
			return
		}
		c.enqueueRow(deleteData(id))
		c.incEvent("delete")
		slog.Debug("delete operation",
			slog.String("namespace", rel.Namespace),
			slog.String("relationName", rel.RelationName),
			slog.String("id", id),
		)

	case *pglogrepl.TruncateMessage:
		c.incEvent("truncate")
		slog.Debug("truncate operation", slog.Int("relationCount", len(logicalMsg.RelationIDs)))

	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessage:
		slog.Info("logical decoding message",
			slog.String("prefix", logicalMsg.Prefix),
			slog.String("content", string(logicalMsg.Content)),
		)

	default:
		slog.Warn("unknown message type in pgoutput stream", slog.String("type", fmt.Sprintf("%T", logicalMsg)))
	}
}
