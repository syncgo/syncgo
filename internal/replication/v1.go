package replication

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func processV1(walData []byte, relations map[uint32]*pglogrepl.RelationMessage, typeMap *pgtype.Map) {
	slog.Debug("v1 processing")

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
