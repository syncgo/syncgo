package replication

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/syncgo/syncgo/internal/bulk_transformer"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func tupleToValues(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData, typeMap *pgtype.Map) map[string]any {
	if tuple == nil {
		return nil
	}

	values := make(map[string]any, len(tuple.Columns))
	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n':
			values[colName] = nil
		case 'u':
			// unchanged TOAST — omit from document body
		case 't':
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				slog.Error("error decoding text column", slog.String("column", colName), slog.String("err", err.Error()))
				continue
			}
			values[colName] = val
		case 'b':
			val, err := decodeBinaryColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				slog.Error("error decoding binary column", slog.String("column", colName), slog.String("err", err.Error()))
				continue
			}
			values[colName] = val
		}
	}
	return values
}

func documentID(idColumn string, values map[string]any) (string, bool) {
	if values == nil {
		return "", false
	}
	val, ok := values[idColumn]
	if !ok || val == nil {
		return "", false
	}
	return fmt.Sprint(val), true
}

func valuesToData(id string, action bulk_transformer.Action, values map[string]any) (bulk_transformer.Data, bool) {
	body, err := json.Marshal(values)
	if err != nil {
		slog.Error("failed to marshal row to JSON", slog.String("err", err.Error()))
		return bulk_transformer.Data{}, false
	}
	return bulk_transformer.Data{
		ID:     id,
		Action: action,
		Body:   body,
	}, true
}

func deleteData(id string) bulk_transformer.Data {
	return bulk_transformer.Data{
		ID:     id,
		Action: bulk_transformer.Delete,
	}
}

func (c *LogicalReplicationConn) enqueueRow(item bulk_transformer.Data) {
	if c.batcher == nil {
		return
	}
	c.batcher.Add(item)
	c.txn.recordAdd()
}
