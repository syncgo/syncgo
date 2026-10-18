package plugins

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/syncgo/syncgo/internal/bulk_transformer"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// pgOutputPlugin decodes the pgoutput stream (PostgreSQL built-in logical
// decoding output plugin). It keeps the relation cache and converts DML
// messages into bulk_transformer.Data items.
type pgOutputPlugin struct {
	publicationName string
	idColumn        string

	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
}

func newPgOutputPlugin(publicationName, idColumn string) *pgOutputPlugin {
	return &pgOutputPlugin{
		publicationName: publicationName,
		idColumn:        idColumn,
		relations:       map[uint32]*pglogrepl.RelationMessage{},
		typeMap:         pgtype.NewMap(),
	}
}

func (p *pgOutputPlugin) Name() string {
	return pgoutputName
}

func (p *pgOutputPlugin) Options() []string {
	return []string{
		"proto_version '1'",
		fmt.Sprintf(`publication_names %s`, quotePluginOption(p.publicationName)),
		"messages 'false'",
	}
}

// Decode parses a single pgoutput XLogData payload into events. Transaction
// boundaries are emitted as EventBegin/EventCommit/EventRollback so the
// caller can commit and rollback batcher rows; row changes carry the decoded
// document.
func (p *pgOutputPlugin) Decode(walData []byte) ([]Event, error) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	slog.Debug("received logical replication message", slog.String("type", logicalMsg.Type().String()))

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		p.relations[logicalMsg.RelationID] = logicalMsg
		return nil, nil

	case *pglogrepl.BeginMessage:
		return []Event{{Kind: EventBegin}}, nil

	case *pglogrepl.CommitMessage:
		return []Event{{Kind: EventCommit}}, nil

	case *pglogrepl.InsertMessage:
		rel, ok := p.relation(logicalMsg.RelationID)
		if !ok {
			return nil, nil
		}
		values := tupleToValues(rel, logicalMsg.Tuple, p.typeMap)
		slog.Debug("insert data", slog.Any("values", values))
		item, ok := p.rowData(rel, values, bulk_transformer.Index)
		if !ok {
			return nil, nil
		}
		return []Event{{Kind: EventChange, Change: item}}, nil

	case *pglogrepl.UpdateMessage:
		rel, ok := p.relation(logicalMsg.RelationID)
		if !ok {
			return nil, nil
		}
		keyValues := tupleToValues(rel, logicalMsg.OldTuple, p.typeMap)
		if len(keyValues) == 0 {
			keyValues = tupleToValues(rel, logicalMsg.NewTuple, p.typeMap)
		}
		slog.Debug("update old data", slog.Any("values", keyValues))
		id, ok := documentID(p.idColumn, keyValues)
		if !ok {
			p.logMissingID(rel, "update")
			return nil, nil
		}
		newValues := tupleToValues(rel, logicalMsg.NewTuple, p.typeMap)
		slog.Debug("update new data", slog.Any("values", keyValues))
		item, ok := valuesToData(id, bulk_transformer.Update, newValues)
		if !ok {
			return nil, nil
		}
		return []Event{{Kind: EventChange, Change: item}}, nil

	case *pglogrepl.DeleteMessage:
		rel, ok := p.relation(logicalMsg.RelationID)
		if !ok {
			return nil, nil
		}
		values := tupleToValues(rel, logicalMsg.OldTuple, p.typeMap)
		slog.Debug("delete data", slog.Any("values", values))
		id, ok := documentID(p.idColumn, values)
		if !ok {
			p.logMissingID(rel, "delete")
			return nil, nil
		}
		return []Event{{Kind: EventChange, Change: deleteData(id)}}, nil

	case *pglogrepl.TruncateMessage:
		slog.Debug("truncate operation", slog.Int("relationCount", len(logicalMsg.RelationIDs)))
		return nil, nil

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

	return nil, nil
}

func (p *pgOutputPlugin) relation(relationID uint32) (*pglogrepl.RelationMessage, bool) {
	rel, ok := p.relations[relationID]
	if !ok {
		slog.Error("unknown relation ID", slog.Uint64("relationID", uint64(relationID)))
	}
	return rel, ok
}

func (p *pgOutputPlugin) rowData(rel *pglogrepl.RelationMessage, values map[string]any, action bulk_transformer.Action) (bulk_transformer.Data, bool) {
	id, ok := documentID(p.idColumn, values)
	if !ok {
		p.logMissingID(rel, action.String())
		return bulk_transformer.Data{}, false
	}
	return valuesToData(id, action, values)
}

func (p *pgOutputPlugin) logMissingID(rel *pglogrepl.RelationMessage, operation string) {
	slog.Error("missing document id column",
		slog.String("operation", operation),
		slog.String("column", p.idColumn),
		slog.String("relation", rel.RelationName),
	)
}

// quotePluginOption quotes a single-quoted START_REPLICATION option value,
// escaping embedded single quotes.
func quotePluginOption(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
