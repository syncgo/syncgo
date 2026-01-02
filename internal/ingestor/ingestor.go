package ingestor

import "context"

type Bulker interface {
	Bulk(ctx context.Context, data []byte) error
}

type Ingestor struct {
	bulker Bulker
}

func New(bulker Bulker) *Ingestor {
	return &Ingestor{
		bulker: bulker,
	}
}
