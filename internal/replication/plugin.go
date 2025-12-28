package replication

type Plugin string

func (p Plugin) String() string {
	return string(p)
}

const (
	pgOutputPlugin Plugin = "pgoutput"
)
