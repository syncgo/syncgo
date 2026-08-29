//go:build e2e

// Binary-mode pipeline tests: syncgo is built locally with `go build` and run
// as a plain OS process against the docker-compose infrastructure.
package pipeline

import "testing"

func TestPipelineBinaryElasticsearch(t *testing.T) {
	ensureInfra(t)
	bin := ensureBinary(t)

	runPipelineTest(t, modeBinary, engineElasticsearch, bin, "")
}

func TestPipelineBinaryOpensearch(t *testing.T) {
	ensureInfra(t)
	bin := ensureBinary(t)

	runPipelineTest(t, modeBinary, engineOpensearch, bin, "")
}
