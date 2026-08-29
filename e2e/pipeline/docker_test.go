//go:build e2e

// Docker-mode pipeline tests: syncgo's Docker image is built from the
// repository's Dockerfile and run with `docker run` on the same compose
// network as the infrastructure, with its config supplied from the e2e
// directory (mounted in from a generated file).
package pipeline

import "testing"

func TestPipelineDockerElasticsearch(t *testing.T) {
	ensureInfra(t)
	image := ensureImage(t)

	runPipelineTest(t, modeDocker, engineElasticsearch, image, networkName)
}

func TestPipelineDockerOpensearch(t *testing.T) {
	ensureInfra(t)
	image := ensureImage(t)

	runPipelineTest(t, modeDocker, engineOpensearch, image, networkName)
}
