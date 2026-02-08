package gzip

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestParseGzipCompressionLevel(t *testing.T) {
	cases := []struct {
		name     string
		level    GzipCompressionLevel
		expected int
	}{
		{
			name:     "default compression",
			level:    GzipCompressionLevelDefault,
			expected: fasthttp.CompressDefaultCompression,
		},
		{
			name:     "no compression",
			level:    GzipCompressionLevelNo,
			expected: fasthttp.CompressNoCompression,
		},
		{
			name:     "best speed",
			level:    GzipCompressionLevelBestSpeed,
			expected: fasthttp.CompressBestSpeed,
		},
		{
			name:     "best compression",
			level:    GzipCompressionLevelBestCompression,
			expected: fasthttp.CompressBestCompression,
		},
		{
			name:     "huffman only",
			level:    GzipCompressionLevelHuffmanOnly,
			expected: fasthttp.CompressHuffmanOnly,
		},
		{
			name:     "invalid level",
			level:    GzipCompressionLevel("invalid"),
			expected: -1,
		},
		{
			name:     "empty string",
			level:    GzipCompressionLevel(""),
			expected: -1,
		},
		{
			name:     "case sensitive mismatch",
			level:    GzipCompressionLevel("Default"),
			expected: -1,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ParseGzipCompressionLevel(tt.level)
			require.Equal(t, tt.expected, result, "ParseGzipCompressionLevel(%q) = %d, want %d", tt.level, result, tt.expected)
		})
	}
}
