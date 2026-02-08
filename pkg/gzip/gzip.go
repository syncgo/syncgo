package gzip

import "github.com/valyala/fasthttp"

type GzipCompressionLevel string

const (
	GzipCompressionLevelDefault         GzipCompressionLevel = "default"
	GzipCompressionLevelNo              GzipCompressionLevel = "no"
	GzipCompressionLevelBestSpeed       GzipCompressionLevel = "best-speed"
	GzipCompressionLevelBestCompression GzipCompressionLevel = "best-compression"
	GzipCompressionLevelHuffmanOnly     GzipCompressionLevel = "huffman-only"
)

func ParseGzipCompressionLevel(level GzipCompressionLevel) int {
	switch level {
	case GzipCompressionLevelDefault:
		return fasthttp.CompressDefaultCompression
	case GzipCompressionLevelNo:
		return fasthttp.CompressNoCompression
	case GzipCompressionLevelBestSpeed:
		return fasthttp.CompressBestSpeed
	case GzipCompressionLevelBestCompression:
		return fasthttp.CompressBestCompression
	case GzipCompressionLevelHuffmanOnly:
		return fasthttp.CompressHuffmanOnly
	default:
		return -1
	}
}
