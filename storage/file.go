package storage

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
)

// S3File ...
type S3File struct {
	CompletePath *string
}

// S3FileParams ...
type S3FileParams struct {
	Bucket string
	Prefix string
}

// GetFileName ...
func (file S3File) GetFileName(bucket string) (string, string) {
	keyString := aws.StringValue(file.CompletePath)
	key := strings.Split(keyString, "/")[3]
	copySource := bucket + "/" + keyString

	return key, copySource
}
