package storage

import (
	"fmt"
	"time"

	"bitbucket.org/dasa_desenv/move-s3-objects/errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/thoas/go-funk"
)

//Parameters ...
type Parameters struct {
	AccessKey string
	SecretKey string
	Region    string
}

// Client ...
type Client struct {
	s3Client *s3.S3
}

//FilterCallBack ...
type FilterCallBack func([]*s3.Object) []*s3.Object

func (client *Client) genericBucketList(params *S3FileParams, keys []*S3File, fn FilterCallBack) []*S3File {
	query := &s3.ListObjectsV2Input{
		Bucket: aws.String(params.Bucket),
		Prefix: aws.String(params.Prefix),
	}

	truncatedListing := true

	for truncatedListing {
		resp, err := client.s3Client.ListObjectsV2(query)

		if err != nil {
			errors.ExitErrorf("Unable to list items in bucket %q, %v", params.Bucket, err)
		}

		r := fn(resp.Contents)

		for _, item := range r {
			keys = append(keys, &S3File{CompletePath: item.Key})
		}

		fmt.Println(len(keys))

		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}

	return keys
}

//CopyObject ...
func (client *Client) CopyObject(bucketTo, copySource, key string) (*s3.CopyObjectOutput, error) {
	return client.s3Client.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(bucketTo), CopySource: aws.String(copySource), Key: aws.String(key)})
}

// WaitUntilObjectExists ...
func (client *Client) WaitUntilObjectExists(bucketTo, key string) error {
	return client.s3Client.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: aws.String(bucketTo), Key: aws.String(key)})
}

//ListBucketFiles ...
func (client *Client) ListBucketFiles(params *S3FileParams, keys []*S3File) []*S3File {
	if params.Prefix == "" {
		return client.genericBucketList(params, keys, func(contents []*s3.Object) []*s3.Object { return contents })
	}

	return client.genericBucketList(params, keys, func(contents []*s3.Object) []*s3.Object {

		deadlineDate := time.Date(2019, 9, 30, 23, 59, 59, 651387237, time.UTC)

		return funk.Filter(contents, func(x *s3.Object) bool {
			return x.LastModified.After(deadlineDate)
		}).([]*s3.Object)
	})
}

// New ...
func New(parms *Parameters) *Client {

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(parms.Region),
		Credentials: credentials.NewStaticCredentials(parms.AccessKey, parms.SecretKey, ""),
	})

	if err != nil {
		errors.ExitErrorf("Error create new session aws, %v", err)
	}

	return &Client{
		s3Client: s3.New(sess),
	}
}
