package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
	"github.com/thoas/go-funk"
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func goDotEnvVariable(key string) string {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file")
	}
	return os.Getenv(key)
}

func spreadErrors(args ...string) string {
	message := "errors: /n"
	for _, v := range args {
		message = message + " " + v
	}

	return message
}

func listAllKeys(svc *s3.S3, bucketFrom string, keys []*string) []*string {
	query := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketFrom),
		Prefix: aws.String("final/faturamentodigital"),
	}

	fmt.Println("start listall keys")

	truncatedListing := true

	for truncatedListing {
		resp, err := svc.ListObjectsV2(query)

		if err != nil {
			exitErrorf("Unable to list items in bucket %q, %v", bucketFrom, err)
		}

		deadlineDate := time.Date(2019, 9, 30, 23, 59, 59, 651387237, time.UTC)

		fmt.Println(len(resp.Contents))

		r := funk.Filter(resp.Contents, func(x *s3.Object) bool {
			return x.LastModified.After(deadlineDate)
		}).([]*s3.Object)

		fmt.Println(len(r))

		for _, item := range r {
			keys = append(keys, item.Key)
		}

		fmt.Println(len(keys))

		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}

	return keys
}

func main() {
	accessKey := goDotEnvVariable("accessKey")
	secretKey := goDotEnvVariable("secretKey")
	bucketFrom := goDotEnvVariable("bucketFrom")
	awsRegion := goDotEnvVariable("awsRegion")
	bucketTo := goDotEnvVariable("bucketTo")

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
	})

	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucketFrom, err)
	}

	svc := s3.New(sess)

	keys := make([]*string, 0)

	finalKeys := listAllKeys(svc, bucketFrom, keys)

	keysErrors := make([]string, 0)

	for _, item := range finalKeys {
		keyString := aws.StringValue(item)
		key := strings.Split(keyString, "/")[1]
		copySource := bucketFrom + "/" + keyString

		if key == "" || len(key) < 10 {
			keysErrors = append(keysErrors, keyString)
			continue
		}

		_, err = svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(bucketTo), CopySource: aws.String(copySource), Key: aws.String(key)})

		if err != nil {
			exitErrorf("Unable to copy item> %q from bucket %q to bucket %q, %v", keyString, bucketFrom, bucketTo, err)
		}

		err = svc.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: aws.String(bucketTo), Key: aws.String(key)})

		if err != nil {
			exitErrorf("Error occurred while waiting for item %q to be copied to bucket %q, %v", item, bucketTo, err)
		}
		fmt.Printf("Item %q successfully copied from bucket %q to bucket %q\n", key, bucketFrom, bucketTo)
	}

	errorsMessage := spreadErrors(keysErrors...)
	fmt.Println("Sum is ", errorsMessage)
}
