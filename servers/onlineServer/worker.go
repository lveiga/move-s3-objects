package main

import (
	"fmt"
	"log"
	"os"

	"bitbucket.org/dasa_desenv/move-s3-objects/errors"
	"bitbucket.org/dasa_desenv/move-s3-objects/storage"
	"github.com/joho/godotenv"
)

func goDotEnvVariable(key string) string {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file")
	}
	return os.Getenv(key)
}

func main() {
	accessKey := goDotEnvVariable("accessKey")
	secretKey := goDotEnvVariable("secretKey")
	bucketFrom := goDotEnvVariable("bucketFrom")
	awsRegion := goDotEnvVariable("awsRegion")
	bucketTo := goDotEnvVariable("bucketTo")

	storageClient := storage.New(&storage.Parameters{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    awsRegion,
	})

	keys := make([]*storage.S3File, 0)

	listedFiles := storageClient.ListBucketFiles(&storage.S3FileParams{
		Bucket: bucketFrom, Prefix: "final/faturamentodigital"},
		keys)

	keysErrors := make([]string, 0)

	for _, item := range listedFiles {
		key, copySource := item.GetFileName(bucketFrom)

		if key == "" || len(key) < 10 {
			keysErrors = append(keysErrors, key)
			continue
		}

		if _, err := storageClient.CopyObject(bucketTo, copySource, key); err != nil {
			errors.ExitErrorf("Unable to copy item> %q from bucket %q to bucket %q, %v", key, bucketFrom, bucketTo, err)
		}

		if err := storageClient.WaitUntilObjectExists(bucketTo, key); err != nil {
			errors.ExitErrorf("Error occurred while waiting for item %q to be copied to bucket %q, %v", item, bucketTo, err)
		}

		fmt.Printf("Item %q successfully copied from bucket %q to bucket %q\n", key, bucketFrom, bucketTo)
	}

	errorsMessage := errors.SpreadErrors(keysErrors...)
	fmt.Println("Sum is ", errorsMessage)
}
