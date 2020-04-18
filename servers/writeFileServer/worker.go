package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"bitbucket.org/dasa_desenv/move-s3-objects/errors"
	"bitbucket.org/dasa_desenv/move-s3-objects/storage"
	"github.com/joho/godotenv"
	"github.com/thoas/go-funk"
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

	keysOld := make([]*storage.S3File, 0)
	keysNew := make([]*storage.S3File, 0)

	listedFilesOld := storageClient.ListBucketFiles(&storage.S3FileParams{
		Bucket: bucketFrom, Prefix: "final/faturamentodigital"},
		keysOld)

	finalOldBucket := funk.Map(listedFilesOld, func(x *storage.S3File) string {
		_, key := x.GetFileName(bucketFrom)
		return key
	}).([]string)

	listedFiles := storageClient.ListBucketFiles(&storage.S3FileParams{
		Bucket: bucketFrom, Prefix: ""},
		keysNew)

	finalNewBucket := funk.Map(listedFiles, func(x *storage.S3File) string {
		return *x.CompletePath
	}).([]string)

	resultFilter := funk.Filter(finalOldBucket, func(x string) bool {
		return !funk.Contains(finalNewBucket, x)
	}).([]string)

	stringByte := "\x00" + strings.Join(resultFilter, "\x20\x00")
	err := ioutil.WriteFile("test.txt", []byte(stringByte), 0644)

	if err != nil {
		errors.ExitErrorf("Unable to create file, %v", err)
	}

	filesOld, filesNew := funk.Difference(finalOldBucket, finalNewBucket)

	files := append(filesOld.([]string), filesNew.([]string)...)

	fmt.Printf("total novo bucket: %d\n", len(finalNewBucket))

	fmt.Printf("old: %d\n", len(filesOld.([]string)))
	fmt.Printf("new: %d\n", len(filesNew.([]string)))
	fmt.Printf("total: %d\n", len(files))

}
