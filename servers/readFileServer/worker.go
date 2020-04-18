package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"bitbucket.org/dasa_desenv/move-s3-objects/storage"
	"github.com/joho/godotenv"
)

// ObjectsS3 ...
type ObjectsS3 struct {
	ObjectsS3 []*FinalJObject
}

// FinalJObject ...
type FinalJObject struct {
	Key  string   `json:"Key"`
	File ObjectS3 `json:"File"`
}

// ObjectS3 ...
type ObjectS3 struct {
	CompletePath string `json:"complete_path"`
	Key          string `json:"file_key"`
	CopySource   string `json:"copy_source"`
}

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

	final, _ := ioutil.ReadFile("faltando.json")

	final = bytes.TrimPrefix(final, []byte("\xef\xbb\xbf"))

	var finalData []*FinalJObject

	err := json.Unmarshal(final, &finalData)
	if err != nil {

	}

	errorsFiles := make([]string, 0)
	for _, item := range finalData {
		if _, err := storageClient.CopyObject(bucketTo, item.File.CopySource, item.Key); err != nil {
			errorsFiles = append(errorsFiles, "error when copy: "+item.File.CopySource)
		}

		if err := storageClient.WaitUntilObjectExists(bucketTo, item.Key); err != nil {
			errorsFiles = append(errorsFiles, "error when WaitUntilObjectExists: "+bucketTo+item.File.Key)
		}

		fmt.Printf("Item %q successfully copied from bucket %q to bucket %q\n", item.File.Key, bucketFrom, bucketTo)
	}

	es, _ := json.MarshalIndent(errorsFiles, "", "")
	ioutil.WriteFile("error.txt", es, 0644)
}
