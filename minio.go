// .\minio.exe server C:\minio --console-address :9001
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/remiges-tech/logharbour/logharbour"
)

var err error

func main() {
	ctx := context.Background()

	logFile, err := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		slog.Error("Error in Opening Log file:", err)
	}
	err = logFile.Truncate(0)
	if err != nil {
		slog.Error("Error in deleting old content:", err)
	}

	fallbackWriter := NewFallbackWriter(logFile, os.Stdout)
	lctx := NewLoggerContext(Info)
	logger := NewLoggerWithFallback(lctx, "MyApp", fallbackWriter)
	logger = logger.WithModule("Module 1").
		WithWho("Vrutik Savla").
		WithStatus(Success).
		WithRemoteIP("")

	// Connect to Minio Client
    minioClient, err := connectMinio(logger)
	if err != nil {
        logger.Error(err)
	}		
    logger.LogActivity("Minio Client got connected", fmt.Sprintf("%#v\n", minioClient)) // minioClient is now set up

	// Make a new bucket
	bucketName := "miniotask"
	makeBucket(minioClient, ctx, bucketName, logger)

	// Upload file in bucket
	uploadFile(minioClient ,ctx, bucketName, "sample1.txt", "testdata/sample1.txt", logger)
	uploadFile(minioClient ,ctx, bucketName, "sample2.txt", "testdata/sample2.txt", logger)

	// List files in bucket
	listFiles(minioClient, bucketName, logger)

	// Delete files in bucket
	deleteFiles(minioClient, bucketName, []string{"sample1.txt"}, logger)
}

// connectMinio: This function loads configuration details for .env file & connects with minioClient
func connectMinio(logger *Logger) (minioClient *minio.Client, err error) {
	err = godotenv.Load("config.env")
	if err != nil {
		logger.Error(err)
	}

	// Access the environment variables
	endpoint := os.Getenv("ENDPOINT")
    accessKeyID := os.Getenv("ACCESSKEYID")
    secretAccessKey := os.Getenv("SECRETACCESSKEY") 
    useSSL := false

	// Initialize minio client object.
    minioClient, err = minio.New(endpoint, &minio.Options{
            Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
            Secure: useSSL,
    })
    
	return minioClient, err
}

func makeBucket(minioClient *minio.Client, ctx context.Context, bucketName string, logger *Logger) {
	err =  minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "sncr-east-05"})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if (errBucketExists == nil && exists) {
			logger.LogActivity("We already own the bucket", bucketName)
		} else {
			log.Fatalln("Error:", err)
		}
	} else {
		logger.LogActivity("Successfully created the bucket", bucketName)
	}
}

func uploadFile(minioClient *minio.Client, ctx context.Context, bucketName string, objectName string, filePath string, logger *Logger) {
	// Upload the test file with FPutObject
	info, err := minioClient.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{ContentType: "application/octet-stream"})

	if err != nil {
        logger.Error(err)
    }
	logger.LogActivity("Successfully uploaded file", fmt.Sprintf("File: %s, Size: %d", objectName, info.Size))
}

func listFiles(client *minio.Client, bucketName string, logger *Logger) {
	// doneCh := make(chan struct{})
	// defer close(doneCh)
	var files []string;
	objectCh := client.ListObjects(context.Background(), bucketName, minio.ListObjectsOptions{})
	for obj := range objectCh {
		if obj.Err != nil {
			logger.Error(obj.Err)
			return
		}
		files = append(files, obj.Key)
	}
	logger.LogActivity("Listing files:", files)
}

func deleteFiles(client *minio.Client, bucketName string, files []string, logger *Logger) {
	for _, file := range files {
		objectName := file

		// Delete file from Minio
		err := client.RemoveObject(context.Background(), bucketName, objectName, minio.RemoveObjectOptions{})
		if err != nil {
			log.Printf("Error deleting file %s: %v\n", file, err)
			continue
		}

	}
	logger.LogActivity("Files deleted successfully", files)
}