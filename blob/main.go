package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/jasonsoft/log"
	"github.com/jasonsoft/log/handlers/console"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
)

//var bucketURL = "file:///./"
var bucketURL = "gs://sit-atp-backend"

func main() {
	log.SetAppID("blob") // unique id for the app

	clog := console.New()
	log.RegisterHandler(clog, log.AllLevels...)

	ctx := context.Background()
	err := writeBlob(ctx)
	if err != nil {
		log.Panic(err)
	}

	err = readBlob(ctx)
	if err != nil {
		log.Panic(err)
	}
}

func writeBlob(ctx context.Context) error {
	fileBucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return err
	}
	defer fileBucket.Close()

	w, err := fileBucket.NewWriter(ctx, "jasontest/hell/hello.txt", nil)
	if err != nil {
		return err
	}

	_, writeErr := fmt.Fprintln(w, "Hello, World!")
	closeErr := w.Close()
	if writeErr != nil {
		return err
	}
	if closeErr != nil {
		return err
	}

	return nil
}

func readBlob(ctx context.Context) error {
	fileBucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return err
	}
	defer fileBucket.Close()

	r, err := fileBucket.NewReader(ctx, "hello.txt", nil)
	if err != nil {
		return err
	}

	// Readers also have a limited view of the blob's metadata.
	fmt.Println("Content-Type:", r.ContentType())
	fmt.Println()

	// Copy from the reader to stdout.
	if _, err := io.Copy(os.Stdout, r); err != nil {
		return err
	}

	return nil
}
