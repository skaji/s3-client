package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
)

var version = "dev"

const helpMessage = `Usage: s3-client [command] args...

Command:
  help
  version
  whoami

  get         bucket/key localFile
  cat         bucket/key
  zcat        bucket/key

  ls          bucket
  ls          bucket/keyPrefix

  put         [--content-type TYPE] localFile bucket/key

  private-url bucket/key
  public-url  bucket/key
`

func main() {
	if len(os.Args) <= 1 {
		fmt.Fprintln(os.Stderr, "need argument")
		os.Exit(1)
	}

	if err := run(context.Background(), os.Args[1], os.Args[2:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, args ...string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}

	switch cmd {
	case "help", "-h", "--help":
		fmt.Print(helpMessage)
		return nil
	case "version", "--version":
		fmt.Println(version)
		return nil
	case "whoami":
		res, err := StsGetCallerIdentity(ctx, cfg)
		if err != nil {
			return err
		}
		jsonDump(res)
		return nil
	case "cat":
		if err := needArgs(args, 1); err != nil {
			return err
		}
		bucket, key, err := parseAsObject(args[0], true)
		if err != nil {
			return err
		}
		reader, closes, err := NewClient(cfg).GetObject(ctx, &Object{
			Bucket: bucket,
			Key:    key,
		})
		if err != nil {
			return err
		}
		defer closes()
		_, err = io.Copy(os.Stdout, reader)
		return err
	case "zcat":
		if err := needArgs(args, 1); err != nil {
			return err
		}
		bucket, key, err := parseAsObject(args[0], true)
		if err != nil {
			return err
		}
		reader, closes, err := NewClient(cfg).GetObject(ctx, &Object{
			Bucket: bucket,
			Key:    key,
		})
		if err != nil {
			return err
		}
		defer closes()
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return err
		}
		_, err = io.Copy(os.Stdout, gzipReader)
		return err
	case "get":
		if err := needArgs(args, 1, 2); err != nil {
			return err
		}
		bucket, key, err := parseAsObject(args[0], true)
		if err != nil {
			return err
		}
		var localFile string
		if len(args) == 1 {
			localFile = strings.ReplaceAll(key, "/", "_")
		} else if len(args) == 2 {
			localFile = args[1]
			if info, err := os.Stat(args[1]); err == nil {
				if info.IsDir() {
					localFile = filepath.Join(args[1], strings.ReplaceAll(key, "/", "_"))
				}
			}
		}
		f, err := os.Create(localFile + "_tmp")
		if err != nil {
			return err
		}
		defer f.Close()
		defer os.Remove(localFile + "_tmp")
		reader, closes, err := NewClient(cfg).GetObject(ctx, &Object{
			Bucket: bucket,
			Key:    key,
		})
		if err != nil {
			return err
		}
		defer closes()
		if _, err := io.Copy(f, reader); err != nil {
			return err
		}
		f.Close()
		return os.Rename(localFile+"_tmp", localFile)
	case "ls":
		if err := needArgs(args, 0, 1); err != nil {
			return err
		}
		if len(args) == 0 {
			res, err := NewClient(cfg).ListBuckets(ctx)
			if err != nil {
				return err
			}
			for _, b := range res.Buckets {
				tm := b.CreationDate.Local().Format(time.RFC3339)
				name := b.Name
				fmt.Printf("%s %s\n", tm, *name)
			}
			return nil
		}
		bucket, keyPrefix, err := parseAsObject(args[0], false)
		if err != nil {
			return err
		}
		res, err := NewClient(cfg).ListObjects(ctx, bucket, keyPrefix)
		if err != nil {
			return err
		}
		for _, obj := range res.Contents {
			tm := obj.LastModified.Local().Format(time.RFC3339)
			key := obj.Key
			size := obj.Size
			fmt.Printf("%s %10d %s/%s\n", tm, *size, bucket, *key)
		}
		fmt.Fprintf(os.Stderr, "IsTruncated: %v\n", *res.IsTruncated)
		return nil
	case "put":
		contentType := ""
		if len(args) > 2 && args[0] == "--content-type" {
			contentType = args[1]
			args = args[2:]
		}
		if err := needArgs(args, 2); err != nil {
			return err
		}
		f, err := os.Open(args[0])
		if err != nil {
			return err
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return err
		}

		bucket, key, err := parseAsObject(args[1], true)
		if err != nil {
			return err
		}
		return NewClient(cfg).PutObject(ctx, PutObjectInput{
			Object: &Object{
				Bucket: bucket,
				Key:    key,
			},
			Body:          f,
			ContentLength: info.Size(),
			ContentType:   contentType,
		})
	case "public-url":
		if err := needArgs(args, 1); err != nil {
			return err
		}
		bucket, key, err := parseAsObject(args[0], true)
		if err != nil {
			return err
		}
		fmt.Printf("https://%s.s3-%s.amazonaws.com/%s\n", bucket, cfg.Region, key)
		return nil
	case "private-url":
		if err := needArgs(args, 1); err != nil {
			return err
		}
		bucket, key, err := parseAsObject(args[0], true)
		if err != nil {
			return err
		}
		res, err := NewClient(cfg).PresignGetObject(ctx, &Object{
			Bucket: bucket,
			Key:    key,
		})
		if err != nil {
			return err
		}
		fmt.Println(res.URL)
		return nil
	}

	return errors.New("unknown command: " + cmd)
}

func jsonDump(v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}

func needArgs(args []string, needs ...int) error {
	if slices.Contains(needs, len(args)) {
		return nil
	}
	return errors.New("invalid arguments")
}

func parseAsObject(arg string, needKey bool) (string, string, error) {
	if strings.HasPrefix(arg, "s3://") {
		u, err := url.Parse(arg)
		if err != nil {
			return "", "", err
		}
		bucket := u.Host
		key := u.Path
		if key == "/" {
			key = ""
		}
		if key == "" && needKey {
			return "", "", errors.New("need key")
		}
		return bucket, key, nil
	}
	parts := strings.SplitN(arg, "/", 2)
	if len(parts) == 1 {
		if needKey {
			return "", "", errors.New("need key")
		}
		return parts[0], "", nil
	}
	return parts[0], parts[1], nil
}
