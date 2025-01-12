package main

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Client struct {
	client *s3.Client
}

func NewClient(cfg aws.Config) *Client {
	client := s3.NewFromConfig(cfg)
	return &Client{client: client}
}

type Object struct {
	Bucket string
	Key    string
}

func (c *Client) ListBuckets(ctx context.Context) (*s3.ListBucketsOutput, error) {
	return c.client.ListBuckets(ctx, &s3.ListBucketsInput{})
}

func (c *Client) ListObjects(ctx context.Context, bucket string, keyPrefix string) (*s3.ListObjectsV2Output, error) {
	return c.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: pointerOrNil(bucket),
		Prefix: pointerOrNil(keyPrefix),
	})
}

func (c *Client) GetObject(ctx context.Context, obj *Object) (io.Reader, func(), error) {
	res, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: pointerOrNil(obj.Bucket),
		Key:    pointerOrNil(obj.Key),
	})
	if err != nil {
		return nil, nil, err
	}
	closes := func() {
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}
	return res.Body, closes, nil
}

type PutObjectInput struct {
	Object        *Object
	Body          io.Reader
	ContentLength int64
	ContentType   string
}

func (c *Client) PutObject(ctx context.Context, input PutObjectInput) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        pointerOrNil(input.Object.Bucket),
		Key:           pointerOrNil(input.Object.Key),
		Body:          input.Body,
		ContentLength: pointerOrNil(input.ContentLength),
		ContentType:   pointerOrNil(input.ContentType),
	})
	return err
}

func (c *Client) DeleteObjects(ctx context.Context, objs []*Object) error {
	var deleteObjects []types.ObjectIdentifier
	bucket := objs[0].Bucket
	for _, obj := range objs {
		if obj.Bucket != bucket {
			return errors.New("cannot delete multiple bucket objects at once")
		}
		deleteObjects = append(deleteObjects, types.ObjectIdentifier{
			Key: aws.String(obj.Key),
		})
	}
	_, err := c.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: deleteObjects,
		},
	})
	return err
}

func (c *Client) PresignGetObject(ctx context.Context, obj *Object) (*signer.PresignedHTTPRequest, error) {
	return s3.NewPresignClient(c.client).PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: pointerOrNil(obj.Bucket),
		Key:    pointerOrNil(obj.Key),
	})
}

func pointerOrNil[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}
	return &v
}
