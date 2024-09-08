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

type Object struct {
	Bucket string
	Key    string
}

func NewClient(cfg aws.Config) *Client {
	client := s3.NewFromConfig(cfg)
	return &Client{client: client}
}

func (c *Client) ListBuckets(ctx context.Context) (*s3.ListBucketsOutput, error) {
	return c.client.ListBuckets(ctx, &s3.ListBucketsInput{})
}

func (c *Client) ListObjects(ctx context.Context, bucket string, keyPrefix string) (*s3.ListObjectsV2Output, error) {
	var keyPrefix2 *string
	if keyPrefix != "" {
		keyPrefix2 = &keyPrefix
	}
	res, err := c.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: keyPrefix2,
	})
	return res, err
}

func (c *Client) GetObject(ctx context.Context, obj *Object) (io.Reader, func(), error) {
	res, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(obj.Bucket),
		Key:    aws.String(obj.Key),
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

func (c *Client) PresignGetObject(ctx context.Context, obj *Object) (*signer.PresignedHTTPRequest, error) {
	return s3.NewPresignClient(c.client).PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(obj.Bucket),
		Key:    aws.String(obj.Key),
	})
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
