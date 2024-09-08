package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func StsGetCallerIdentity(ctx context.Context, cfg aws.Config) (*sts.GetCallerIdentityOutput, error) {
	cli := sts.NewFromConfig(cfg)
	return cli.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
}
