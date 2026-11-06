package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type R2Config struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	PublicDomain    string
}

func (c R2Config) Enabled() bool {
	return c.Endpoint != ""
}

type R2Client struct {
	client       *s3.Client
	bucket       string
	publicDomain string
}

func NewR2Client(cfg R2Config) (*R2Client, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("snapshot storage endpoint is empty (SFU_R2_ENDPOINT)")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("snapshot storage bucket is empty (SFU_R2_BUCKET)")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("snapshot storage credentials are incomplete (SFU_R2_ACCESS_KEY_ID, SFU_R2_SECRET_ACCESS_KEY)")
	}

	client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(strings.TrimRight(cfg.Endpoint, "/")),
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		UsePathStyle: true,
	})

	return &R2Client{
		client:       client,
		bucket:       cfg.Bucket,
		publicDomain: strings.TrimSpace(cfg.PublicDomain),
	}, nil
}

func (c *R2Client) PutObject(ctx context.Context, key string, data []byte) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("snapshot R2 client is nil")
	}

	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("image/jpeg"),
	})
	if err != nil {
		return fmt.Errorf("put snapshot object: %w", err)
	}
	return nil
}

func (c *R2Client) PublicURLForKey(key string) string {
	if c == nil || c.publicDomain == "" {
		return ""
	}
	return "https://" + strings.Trim(c.publicDomain, "/") + "/" + key
}
