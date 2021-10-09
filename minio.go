package minio

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	Access   string
	Secret   string
	Bucket   string
	Endpoint string
}

type Client struct {
	c      *minio.Client
	config *Config
	bucket string
}

func New(config *Config) (*Client, error) {
	if config == nil {
		return nil, errors.New("must provide config")
	}
	c := config
	if c.Access == "" || c.Secret == "" || c.Bucket == "" || c.Endpoint == "" {
		return nil, errors.New("must provide all fields in config")
	}

	mc, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.Access, c.Secret, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}
	found, err := mc.BucketExists(ctx(), c.Bucket)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("bucket '%s' doesn't existt", c.Bucket)
	}

	return &Client{
		c:      mc,
		config: config,
		bucket: config.Bucket,
	}, nil
}

func (c *Client) URLBase() string {
	url := c.c.EndpointURL()
	return fmt.Sprintf("https://%s.%s/", c.bucket, url.Host)
}

func (c *Client) URLForPath(remotePath string) string {
	return c.URLBase() + strings.TrimPrefix(remotePath, "/")
}

func (c *Client) Exists(remotePath string) bool {
	_, err := c.c.StatObject(ctx(), c.bucket, remotePath, minio.StatObjectOptions{})
	return err == nil
}

func ctx() context.Context {
	return context.Background()
}
