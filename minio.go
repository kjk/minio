package minio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/kjk/common/u"
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

func (c *Client) UploadFilePublic(remotePath string, path string) (info minio.UploadInfo, err error) {
	ext := filepath.Ext(remotePath)
	contentType := mime.TypeByExtension(ext)
	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	setPublicObjectMetadata(&opts)
	return c.c.FPutObject(ctx(), c.bucket, remotePath, path, opts)
}

func (c *Client) UploadDataPublic(remotePath string, data []byte) error {
	contentType := u.MimeTypeFromFileName(remotePath)
	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	setPublicObjectMetadata(&opts)
	r := bytes.NewBuffer(data)
	_, err := c.c.PutObject(ctx(), c.bucket, remotePath, r, int64(len(data)), opts)
	return err
}

func (mc *Client) UploadDir(dirRemote string, dirLocal string) error {
	files, err := ioutil.ReadDir(dirLocal)
	if err != nil {
		return err
	}
	for _, f := range files {
		fname := f.Name()
		pathLocal := filepath.Join(dirLocal, fname)
		pathRemote := path.Join(dirRemote, fname)
		_, err := mc.UploadFilePublic(pathRemote, pathLocal)
		if err != nil {
			return fmt.Errorf("upload of '%s' as '%s' failed with '%s'", pathLocal, pathRemote, err)
		}
	}
	return nil
}

func brotliCompress(path string) ([]byte, error) {
	var buf bytes.Buffer
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	w := brotli.NewWriterLevel(&buf, brotli.BestCompression)
	_, err = io.Copy(w, f)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	err = f.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Client) UploadFileBrotliCompressedPublic(remotePath string, path string) (info minio.UploadInfo, err error) {
	// TODO: use io.Pipe() to do compression more efficiently
	d, err := brotliCompress(path)
	if err != nil {
		return
	}
	ext := filepath.Ext(remotePath)
	contentType := mime.TypeByExtension(ext)
	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	setPublicObjectMetadata(&opts)
	r := bytes.NewReader(d)
	fsize := int64(len(d))
	return c.c.PutObject(ctx(), c.bucket, remotePath, r, fsize, opts)
}

func ctx() context.Context {
	return context.Background()
}

func setPublicObjectMetadata(opts *minio.PutObjectOptions) {
	if opts.UserMetadata == nil {
		opts.UserMetadata = map[string]string{}
	}
	opts.UserMetadata["x-amz-acl"] = "public-read"
}
