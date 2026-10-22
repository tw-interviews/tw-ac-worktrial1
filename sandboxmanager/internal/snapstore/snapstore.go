// Package snapstore keeps sandbox workspace snapshots (tar streams, see
// internal/workspace) in S3, under <prefix><sandbox id>/<uuid>.tar.
//
// Configuration (both the API and the node agent): SNAPSHOT_BUCKET
// (required), SNAPSHOT_PREFIX (default "sandboxes/"), and optionally
// S3_ENDPOINT for S3-compatible stores (path-style). Credentials and region
// come from the default AWS chain (EKS Pod Identity in the cluster).
package snapstore

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

type Store struct {
	client *s3.Client
	up     *manager.Uploader
	bucket string
	prefix string
}

// FromEnv configures a Store from the environment.
func FromEnv(ctx context.Context) (*Store, error) {
	bucket := os.Getenv("SNAPSHOT_BUCKET")
	if bucket == "" {
		return nil, errors.New("SNAPSHOT_BUCKET is required")
	}
	prefix := os.Getenv("SNAPSHOT_PREFIX")
	if prefix == "" {
		prefix = "sandboxes/"
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if ep := os.Getenv("S3_ENDPOINT"); ep != "" {
			o.BaseEndpoint, o.UsePathStyle = aws.String(ep), true
		}
	})
	// Uploads stream from tar of unknown length, so they're multipart; keep
	// the buffered parts small (PartSize x Concurrency per upload).
	up := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize, u.Concurrency = 8<<20, 2
	})
	return &Store{client: client, up: up, bucket: bucket, prefix: prefix}, nil
}

// NewKey returns a fresh object key for a snapshot of sandboxID. Each
// snapshot gets its own key, so a failed upload never replaces a good one.
func (s *Store) NewKey(sandboxID string) string {
	return s.prefix + sandboxID + "/" + uuid.NewString() + ".tar"
}

// Put uploads r to key and returns the number of bytes stored. If r fails,
// nothing is stored.
func (s *Store) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	cr := &countingReader{r: r}
	_, err := s.up.Upload(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket, Key: &key, Body: cr, ContentType: aws.String("application/x-tar"),
	})
	return cr.n, err
}

// Get streams key. The caller closes it.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// Delete removes key (missing keys are fine).
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	return err
}

// DeleteAll removes every snapshot of sandboxID, including ones left over
// by interrupted operations.
func (s *Store) DeleteAll(ctx context.Context, sandboxID string) error {
	prefix := s.prefix + sandboxID + "/"
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return err
		}
		if len(page.Contents) == 0 {
			continue
		}
		ids := make([]types.ObjectIdentifier, len(page.Contents))
		for i, o := range page.Contents {
			ids[i] = types.ObjectIdentifier{Key: o.Key}
		}
		out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &s.bucket, Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)}})
		if err != nil {
			return err
		}
		if len(out.Errors) > 0 {
			return errors.New("delete snapshots: " + aws.ToString(out.Errors[0].Message))
		}
	}
	return nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
