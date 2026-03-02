/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package s3check provides a minimal S3 client for checking whether a restic
// repository already exists in a bucket.  It is ported directly from pvc-plumber's
// internal/s3/client.go with only the module path changed.
package s3check

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Client wraps a MinIO client for backup-existence checks.
type Client struct {
	minioClient *minio.Client
	bucket      string
}

// NewClient constructs an S3 client.  endpoint must not include a scheme.
// secure controls whether TLS is used.
func NewClient(endpoint, bucket, accessKey, secretKey string, secure bool) (*Client, error) {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}
	return &Client{minioClient: mc, bucket: bucket}, nil
}

// CheckBackupExists returns true if at least one object exists under repoPath
// in the bucket.  repoPath should not have a trailing slash.
func (c *Client) CheckBackupExists(ctx context.Context, repoPath string) (bool, error) {
	prefix := repoPath + "/"

	opts := minio.ListObjectsOptions{
		Prefix:  prefix,
		MaxKeys: 1,
	}

	objectCh := c.minioClient.ListObjects(ctx, c.bucket, opts)

	object, ok := <-objectCh
	if !ok {
		// Channel closed with no objects — repository does not exist yet.
		return false, nil
	}
	if object.Err != nil {
		return false, fmt.Errorf("list objects under %q: %w", prefix, object.Err)
	}
	return true, nil
}
