package mutationbackend

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/juicedata/juicefs/pkg/compress"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

var sdkVolumeName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
var sdkRegion = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

func validSDKFormat(f meta.Format) bool {
	return sdkVolumeName.MatchString(f.Name) && f.BlockSize >= 64 && f.BlockSize <= 16384 &&
		f.BlockSize&(f.BlockSize-1) == 0 && compress.NewCompressor(f.Compression) != nil &&
		f.Shards >= 0 && f.Shards <= 256 && f.RangerRestUrl == "" && f.RangerService == "" &&
		f.UploadLimit >= 0 && f.UploadLimit <= math.MaxInt64/1_000_000 &&
		f.DownloadLimit >= 0 && f.DownloadLimit <= math.MaxInt64/1_000_000 &&
		(f.EncryptKey != "" || f.EncryptAlgo == "")
}

// Reject implicit SDK capabilities before constructing a provider. The explicit
// settings below replace AWS shared-file discovery in this dedicated process.
func cleanSDKEnvironment() bool {
	for _, entry := range os.Environ() {
		name, value, _ := strings.Cut(entry, "=")
		if value == "" {
			continue
		}
		if strings.HasPrefix(name, "AWS_") || strings.HasPrefix(name, "MINIO_") || strings.HasPrefix(name, "JFS_") {
			return false
		}
		switch strings.ToUpper(name) {
		case "META_PASSWORD", "REDIS_PASSWORD", "SENTINEL_PASSWORD", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY":
			return false
		}
	}
	return true
}

func providerEnvironment(cfg SDKConfig) error {
	if !sdkRegion.MatchString(cfg.Region) || cfg.PathStyle == nil {
		return ErrUnavailable
	}
	// JuiceFS's factories call LoadDefaultConfig even with static credentials.
	// Disable shared profiles and instance metadata instead of reading the host's
	// AWS files. This is irreversible process setup; only one SDK is permitted.
	settings := map[string]string{"AWS_CONFIG_FILE": os.DevNull, "AWS_SHARED_CREDENTIALS_FILE": os.DevNull,
		"AWS_EC2_METADATA_DISABLED": "true", "AWS_REGION": cfg.Region,
		"AWS_DEFAULT_REGION": cfg.Region, "MINIO_REGION": cfg.Region,
		"JFS_S3_VHOST_STYLE": strconv.FormatBool(!*cfg.PathStyle)}
	for key, value := range settings {
		if err := os.Setenv(key, value); err != nil {
			return ErrUnavailable
		}
	}
	return nil
}

func sdkStorage(f meta.Format, cfg SDKConfig) (object.ObjectStorage, error) {
	return sdkStorageContext(context.Background(), f, cfg)
}

func sdkStorageContext(ctx context.Context, f meta.Format, cfg SDKConfig) (object.ObjectStorage, error) {
	return sdkStorageRecorded(ctx, f, cfg, nil)
}

func sdkStorageRecorded(ctx context.Context, f meta.Format, cfg SDKConfig, recorder objectRecorder) (object.ObjectStorage, error) {
	if f.KeyEncrypted {
		// The pinned SDK slices nonce[:12] without checking decoded length.
		for _, value := range []string{f.SecretKey, f.SessionToken, f.EncryptKey} {
			if value != "" {
				b, err := base64.StdEncoding.DecodeString(value)
				if err != nil || len(b) < 28 {
					return nil, ErrUnavailable
				}
			}
		}
	}
	if err := f.Decrypt(); err != nil {
		return nil, ErrUnavailable
	}
	storage := strings.ToLower(f.Storage)
	if storage != "file" && storage != "s3" && storage != "minio" {
		return nil, ErrUnavailable
	}
	if storage == "file" {
		if cfg.Region != "" || cfg.PathStyle != nil || f.AccessKey != "" || f.SecretKey != "" || f.SessionToken != "" {
			return nil, ErrUnavailable
		}
	} else {
		if f.AccessKey == "" || f.AccessKey == "anonymous" || f.SecretKey == "" || !sdkRegion.MatchString(cfg.Region) || cfg.PathStyle == nil {
			return nil, ErrUnavailable
		}
	}
	count := max(1, f.Shards)
	seen := make(map[string]bool, count)
	for i := range count {
		bucket := f.Bucket
		if f.Shards > 1 {
			bucket = fmt.Sprintf(bucket, i)
			if strings.Contains(bucket, "%!") {
				return nil, ErrUnavailable
			}
		}
		if seen[bucket] || !validSDKBucket(storage, bucket, cfg.Region) {
			return nil, ErrUnavailable
		}
		seen[bucket] = true
	}
	if storage != "file" {
		if err := providerEnvironment(cfg); err != nil {
			return nil, err
		}
	}
	var blob object.ObjectStorage
	var err error
	if f.Shards > 1 {
		blob, err = object.NewSharded(storage, f.Bucket, f.AccessKey, f.SecretKey, f.SessionToken, f.Shards)
	} else {
		blob, err = object.CreateStorage(storage, f.Bucket, f.AccessKey, f.SecretKey, f.SessionToken)
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	// Verify the raw prefixed ciphertext through the same shard routing before
	// reporting PUT completion. Keep the upstream prefix/encryption order.
	raw := blob
	blob = &verifiedObjects{ObjectStorage: raw, recorder: recorder}
	// Match JuiceFS cmd/format.go:createStorage wrapper order exactly.
	blob = object.WithPrefix(blob, f.Name+"/")
	if f.StorageClass != "" {
		setter, ok := blob.(object.SupportStorageClass)
		if !ok || setter.SetStorageClass(f.StorageClass) != nil {
			return nil, ErrUnavailable
		}
	}
	var passphrase []byte
	if cfg.EncryptionPassphraseFile != "" {
		passphrase, err = privateRead(cfg.EncryptionPassphraseFile, 4096)
		if err != nil || digest(passphrase) != cfg.EncryptionPassphraseSHA256 || f.EncryptKey == "" {
			return nil, ErrUnavailable
		}
	}
	if f.EncryptKey != "" {
		key, err := object.ParseRsaPrivateKeyFromPem([]byte(f.EncryptKey), passphrase)
		if err != nil {
			return nil, ErrUnavailable
		}
		encryptor, err := object.NewDataEncryptor(object.NewRSAEncryptor(key), f.EncryptAlgo)
		if err != nil {
			return nil, ErrUnavailable
		}
		blob = object.NewEncrypted(blob, encryptor)
	}
	if err := qualifyCreateOnly(ctx, raw, f.Name, count); err != nil {
		return nil, ErrUnavailable
	}
	return blob, nil
}

func validSDKBucket(storage, bucket, region string) bool {
	if storage == "file" {
		// The SDK concatenates root and key; a trailing slash is significant.
		return filepath.IsAbs(bucket) && strings.HasSuffix(bucket, "/") &&
			filepath.Clean(bucket)+"/" == bucket && !strings.ContainsAny(bucket, "?\x00")
	}
	u, err := url.Parse(bucket)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return false
	}
	// Admit unambiguous endpoint/bucket layout. Refuse unsupported TLS URL
	// switches (including insecure verification), paths and unknown options.
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 1 || parts[0] == "" || parts[0] == "." || parts[0] == ".." || u.RawPath != "" {
		return false
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return false
	}
	for key, values := range q {
		if len(values) != 1 {
			return false
		}
		if storage == "minio" && key == "region" && values[0] == region {
			continue
		}
		if storage == "s3" && (key == "disable-100-continue" || key == "disable-checksum") && (values[0] == "true" || values[0] == "false") {
			continue
		}
		return false
	}
	if storage == "s3" && strings.Contains(u.Hostname(), ".amazonaws.com") {
		// The upstream S3 constructor derives region from AWS hostnames before
		// consulting explicit environment. Require agreement with configuration.
		host := u.Hostname()
		if !strings.HasPrefix(host, "s3.") && !strings.HasPrefix(host, "s3-") {
			return false
		}
		host = strings.TrimPrefix(strings.TrimPrefix(host[3:], "dualstack."), "dualstack-")
		actual, _, _ := strings.Cut(host, ".")
		if actual == "amazonaws" {
			actual = "us-east-1"
		}
		if actual != region {
			return false
		}
	}
	return true
}
