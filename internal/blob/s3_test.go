package blob_test

// The S3 client, against a real S3-compatible service.
//
// This is the only kind of test worth writing for a hand-rolled signer. A mock
// that checks the signature against my own idea of the algorithm would agree
// with every mistake I made; RustFS implements SigV4 independently and answers
// 403 SignatureDoesNotMatch to anything wrong. Every test here that passes is a
// test that the signature was accepted by something that did not learn it from
// this repository.
//
//	docker compose -f deploy/docker-compose.yml up -d rustfs
//	AWS_ACCESS_KEY_ID=ycollab AWS_SECRET_ACCESS_KEY=ycollab-secret AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url http://127.0.0.1:9002 s3api create-bucket --bucket ycollab
//	YCOLLAB_TEST_S3_ENDPOINT=http://127.0.0.1:9002 go test ./internal/blob/

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/blob"
)

const (
	endpointEnv = "YCOLLAB_TEST_S3_ENDPOINT"
	testBucket  = "ycollab"
	testKey     = "ycollab"
	testSecret  = "ycollab-secret"
)

func newClient(t *testing.T) (*blob.Client, context.Context) {
	t.Helper()
	endpoint := os.Getenv(endpointEnv)
	if endpoint == "" {
		t.Skipf("%s is not set; start deploy/docker-compose.yml to run these", endpointEnv)
	}
	c, err := blob.New(blob.Config{
		Bucket:      testBucket,
		Region:      "us-east-1",
		Endpoint:    endpoint,
		Prefix:      fmt.Sprintf("test-%d/", time.Now().UnixNano()),
		Credentials: blob.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, t.Context()
}

// The signature is accepted by a real S3 implementation, and the bytes survive.
func TestAnObjectRoundTrips(t *testing.T) {
	c, ctx := newClient(t)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"a snapshot", bytes.Repeat([]byte("document "), 5000)},
		{"binary", []byte{0x00, 0xff, 0x1b, 0x00, 0x7f, 0x80}},
		{"one byte", []byte{0x00}},
		{"empty", []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := "round-trip/" + strings.ReplaceAll(tc.name, " ", "-")
			if err := c.Put(ctx, key, tc.body); err != nil {
				t.Fatalf("put: %v", err)
			}
			got, err := c.Get(ctx, key)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !bytes.Equal(got, tc.body) {
				t.Fatalf("%d bytes in, %d out", len(tc.body), len(got))
			}
		})
	}
}

// A missing object is its own error, because "this document was deleted" and
// "the bucket is unreachable" call for opposite responses.
func TestAMissingObjectIsNotFound(t *testing.T) {
	c, ctx := newClient(t)
	_, err := c.Get(ctx, "nothing/here")
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	exists, err := c.Exists(ctx, "nothing/here")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists {
		t.Error("an object that was never written exists")
	}
}

func TestDeleteRemovesAnObjectAndIsIdempotent(t *testing.T) {
	c, ctx := newClient(t)
	const key = "delete/me"
	if err := c.Put(ctx, key, []byte("some bytes")); err != nil {
		t.Fatal(err)
	}
	if exists, err := c.Exists(ctx, key); err != nil || !exists {
		t.Fatalf("exists=%v err=%v after a put", exists, err)
	}
	if err := c.Delete(ctx, key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if exists, err := c.Exists(ctx, key); err != nil || exists {
		t.Fatalf("exists=%v err=%v after a delete", exists, err)
	}
	// Deleting again is not an error: the caller wanted it gone and it is gone.
	if err := c.Delete(ctx, key); err != nil {
		t.Errorf("deleting an absent object: %v", err)
	}
}

// A key is a path, and the signer has to encode it the way S3 does. These are
// the shapes this server actually writes plus the ones that break a naive
// encoder.
func TestKeysWithAwkwardCharacters(t *testing.T) {
	c, ctx := newClient(t)
	keys := []string{
		"snapshots/f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
		"versions/f81d4fae-7dec-11d0-a765-00a0c91e6bf6/42",
		"deep/nested/path/with/many/segments",
		"has spaces in it",
		"plus+and=equals",
		"tilde~and.dots",
		"unicode-ünïcödé",
		"percent%25encoded",
		"ampersand&question?mark",
	}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			want := []byte("content for " + key)
			if err := c.Put(ctx, key, want); err != nil {
				t.Fatalf("put: %v", err)
			}
			got, err := c.Get(ctx, key)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("got %q", got)
			}
		})
	}
}

// A wrong secret must be refused. Without this the round-trip tests would pass
// against a service that was not checking signatures at all, and would prove
// nothing about the signer.
func TestABadSecretIsRefused(t *testing.T) {
	endpoint := os.Getenv(endpointEnv)
	if endpoint == "" {
		t.Skipf("%s is not set", endpointEnv)
	}
	c, err := blob.New(blob.Config{
		Bucket:      testBucket,
		Region:      "us-east-1",
		Endpoint:    endpoint,
		Credentials: blob.Credentials{AccessKeyID: testKey, SecretAccessKey: "not-the-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Put(t.Context(), "should-not-land", []byte("hello"))
	if err == nil {
		t.Fatal("a request signed with the wrong secret was accepted")
	}
	if !strings.Contains(err.Error(), "403") && !strings.Contains(strings.ToLower(err.Error()), "signature") {
		t.Errorf("refused, but not as a signature failure: %v", err)
	}
}

// The prefix is what lets one bucket hold more than one deployment, so it has to
// actually be part of the key rather than decoration.
func TestThePrefixNamespacesTheKeys(t *testing.T) {
	endpoint := os.Getenv(endpointEnv)
	if endpoint == "" {
		t.Skipf("%s is not set", endpointEnv)
	}
	run := time.Now().UnixNano()
	build := func(prefix string) *blob.Client {
		c, err := blob.New(blob.Config{
			Bucket: testBucket, Region: "us-east-1", Endpoint: endpoint,
			Prefix:      fmt.Sprintf("tenant-%d-%s/", run, prefix),
			Credentials: blob.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := build("a"), build("b")
	if err := a.Put(t.Context(), "shared-key", []byte("a's bytes")); err != nil {
		t.Fatal(err)
	}
	// The same key under a different prefix is a different object.
	if _, err := b.Get(t.Context(), "shared-key"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the second prefix saw the first one's object: %v", err)
	}
	got, err := a.Get(t.Context(), "shared-key")
	if err != nil || string(got) != "a's bytes" {
		t.Fatalf("got %q err=%v", got, err)
	}
}

// Overwriting is how a snapshot is replaced, so last write has to win.
func TestPutOverwrites(t *testing.T) {
	c, ctx := newClient(t)
	const key = "overwrite/me"
	if err := c.Put(ctx, key, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, key, []byte("second")); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Fatalf("got %q, want the second write", got)
	}
}

func TestNewRejectsAnUnusableConfiguration(t *testing.T) {
	creds := blob.Credentials{AccessKeyID: "k", SecretAccessKey: "s"}
	for _, tc := range []struct {
		name string
		cfg  blob.Config
	}{
		{"no bucket", blob.Config{Region: "us-east-1", Credentials: creds}},
		{"no region", blob.Config{Bucket: "b", Credentials: creds}},
		{"no credentials", blob.Config{Bucket: "b", Region: "us-east-1"}},
		{"unparseable endpoint", blob.Config{Bucket: "b", Region: "us-east-1", Endpoint: "://nope", Credentials: creds}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Cleared so the environment of whoever runs the tests cannot make
			// the "no credentials" case pass.
			t.Setenv("AWS_ACCESS_KEY_ID", "")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "")
			if _, err := blob.New(tc.cfg); err == nil {
				t.Error("accepted")
			}
		})
	}
}

// Credentials fall back to the variables every S3 tool honours.
func TestCredentialsComeFromTheEnvironment(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "from-env")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "also-from-env")
	if _, err := blob.New(blob.Config{Bucket: "b", Region: "us-east-1"}); err != nil {
		t.Fatalf("credentials in the environment were not used: %v", err)
	}
}

// A bucket that does not exist is not an object that does not exist.
//
// S3 answers both with a 404, and reading them as the same thing sends whoever
// is debugging a mistyped -s3-bucket to look for a document that was never the
// problem. It is also unsafe: Delete treats a missing object as success, so a
// bucket that is not there would have every delete report that it worked.
//
// This test was written after the real thing happened - the tests ran against a
// MinIO whose bucket had been wiped, and every failure said "no such object".
func TestAMissingBucketIsNotAMissingObject(t *testing.T) {
	endpoint := os.Getenv(endpointEnv)
	if endpoint == "" {
		t.Skipf("%s is not set; start deploy/docker-compose.yml to run these", endpointEnv)
	}
	c, err := blob.New(blob.Config{
		Bucket:      fmt.Sprintf("nobody-created-this-%d", time.Now().UnixNano()),
		Region:      "us-east-1",
		Endpoint:    endpoint,
		Credentials: blob.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	for _, tc := range []struct {
		op  string
		run func() error
	}{
		{"put", func() error { return c.Put(ctx, "a/key", []byte("x")) }},
		{"get", func() error { _, err := c.Get(ctx, "a/key"); return err }},
		// The one that matters most: without this, a mistyped bucket means
		// every delete silently succeeds and nothing is ever removed.
		{"delete", func() error { return c.Delete(ctx, "a/key") }},
	} {
		t.Run(tc.op, func(t *testing.T) {
			err := tc.run()
			if !errors.Is(err, blob.ErrNoBucket) {
				t.Fatalf("err = %v, want ErrNoBucket", err)
			}
			if errors.Is(err, blob.ErrNotFound) {
				t.Error("a missing bucket also reported as a missing object")
			}
			// And the message names the bucket, not a key nobody asked about.
			if !strings.Contains(err.Error(), "nobody-created-this") {
				t.Errorf("the error does not say which bucket: %v", err)
			}
		})
	}

	// A missing object in a bucket that *does* exist is still ErrNotFound, and
	// deleting it is still success. Otherwise the fix above would have turned
	// every ordinary miss into an error.
	good, _ := newClient(t)
	if _, err := good.Get(ctx, "not/here"); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("a missing object: %v, want ErrNotFound", err)
	}
	if err := good.Delete(ctx, "not/here"); err != nil {
		t.Errorf("deleting a missing object: %v, want success", err)
	}
}
