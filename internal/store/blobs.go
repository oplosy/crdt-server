package store

// Putting the big blobs somewhere other than PostgreSQL.
//
// Two columns decide where each blob is, per row: an empty key means the bytes
// are in the BYTEA column next to it, and a non-empty key means they are an
// object. So a database can hold both at once. Turning object storage on
// migrates nothing and reads what is already there; turning it off leaves the
// objects readable, because the rows still name them.
//
// # The order of operations, which is the whole of the correctness argument
//
// Writing: the object first, then the row. If the object lands and the row does
// not, the object is an orphan - wasted storage, and nothing else. If the row
// landed first and the object did not, the row would point at nothing, which is
// a document that cannot be read. One of those is a bill and the other is data
// loss.
//
// Deleting: the row first, then the object. Same argument in reverse. A row
// deleted whose object survives is an orphan; an object deleted whose row
// survives is a version that has quietly become unreadable.
//
// Orphans are possible by design after a failure between the object write and
// row commit, or when a commit succeeds but its response is lost. Snapshot
// objects use unique per-attempt keys, so retries cannot overwrite a live
// snapshot; orphan cleanup remains an operational task.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/mesutokul/ycollab/internal/blob"
	"github.com/mesutokul/ycollab/internal/pack"
)

// Blobs is the part of internal/blob this package needs. An interface rather
// than the concrete client so a test can make a put fail on demand, which is the
// only way to check that the ordering above actually holds.
type Blobs interface {
	Put(ctx context.Context, key string, body []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

// UseBlobs sends snapshots and version payloads to object storage from now on.
// Rows already written stay where they are and stay readable.
func (s *Store) UseBlobs(b Blobs, log *slog.Logger) {
	s.blobs = b
	if log != nil {
		s.log = log
	}
}

// uniqueSnapshotKey names one immutable snapshot attempt. A key per attempt
// prevents a losing same-watermark compaction from overwriting bytes named by
// the winner's row.
func uniqueSnapshotKey(id UUID) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("store: snapshot key: %w", err)
	}
	return fmt.Sprintf("snapshots/%s/%s", id, hex.EncodeToString(b[:])), nil
}

// versionKey names a version's payload object.
//
// Random rather than derived from the version's id, because the id comes from an
// identity column and is only known after the insert - and the insert has to
// come second. Random rather than a hash of the content, because two versions
// with identical payloads would then share an object, and deleting one would
// take the other's bytes away.
func versionKey(id UUID) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("store: version key: %w", err)
	}
	return fmt.Sprintf("versions/%s/%s", id, hex.EncodeToString(b[:])), nil
}

// putVersionBlob writes a version payload under a fresh key.
func (s *Store) putVersionBlob(ctx context.Context, id UUID, body []byte) (string, error) {
	if s.blobs == nil {
		return "", nil
	}
	key, err := versionKey(id)
	if err != nil {
		return "", err
	}
	return s.putBlob(ctx, key, body)
}

// putBlob writes a payload to object storage, returning the key to record.
// Returns an empty key when object storage is not configured, which is the
// signal to store the bytes in the column instead. Callers always write the
// object before the row (see package comment), so a failure here leaves no
// orphan.
func (s *Store) putBlob(ctx context.Context, key string, body []byte) (string, error) {
	if s.blobs == nil {
		return "", nil
	}
	if err := s.blobs.Put(ctx, key, body); err != nil {
		return "", err
	}
	return key, nil
}

// readBlob returns the bytes for a row, from wherever the row says they are.
//
// A row naming an object on a server with no object storage configured is a real
// situation - somebody removed the flag - and it gets an error that says so,
// rather than an empty document that looks like a document with nothing in it.
func (s *Store) readBlob(ctx context.Context, key string, inline []byte, codec pack.Codec) ([]byte, error) {
	if key == "" {
		return pack.Unpack(inline, codec)
	}
	if s.blobs == nil {
		return nil, fmt.Errorf("store: %s is in object storage, and this server has none configured", key)
	}
	body, err := s.blobs.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return pack.Unpack(body, codec)
}

// dropBlob deletes an object whose row has already gone. Failures are logged
// rather than returned: the row is committed, the caller's operation succeeded,
// and turning a leaked object into a failed delete would be reporting the wrong
// thing. What is left is an orphan, and the runbook says how to find them.
func (s *Store) dropBlob(ctx context.Context, key string) {
	if key == "" || s.blobs == nil {
		return
	}
	if err := s.blobs.Delete(ctx, key); err != nil {
		s.log.Warn("could not delete an object whose row is gone; it is now an orphan",
			"key", key, "err", err)
	}
}

// dropBlobs is dropBlob over a list.
func (s *Store) dropBlobs(ctx context.Context, keys []string) {
	for _, k := range keys {
		s.dropBlob(ctx, k)
	}
}

// blobKeysFor collects the object keys a document owns, so a delete can remove
// them after the rows have gone.
func (s *Store) blobKeysFor(ctx context.Context, id UUID) ([]string, error) {
	if s.blobs == nil {
		return nil, nil
	}
	var keys []string
	rows, err := s.pool.Query(ctx,
		`SELECT snapshot_key FROM documents WHERE id = $1 AND snapshot_key <> ''
		 UNION ALL
		 SELECT blob_key FROM doc_versions WHERE doc_id = $1 AND blob_key <> ''`, id)
	if err != nil {
		return nil, fmt.Errorf("store: read object keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("store: read object keys: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// Compile-time proof that the real client satisfies the interface, so a change
// to either is a build failure rather than a runtime surprise.
var _ Blobs = (*blob.Client)(nil)
