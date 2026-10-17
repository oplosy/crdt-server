package store_test

// Snapshots and versions in object storage, against a real PostgreSQL and a real
// S3-compatible storage.
//
// The tests that earn their place are the ones about the boundary between the
// two systems: that a row and the object it names agree, that a database holding
// both kinds of row reads both, and that a failure on either side leaves the
// harmless kind of mess rather than the other kind.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/blob"
	"github.com/mesutokul/ycollab/internal/store"
)

const s3EndpointEnv = "YCOLLAB_TEST_S3_ENDPOINT"

// recordingBlobs wraps a real client and can be told to fail, which is the only
// way to check what happens when one of the two systems does not do its part.
type recordingBlobs struct {
	inner store.Blobs

	mu       sync.Mutex
	puts     []string
	deletes  []string
	failPuts bool
}

func (r *recordingBlobs) Put(ctx context.Context, key string, body []byte) error {
	r.mu.Lock()
	fail := r.failPuts
	r.puts = append(r.puts, key)
	r.mu.Unlock()
	if fail {
		return errors.New("the bucket is not answering")
	}
	return r.inner.Put(ctx, key, body)
}

func (r *recordingBlobs) Get(ctx context.Context, key string) ([]byte, error) {
	return r.inner.Get(ctx, key)
}

func (r *recordingBlobs) Delete(ctx context.Context, key string) error {
	r.mu.Lock()
	r.deletes = append(r.deletes, key)
	r.mu.Unlock()
	return r.inner.Delete(ctx, key)
}

func (r *recordingBlobs) deleted(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range r.deletes {
		if k == key {
			return true
		}
	}
	return false
}

// blobStore returns a store writing its blobs to S3, and the wrapper so a
// test can see and interfere with what it does.
func blobStore(t *testing.T) (*store.Store, *recordingBlobs, context.Context) {
	t.Helper()
	endpoint := os.Getenv(s3EndpointEnv)
	if endpoint == "" {
		t.Skipf("%s is not set; start deploy/docker-compose.yml to run these", s3EndpointEnv)
	}
	s, ctx := openStore(t)
	client, err := blob.New(blob.Config{
		Bucket:      "ycollab",
		Region:      "us-east-1",
		Endpoint:    endpoint,
		Prefix:      fmt.Sprintf("store-%d/", time.Now().UnixNano()),
		Credentials: blob.Credentials{AccessKeyID: "ycollab", SecretAccessKey: "ycollab-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingBlobs{inner: client}
	s.UseBlobs(rec, nil)
	return s, rec, ctx
}

func newDoc(t *testing.T, s *store.Store, ctx context.Context, label string) (store.UUID, string) {
	t.Helper()
	name := fmt.Sprintf("%s-%d", label, time.Now().UnixNano())
	id := store.DocumentID(name)
	if _, _, err := s.Ensure(ctx, id, name, store.NilUUID); err != nil {
		t.Fatal(err)
	}
	return id, name
}

// The basic claim: the bytes go to the bucket, the column is empty, and reading
// the document gets them back.
func TestASnapshotGoesToTheBucket(t *testing.T) {
	s, _, ctx := blobStore(t)
	id, name := newDoc(t, s, ctx, "s3-snap")

	snapshot := bytes.Repeat([]byte("a document that is worth storing "), 400)
	seqs, err := appendCurrent(t, s, ctx, id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, ctx, id, snapshot, seqs); err != nil {
		t.Fatal(err)
	}

	doc, err := s.Load(ctx, id, name, store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(doc.Snapshot, snapshot) {
		t.Fatalf("read back %d bytes, want %d", len(doc.Snapshot), len(snapshot))
	}

	// The row names an object and the column holds nothing: the database is not
	// keeping a second copy, which is the entire point.
	key, inline := snapshotPlacement(t, s, id)
	if key == "" {
		t.Fatal("the row does not name an object")
	}
	if !strings.HasPrefix(key, "snapshots/") {
		t.Errorf("key = %q", key)
	}
	if inline != 0 {
		t.Errorf("the snapshot column still holds %d bytes", inline)
	}
}

// A listing has to report what a document costs whichever side of the two
// columns the bytes are on. It reports the stored size, so a compressible
// document is smaller here than the snapshot handed in - what it must not be is
// zero, which is what reading the empty column alone would say.
func TestAListingReportsTheSizeOfABlobBackedDocument(t *testing.T) {
	s, _, ctx := blobStore(t)
	id, name := newDoc(t, s, ctx, "s3-size")

	snapshot := bytes.Repeat([]byte("a document that is worth storing "), 400)
	seqs, err := appendCurrent(t, s, ctx, id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, ctx, id, snapshot, seqs); err != nil {
		t.Fatal(err)
	}

	page, err := s.List(ctx, store.ListRequest{AllOwners: true})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, d := range page.Documents {
		if d.Name != name {
			continue
		}
		found = true
		if d.SnapshotBytes <= 0 {
			t.Errorf("snapshot bytes = %d for a document whose snapshot is an object", d.SnapshotBytes)
		}
		if d.SnapshotBytes > int64(len(snapshot)) {
			t.Errorf("snapshot bytes = %d, more than the %d handed in", d.SnapshotBytes, len(snapshot))
		}
	}
	if !found {
		t.Fatalf("%q is not in the listing", name)
	}
}

// The same claim for history, which is the larger of the two: a version listing
// exists to decide what to fetch, and every row reporting zero would make it
// useless for that.
func TestAVersionListingReportsTheSizeOfABlobBackedVersion(t *testing.T) {
	s, _, ctx := blobStore(t)
	id, _ := newDoc(t, s, ctx, "s3-version-size")

	payload := bytes.Repeat([]byte("history is the expensive part "), 400)
	if written, err := saveVersionCurrent(t, s, ctx, id, store.Version{StateVector: []byte{1}, Payload: payload}, 0); err != nil || !written {
		t.Fatalf("written=%v err=%v", written, err)
	}

	list, err := s.ListVersions(ctx, id, 0)
	if err != nil || len(list) != 1 {
		t.Fatalf("%d versions, err=%v", len(list), err)
	}
	if list[0].Bytes <= 0 {
		t.Errorf("bytes = %d for a version whose payload is an object", list[0].Bytes)
	}
	if list[0].Bytes > len(payload) {
		t.Errorf("bytes = %d, more than the %d handed in", list[0].Bytes, len(payload))
	}
}

func TestAVersionGoesToTheBucket(t *testing.T) {
	s, _, ctx := blobStore(t)
	id, _ := newDoc(t, s, ctx, "s3-version")

	payload := bytes.Repeat([]byte("history is the expensive part "), 400)
	v := store.Version{StateVector: []byte{1}, Payload: payload, Label: "before the migration"}
	if written, err := saveVersionCurrent(t, s, ctx, id, v, 0); err != nil || !written {
		t.Fatalf("written=%v err=%v", written, err)
	}

	list, err := s.ListVersions(ctx, id, 0)
	if err != nil || len(list) != 1 {
		t.Fatalf("%d versions, err=%v", len(list), err)
	}
	got, err := s.LoadVersion(ctx, id, list[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("read back %d bytes, want %d", len(got.Payload), len(payload))
	}
	if got.Label != v.Label {
		t.Errorf("label = %q", got.Label)
	}
}

// The upgrade case, and the reason there are two columns rather than a mode
// switch: a database can hold rows of both kinds and one query reads both.
func TestRowsInTheDatabaseAndInTheBucketReadTheSame(t *testing.T) {
	s, _, ctx := blobStore(t)

	// A document written before object storage was turned on. openStore gives a
	// second handle to the same database with no bucket attached, which is
	// exactly what the old server was.
	plain, _ := openStore(t)
	inlineID, inlineName := newDoc(t, plain, ctx, "s3-mixed-inline")
	inlineBytes := bytes.Repeat([]byte("stored in the database "), 300)
	seqs, err := appendCurrent(t, plain, ctx, inlineID, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, plain, ctx, inlineID, inlineBytes, seqs); err != nil {
		t.Fatal(err)
	}

	// And one written after.
	bucketID, bucketName := newDoc(t, s, ctx, "s3-mixed-bucket")
	bucketBytes := bytes.Repeat([]byte("stored in the bucket "), 300)
	seqs, err = appendCurrent(t, s, ctx, bucketID, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, ctx, bucketID, bucketBytes, seqs); err != nil {
		t.Fatal(err)
	}

	// The server with a bucket reads both.
	for _, tc := range []struct {
		what string
		id   store.UUID
		name string
		want []byte
	}{
		{"a row from before object storage", inlineID, inlineName, inlineBytes},
		{"a row from after it", bucketID, bucketName, bucketBytes},
	} {
		doc, err := s.Load(ctx, tc.id, tc.name, store.NilUUID)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		if !bytes.Equal(doc.Snapshot, tc.want) {
			t.Errorf("%s: read back %d bytes, want %d", tc.what, len(doc.Snapshot), len(tc.want))
		}
	}

	// And the two rows really are stored differently, or this test compares
	// nothing.
	if key, _ := snapshotPlacement(t, s, inlineID); key != "" {
		t.Error("the row meant to be in the database names an object")
	}
	if key, _ := snapshotPlacement(t, s, bucketID); key == "" {
		t.Error("the row meant to be in the bucket does not name an object")
	}
}

// A server that has had its bucket taken away must say so rather than serve an
// empty document, which would read as a document somebody had emptied.
func TestARowNamingAnObjectWithNoBucketIsAnError(t *testing.T) {
	s, _, ctx := blobStore(t)
	id, name := newDoc(t, s, ctx, "s3-orphaned-config")
	seqs, err := appendCurrent(t, s, ctx, id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, ctx, id, bytes.Repeat([]byte("content "), 100), seqs); err != nil {
		t.Fatal(err)
	}

	plain, _ := openStore(t) // No bucket.
	_, err = plain.Load(ctx, id, name, store.NilUUID)
	if err == nil {
		t.Fatal("a document whose bytes are in a bucket loaded without one")
	}
	if !strings.Contains(err.Error(), "object storage") {
		t.Errorf("the error does not say what is wrong: %v", err)
	}
}

// The ordering claim, checked rather than asserted. A put that fails must leave
// no row: the alternative is a row naming bytes that are not there, which is a
// document that cannot be read.
func TestAFailedPutWritesNoRow(t *testing.T) {
	s, rec, ctx := blobStore(t)
	id, name := newDoc(t, s, ctx, "s3-failed-put")

	seqs, err := appendCurrent(t, s, ctx, id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	rec.failPuts = true
	rec.mu.Unlock()

	if err := compactCurrent(t, s, ctx, id, bytes.Repeat([]byte("never lands "), 100), seqs); err == nil {
		t.Fatal("compaction succeeded with a bucket that was refusing writes")
	}
	// No snapshot, and the log row survives, so the document is exactly as it
	// was and the next compaction will try again.
	key, inline := snapshotPlacement(t, s, id)
	if key != "" || inline != 0 {
		t.Errorf("a failed put left a snapshot behind: key=%q inline=%d", key, inline)
	}
	doc, err := s.Load(ctx, id, name, store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Updates) != 1 {
		t.Errorf("%d log rows survived a failed compaction, want 1", len(doc.Updates))
	}

	// The same for a version: no object, no row.
	if written, err := saveVersionCurrent(t, s, ctx, id, store.Version{StateVector: []byte{7}, Payload: []byte("x")}, 0); err == nil && written {
		t.Error("a version was written with a bucket that was refusing writes")
	}
}

// Deleting a document takes its objects with it, or every deleted document is a
// permanent line on the storage bill.
func TestDeletingADocumentDeletesItsObjects(t *testing.T) {
	s, rec, ctx := blobStore(t)
	id, _ := newDoc(t, s, ctx, "s3-delete")

	seqs, err := appendCurrent(t, s, ctx, id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, ctx, id, bytes.Repeat([]byte("snapshot "), 100), seqs); err != nil {
		t.Fatal(err)
	}
	if _, err := saveVersionCurrent(t, s, ctx, id,
		store.Version{StateVector: []byte{3}, Payload: bytes.Repeat([]byte("version "), 100)}, 0); err != nil {
		t.Fatal(err)
	}
	snapKey, _ := snapshotPlacement(t, s, id)
	versionKeys := versionKeysOf(t, s, id)
	if snapKey == "" || len(versionKeys) != 1 {
		t.Fatalf("nothing to delete: snapshot=%q versions=%v", snapKey, versionKeys)
	}

	if existed, err := s.Delete(ctx, id); err != nil || !existed {
		t.Fatalf("delete: existed=%v err=%v", existed, err)
	}
	for _, key := range append([]string{snapKey}, versionKeys...) {
		if !rec.deleted(key) {
			t.Errorf("%s was left in the bucket", key)
		}
		if _, err := rec.Get(ctx, key); !errors.Is(err, blob.ErrNotFound) {
			t.Errorf("%s is still readable: %v", key, err)
		}
	}
}

// Pruning history is the operation that runs most often, so a leak here is the
// one that would actually grow.
func TestPruningVersionsDeletesTheirObjects(t *testing.T) {
	s, rec, ctx := blobStore(t)
	id, _ := newDoc(t, s, ctx, "s3-prune")

	for i := range 5 {
		v := store.Version{
			StateVector: []byte{byte(i)},
			Payload:     bytes.Repeat([]byte(fmt.Sprintf("version %d ", i)), 100),
		}
		if written, err := saveVersionCurrent(t, s, ctx, id, v, 0); err != nil || !written {
			t.Fatalf("version %d: written=%v err=%v", i, written, err)
		}
	}
	before := versionKeysOf(t, s, id)
	if len(before) != 5 {
		t.Fatalf("%d version objects, want 5", len(before))
	}

	n, err := pruneVersionsCurrent(t, s, ctx, id, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("pruned %d, want 3", n)
	}
	after := versionKeysOf(t, s, id)
	if len(after) != 2 {
		t.Fatalf("%d versions left, want 2", len(after))
	}
	kept := map[string]bool{}
	for _, k := range after {
		kept[k] = true
	}
	for _, k := range before {
		if kept[k] {
			continue
		}
		if !rec.deleted(k) {
			t.Errorf("pruned version %s was left in the bucket", k)
		}
	}
	// And what was kept is still readable, which a too-eager delete would break.
	list, err := s.ListVersions(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range list {
		if _, err := s.LoadVersion(ctx, id, v.ID); err != nil {
			t.Errorf("a kept version is unreadable: %v", err)
		}
	}
}

// A new snapshot supersedes the old one, and the old object goes - otherwise
// every compaction of every document leaves a copy behind forever.
func TestANewSnapshotRemovesTheOneItReplaces(t *testing.T) {
	s, rec, ctx := blobStore(t)
	id, name := newDoc(t, s, ctx, "s3-supersede")

	var keys []string
	for i := range 3 {
		seqs, err := appendCurrent(t, s, ctx, id, [][]byte{{0, 0}})
		if err != nil {
			t.Fatal(err)
		}
		if err := compactCurrent(t, s, ctx, id, bytes.Repeat([]byte(fmt.Sprintf("round %d ", i)), 100), seqs); err != nil {
			t.Fatal(err)
		}
		key, _ := snapshotPlacement(t, s, id)
		keys = append(keys, key)
	}
	// Each round wrote a different key - the sequence number is in it - and
	// every one but the last has been removed.
	for i, key := range keys[:len(keys)-1] {
		if key == keys[len(keys)-1] {
			t.Fatalf("round %d reused the final key; the sequence number is not in it", i)
		}
		if !rec.deleted(key) {
			t.Errorf("the snapshot from round %d was left in the bucket: %s", i, key)
		}
	}
	// And the document still reads.
	doc, err := s.Load(ctx, id, name, store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(doc.Snapshot, []byte("round 2")) {
		t.Error("the document does not hold the last snapshot written")
	}
}

// snapshotPlacement reports where a document's snapshot actually is.
func snapshotPlacement(t *testing.T, s *store.Store, id store.UUID) (string, int) {
	t.Helper()
	var key string
	var inline int
	if err := s.QueryRowForTest(t.Context(),
		`SELECT snapshot_key, coalesce(octet_length(snapshot), 0) FROM documents WHERE id = $1`,
		id).Scan(&key, &inline); err != nil {
		t.Fatal(err)
	}
	return key, inline
}

func versionKeysOf(t *testing.T, s *store.Store, id store.UUID) []string {
	t.Helper()
	var keys []string
	rows, err := s.QueryForTest(t.Context(),
		`SELECT blob_key FROM doc_versions WHERE doc_id = $1 AND blob_key <> '' ORDER BY id`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	return keys
}
