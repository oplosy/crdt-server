package store_test

// Compression, against a real PostgreSQL.
//
// The test that earns its place here is the mixed one: a row written before
// compression existed and a row written after it have to read back the same way,
// out of the same table, in the same query. That is what an upgrade looks like,
// and it cannot be checked against a fake.

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/store"
)

// compressible is a payload deflate can do something with, big enough to be
// over the threshold. Real Yjs updates compress about twofold; this one
// compresses further, which is fine - the assertion is that it round trips, not
// that it hits a particular ratio.
func compressible(n int) []byte {
	return bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog "), n)
}

// compact writes a snapshot the way a room does: a log row exists, and the
// snapshot folds it in. Compact with nothing folded is a documented no-op, so a
// test that skipped this step would silently assert nothing.
func compact(t *testing.T, s *store.Store, id store.UUID, snapshot []byte) {
	t.Helper()
	seqs, err := s.Append(t.Context(), id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, t.Context(), id, snapshot, seqs); err != nil {
		t.Fatal(err)
	}
}

func TestASnapshotSurvivesCompression(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("packed-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	if _, err := s.Ensure(ctx, id, name, store.NilUUID); err != nil {
		t.Fatal(err)
	}

	snapshot := compressible(200)
	compact(t, s, id, snapshot)
	doc, err := s.Load(ctx, id, name, store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(doc.Snapshot, snapshot) {
		t.Fatalf("the snapshot came back as %d bytes, want %d", len(doc.Snapshot), len(snapshot))
	}

	// And it really was stored smaller, or this feature is doing nothing.
	stored := storedSnapshotSize(t, s, id)
	if stored >= len(snapshot) {
		t.Errorf("stored %d bytes for a %d byte snapshot; it was not compressed", stored, len(snapshot))
	}
	t.Logf("snapshot: %d bytes stored as %d (%.2fx)", len(snapshot), stored, float64(len(snapshot))/float64(stored))
}

// The upgrade case. A row written the old way - no codec, raw bytes - sits next
// to one written the new way, and both have to read back correctly.
func TestRowsWrittenBeforeCompressionStillRead(t *testing.T) {
	s, ctx := openStore(t)
	run := time.Now().UnixNano()

	oldName := fmt.Sprintf("legacy-snap-%d", run)
	oldID := store.DocumentID(oldName)
	oldBytes := compressible(150)
	if _, err := s.Ensure(ctx, oldID, oldName, store.NilUUID); err != nil {
		t.Fatal(err)
	}
	// Written the way the previous version of this server wrote it: the bytes
	// as they came, and no codec column set, which defaults to 0.
	writeLegacySnapshot(t, s, oldID, oldBytes)

	newName := fmt.Sprintf("new-snap-%d", run)
	newID := store.DocumentID(newName)
	newBytes := compressible(150)
	if _, err := s.Ensure(ctx, newID, newName, store.NilUUID); err != nil {
		t.Fatal(err)
	}
	compact(t, s, newID, newBytes)

	for _, tc := range []struct {
		what string
		id   store.UUID
		name string
		want []byte
	}{
		{"a row from before compression", oldID, oldName, oldBytes},
		{"a row from after it", newID, newName, newBytes},
	} {
		doc, err := s.Load(ctx, tc.id, tc.name, store.NilUUID)
		if err != nil {
			t.Fatalf("%s: %v", tc.what, err)
		}
		if !bytes.Equal(doc.Snapshot, tc.want) {
			t.Errorf("%s: read back %d bytes, want %d", tc.what, len(doc.Snapshot), len(tc.want))
		}
	}

	// The old row is genuinely still raw in the table - otherwise this test
	// would pass by having quietly rewritten it, and would prove nothing about
	// reading what is already on disk.
	if codec := storedCodec(t, s, oldID); codec != 0 {
		t.Errorf("the legacy row now says codec %d; the test rewrote what it meant to read", codec)
	}
	if codec := storedCodec(t, s, newID); codec == 0 {
		t.Error("the new row was stored raw, so nothing was being compared")
	}
}

func TestAVersionSurvivesCompression(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("packed-version-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	if _, err := s.Ensure(ctx, id, name, store.NilUUID); err != nil {
		t.Fatal(err)
	}

	payload := compressible(300)
	v := store.Version{StateVector: []byte{1, 2, 3}, Payload: payload, Label: "before the migration"}
	if written, err := s.SaveVersion(ctx, id, v, 0); err != nil || !written {
		t.Fatalf("written=%v err=%v", written, err)
	}

	list, err := s.ListVersions(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("%d versions", len(list))
	}
	// A listing reports the stored size, which is what the version costs.
	if list[0].Bytes >= len(payload) {
		t.Errorf("the listing reports %d bytes for a %d byte payload; it was not compressed",
			list[0].Bytes, len(payload))
	}

	got, err := s.LoadVersion(ctx, id, list[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("the version came back as %d bytes, want %d", len(got.Payload), len(payload))
	}
	if got.Label != v.Label {
		t.Errorf("label = %q", got.Label)
	}
	// On a load, Bytes is the document rather than what it cost to keep.
	if got.Bytes != len(payload) {
		t.Errorf("a loaded version reports %d bytes, want the document's %d", got.Bytes, len(payload))
	}
}

// The deduplication that keeps history from growing forever compares state
// vectors, which are stored uncompressed. Compressing the payload must not have
// changed that.
func TestCompressionDoesNotBreakVersionDeduplication(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("dedup-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	if _, err := s.Ensure(ctx, id, name, store.NilUUID); err != nil {
		t.Fatal(err)
	}

	v := store.Version{StateVector: []byte{9, 9}, Payload: compressible(100)}
	if written, err := s.SaveVersion(ctx, id, v, 0); err != nil || !written {
		t.Fatalf("the first version: written=%v err=%v", written, err)
	}
	if written, err := s.SaveVersion(ctx, id, v, 0); err != nil || written {
		t.Fatalf("an identical version was written again: written=%v err=%v", written, err)
	}
}

// storedSnapshotSize reads what is actually in the column, which is the number
// the whole feature is about.
func storedSnapshotSize(t *testing.T, s *store.Store, id store.UUID) int {
	t.Helper()
	var n int
	if err := s.QueryRowForTest(t.Context(),
		`SELECT octet_length(snapshot) FROM documents WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func storedCodec(t *testing.T, s *store.Store, id store.UUID) int {
	t.Helper()
	var c int
	if err := s.QueryRowForTest(t.Context(),
		`SELECT snapshot_codec FROM documents WHERE id = $1`, id).Scan(&c); err != nil {
		t.Fatal(err)
	}
	return c
}

// writeLegacySnapshot writes a snapshot the way the server did before this
// package existed: raw bytes, and the codec column left at its default.
func writeLegacySnapshot(t *testing.T, s *store.Store, id store.UUID, snapshot []byte) {
	t.Helper()
	if err := s.ExecForTest(t.Context(),
		`UPDATE documents SET snapshot = $2, snapshot_seq = 1 WHERE id = $1`, id, snapshot); err != nil {
		t.Fatal(err)
	}
}
