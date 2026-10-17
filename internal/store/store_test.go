package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/store"
)

// The brief asks for an integration test against a real Postgres in Docker,
// not a mock, and this package has nothing worth testing against a mock: every
// interesting property here is a property of the database - identity columns,
// transaction boundaries, advisory locks.
//
//	docker compose -f deploy/docker-compose.yml up -d
//	YCOLLAB_TEST_DATABASE_URL=postgres://ycollab:ycollab@127.0.0.1:5433/ycollab go test ./internal/store/
const dbEnv = "YCOLLAB_TEST_DATABASE_URL"

func testStore(t *testing.T) *store.Store {
	t.Helper()
	url := os.Getenv(dbEnv)
	if url == "" {
		t.Skipf("%s is not set; start deploy/docker-compose.yml to run these", dbEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func compactCurrent(t *testing.T, s *store.Store, ctx context.Context, id store.UUID, snapshot []byte, folded []int64) error {
	t.Helper()
	doc, err := s.LoadAny(ctx, id)
	if err != nil {
		return err
	}
	if !doc.Exists {
		return errors.New("document does not exist")
	}
	_, err = s.Compact(ctx, id, doc.Owner, doc.Incarnation, doc.SnapshotGeneration, snapshot, folded)
	return err
}

// Each test gets its own document, so tests do not interfere and none of them
// has to truncate a table somebody else might be using.
func testDoc(t *testing.T) store.UUID {
	t.Helper()
	return store.DocumentID("test-" + t.Name() + "-" + time.Now().Format(time.RFC3339Nano))
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestLoadCreatesAnEmptyDocument(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)

	doc, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if doc.Snapshot != nil || doc.SnapshotSeq != 0 || len(doc.Updates) != 0 || doc.LastSeq != 0 {
		t.Fatalf("fresh document is not empty: %+v", doc)
	}

	// Loading twice must not fail on the primary key, because every connection
	// to a document does it.
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatalf("second load: %v", err)
	}
}

func TestConcurrentMigrateOnCleanSchema(t *testing.T) {
	dbURL := os.Getenv(dbEnv)
	if dbURL == "" {
		t.Skipf("%s is not set; start deploy/docker-compose.yml to run these", dbEnv)
	}
	c := ctx(t)
	admin, err := pgx.Connect(c, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schemaName := fmt.Sprintf("migrate_%d", time.Now().UnixNano())
	if _, err := admin.Exec(c, `CREATE SCHEMA `+schemaName); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), `DROP SCHEMA `+schemaName+` CASCADE`)

	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schemaName)
	u.RawQuery = query.Encode()

	stores := make([]*store.Store, 2)
	for i := range stores {
		stores[i], err = store.Open(c, u.String())
		if err != nil {
			t.Fatal(err)
		}
		defer stores[i].Close()
	}
	start := make(chan struct{})
	errs := make(chan error, len(stores))
	for _, s := range stores {
		go func(s *store.Store) {
			<-start
			errs <- s.Migrate(c)
		}(s)
	}
	close(start)
	for range stores {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Migrate: %v", err)
		}
	}

	isolated, err := pgx.Connect(c, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close(context.Background())
	var documents, incarnationSeq bool
	if err := isolated.QueryRow(c, `SELECT to_regclass('documents') IS NOT NULL,
		to_regclass('document_incarnation_seq') IS NOT NULL`).Scan(&documents, &incarnationSeq); err != nil {
		t.Fatal(err)
	}
	if !documents || !incarnationSeq {
		t.Fatalf("clean schema migration left documents=%v incarnation_seq=%v", documents, incarnationSeq)
	}
}

func TestAppendAndLoadPreservesOrder(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	want := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	first, err := s.Append(c, id, want[:2])
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("got %d seqs for 2 payloads", len(first))
	}
	second, err := s.Append(c, id, want[2:])
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if second[0] <= first[1] {
		t.Fatalf("seq did not advance: %v then %v", first, second)
	}

	doc, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(doc.Updates) != len(want) {
		t.Fatalf("got %d updates, want %d", len(doc.Updates), len(want))
	}
	for i := range want {
		if !bytes.Equal(doc.Updates[i], want[i]) {
			t.Fatalf("update %d is %q, want %q", i, doc.Updates[i], want[i])
		}
	}
	if doc.LastSeq != second[0] {
		t.Fatalf("LastSeq %d, want %d", doc.LastSeq, second[0])
	}
	// The seqs come back with the payloads, so a caller that folds them into a
	// snapshot can say exactly which rows it covered.
	if len(doc.Seqs) != len(doc.Updates) {
		t.Fatalf("%d seqs for %d updates", len(doc.Seqs), len(doc.Updates))
	}
	if doc.Seqs[0] != first[0] || doc.Seqs[2] != second[0] {
		t.Fatalf("seqs %v, want to start with %v and end with %v", doc.Seqs, first, second)
	}
}

func TestCompactFoldsTheLog(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	folded, err := s.Append(c, id, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, c, id, []byte("snapshot-1"), folded); err != nil {
		t.Fatalf("compact: %v", err)
	}

	doc, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(doc.Snapshot, []byte("snapshot-1")) {
		t.Fatalf("snapshot %q", doc.Snapshot)
	}
	if doc.SnapshotSeq != folded[len(folded)-1] {
		t.Fatalf("snapshot_seq %d, want %d", doc.SnapshotSeq, folded[len(folded)-1])
	}
	if len(doc.Updates) != 0 {
		t.Fatalf("compaction left %d rows behind", len(doc.Updates))
	}
}

// An update that arrives while a snapshot is being prepared has a higher seq
// than the watermark, so it must survive the delete. Losing it would be exactly
// the data loss the single-transaction requirement exists to prevent.
func TestCompactKeepsUpdatesAboveTheWatermark(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	folded, err := s.Append(c, id, [][]byte{[]byte("folded")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(c, id, [][]byte{[]byte("in flight")}); err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, c, id, []byte("snapshot"), folded); err != nil {
		t.Fatalf("compact: %v", err)
	}

	doc, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Updates) != 1 || !bytes.Equal(doc.Updates[0], []byte("in flight")) {
		t.Fatalf("remaining log is %q", doc.Updates)
	}
}

// C2: any replica may compact any document. An older snapshot must not be able
// to overwrite a newer one and then delete rows the newer one does not contain.
func TestCompactRefusesToGoBackwards(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	first, err := s.Append(c, id, [][]byte{[]byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Append(c, id, [][]byte{[]byte("b")})
	if err != nil {
		t.Fatal(err)
	}

	base, err := s.LoadAny(c, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(c, id, base.Owner, base.Incarnation, base.SnapshotGeneration, []byte("newer"), append(first, second...)); err != nil {
		t.Fatalf("compact: %v", err)
	}
	_, err = s.Compact(c, id, base.Owner, base.Incarnation, base.SnapshotGeneration, []byte("older"), first)
	if !errors.Is(err, store.ErrStaleSnapshot) {
		t.Fatalf("got %v, want ErrStaleSnapshot", err)
	}

	doc, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(doc.Snapshot, []byte("newer")) {
		t.Fatalf("the losing snapshot won: %q", doc.Snapshot)
	}
	if doc.SnapshotSeq != second[0] {
		t.Fatalf("snapshot_seq went backwards: %d", doc.SnapshotSeq)
	}
}

// Two replicas can hold different valid Yjs states. A snapshot based on an
// older state must not overwrite another replica's newer snapshot merely
// because its own update has a higher seq.
func TestCompactPreservesConcurrentReplicaUpdates(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	update := func(name string) []byte {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "fixtures", "text-three-client-interleaved", name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	x, y, z := update("update-000.bin"), update("update-003.bin"), update("update-006.bin")

	if _, err := s.Append(c, id, [][]byte{x}); err != nil {
		t.Fatal(err)
	}
	a := crdt.NewDoc(0)
	loaded, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Snapshot != nil {
		if err := a.ApplyUpdate(loaded.Snapshot); err != nil {
			t.Fatal(err)
		}
	}
	for _, payload := range loaded.Updates {
		if err := a.ApplyUpdate(payload); err != nil {
			t.Fatal(err)
		}
	}

	b := crdt.NewDoc(0)
	if loaded.Snapshot != nil {
		if err := b.ApplyUpdate(loaded.Snapshot); err != nil {
			t.Fatal(err)
		}
	}
	for _, payload := range loaded.Updates {
		if err := b.ApplyUpdate(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.ApplyUpdate(y); err != nil {
		t.Fatal(err)
	}
	ySeqs, err := s.Append(c, id, [][]byte{y})
	if err != nil {
		t.Fatal(err)
	}
	xy, err := b.EncodeStateAsUpdate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(c, id, loaded.Owner, loaded.Incarnation, loaded.SnapshotGeneration, xy, append(append([]int64(nil), loaded.Seqs...), ySeqs...)); err != nil {
		t.Fatalf("replica B compact: %v", err)
	}

	if err := a.ApplyUpdate(z); err != nil {
		t.Fatal(err)
	}
	zSeqs, err := s.Append(c, id, [][]byte{z})
	if err != nil {
		t.Fatal(err)
	}
	xz, err := a.EncodeStateAsUpdate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(c, id, loaded.Owner, loaded.Incarnation, loaded.SnapshotGeneration, xz, append(append([]int64(nil), loaded.Seqs...), zSeqs...)); !errors.Is(err, store.ErrStaleSnapshot) {
		t.Fatalf("stale replica A compact: got %v, want ErrStaleSnapshot", err)
	}

	stored, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	restored := crdt.NewDoc(0)
	if stored.Snapshot != nil {
		if err := restored.ApplyUpdate(stored.Snapshot); err != nil {
			t.Fatal(err)
		}
	}
	for _, payload := range stored.Updates {
		if err := restored.ApplyUpdate(payload); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := restored.Text("text").String(), "zeroonethree"; got != want {
		t.Fatalf("reloaded text %q, want merged X/Y/Z state %q", got, want)
	}
}

// Compaction happens in one transaction, so a reader either sees the old
// snapshot with the whole log, or the new snapshot with the log pruned - never
// a document missing both.
func TestCompactIsAtomicForReaders(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	folded, err := s.Append(c, id, [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	base, err := s.LoadAny(c, id)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, err := s.Compact(c, id, base.Owner, base.Incarnation, base.SnapshotGeneration, []byte("snapshot"), folded)
		done <- err
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		doc, err := s.Load(c, id, "", store.NilUUID)
		if err != nil {
			t.Errorf("load during compaction: %v", err)
			break
		}
		if doc.Snapshot == nil && len(doc.Updates) != 2 {
			t.Fatalf("reader saw a document with no snapshot and %d updates", len(doc.Updates))
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("compact: %v", err)
			}
			return
		default:
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("compact: %v", err)
	}
}

// C6: identity values are handed out before commit, so a row with a lower seq
// can become visible after one with a higher seq. Compaction must therefore
// delete the rows it actually folded in, not everything below a watermark - a
// range delete would take the late row with it and lose the update.
func TestCompactOnlyDeletesTheRowsItFolded(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	early, err := s.Append(c, id, [][]byte{[]byte("early")})
	if err != nil {
		t.Fatal(err)
	}
	late, err := s.Append(c, id, [][]byte{[]byte("late")})
	if err != nil {
		t.Fatal(err)
	}

	// The snapshot covers only the later row, standing in for a row that was
	// still invisible when the snapshot was taken but has a lower seq.
	if err := compactCurrent(t, s, c, id, []byte("snapshot"), late); err != nil {
		t.Fatalf("compact: %v", err)
	}

	doc, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Updates) != 1 || !bytes.Equal(doc.Updates[0], []byte("early")) {
		t.Fatalf("the row below the watermark was deleted: remaining %q", doc.Updates)
	}
	if doc.Seqs[0] != early[0] {
		t.Fatalf("remaining seq %d, want %d", doc.Seqs[0], early[0])
	}
	// And it is still loaded, because Load does not filter on snapshot_seq.
	if doc.SnapshotSeq <= early[0] {
		t.Fatalf("this test proves nothing unless snapshot_seq (%d) is above the surviving row (%d)",
			doc.SnapshotSeq, early[0])
	}
	firstGeneration := doc.SnapshotGeneration
	if err := compactCurrent(t, s, c, id, []byte("snapshot includes the late row"), doc.Seqs); err != nil {
		t.Fatalf("compact the lower-seq row at the same watermark: %v", err)
	}
	doc, err = s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if doc.SnapshotSeq != late[0] || doc.SnapshotGeneration != firstGeneration+1 {
		t.Fatalf("same-watermark compaction has seq/generation %d/%d, want %d/%d",
			doc.SnapshotSeq, doc.SnapshotGeneration, late[0], firstGeneration+1)
	}
	if len(doc.Updates) != 0 {
		t.Fatalf("same-watermark compaction left %d folded rows", len(doc.Updates))
	}
}

func TestCompactCannotCrossDocumentRecreation(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	old, err := s.LoadAny(c, id)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := s.Delete(c, id); err != nil || !deleted {
		t.Fatalf("delete: deleted=%v err=%v", deleted, err)
	}
	if _, err := s.Ensure(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	current, err := s.LoadAny(c, id)
	if err != nil {
		t.Fatal(err)
	}
	if !current.Exists || current.Incarnation == old.Incarnation {
		t.Fatalf("recreated row incarnation %d, old incarnation %d", current.Incarnation, old.Incarnation)
	}
	if _, err := s.Compact(c, id, old.Owner, old.Incarnation, old.SnapshotGeneration, []byte("stale"), []int64{1}); !errors.Is(err, store.ErrStaleSnapshot) {
		t.Fatalf("stale compact got %v, want ErrStaleSnapshot", err)
	}
	current, err = s.LoadAny(c, id)
	if err != nil {
		t.Fatal(err)
	}
	if current.Snapshot != nil || len(current.Updates) != 0 {
		t.Fatalf("stale compact changed recreated document: %+v", current)
	}
}

// Compacting with nothing to fold is a no-op rather than a snapshot that
// deletes nothing: an evicted room that never saw an edit has nothing to say.
func TestCompactWithNothingFoldedIsANoOp(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	if err := compactCurrent(t, s, c, id, []byte("snapshot"), nil); err != nil {
		t.Fatalf("compact: %v", err)
	}
	doc, err := s.Load(c, id, "", store.NilUUID)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Snapshot != nil {
		t.Fatalf("wrote a snapshot for an empty compaction: %q", doc.Snapshot)
	}
}

func TestUpdateCount(t *testing.T) {
	s := testStore(t)
	c := ctx(t)
	id := testDoc(t)
	if _, err := s.Load(c, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(c, id, [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	n, err := s.UpdateCount(c, id)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("count %d, want 2", n)
	}
}

// Documents are keyed by UUID but rooms are named in a URL, so the mapping has
// to be deterministic and has to leave real UUIDs alone.
func TestDocumentID(t *testing.T) {
	a := store.DocumentID("notes-2026")
	if a != store.DocumentID("notes-2026") {
		t.Fatal("the same name produced two different ids")
	}
	if a == store.DocumentID("notes-2027") {
		t.Fatal("two names collided")
	}
	if got := a.String()[14]; got != '5' {
		t.Fatalf("version nibble is %q, want '5'", got)
	}
	if v := a[8] & 0xc0; v != 0x80 {
		t.Fatalf("variant bits are %#x, want 0x80", v)
	}

	const canonical = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
	id := store.DocumentID(canonical)
	if id.String() != canonical {
		t.Fatalf("a name that is already a UUID was rehashed: %s", id)
	}
	if _, err := store.ParseUUID("not-a-uuid"); !errors.Is(err, store.ErrBadUUID) {
		t.Fatalf("got %v, want ErrBadUUID", err)
	}
	if _, err := store.ParseUUID("f81d4fae-7dec-11d0-a765-00a0c91e6bfZ"); !errors.Is(err, store.ErrBadUUID) {
		t.Fatal("accepted a non-hex UUID")
	}
}

// Deleting a document takes its log with it, through the foreign key's cascade.
func TestDeleteRemovesTheDocumentAndItsLog(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	id := store.DocumentID(fmt.Sprintf("delete-%d", time.Now().UnixNano()))

	if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, id, [][]byte{{1}, {2}}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.UpdateCount(ctx, id); err != nil || n != 2 {
		t.Fatalf("count is %d (%v), want 2", n, err)
	}

	existed, err := s.Delete(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !existed {
		t.Fatal("Delete reported no such document")
	}
	if n, err := s.UpdateCount(ctx, id); err != nil || n != 0 {
		t.Fatalf("%d log rows survived the delete (%v)", n, err)
	}
	// And deleting again says so rather than pretending.
	if existed, err := s.Delete(ctx, id); err != nil || existed {
		t.Fatalf("deleting twice reported existed=%v (%v)", existed, err)
	}
}

// Retention deletes what nothing has touched, and only that. A document whose
// snapshot is old but whose log has a recent row is active, which is the case
// that makes the naive "look at updated_at alone" query wrong.
func TestDeleteIdleSparesRecentlyWrittenDocuments(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	stale := store.DocumentID(fmt.Sprintf("stale-%d", time.Now().UnixNano()))
	active := store.DocumentID(fmt.Sprintf("active-%d", time.Now().UnixNano()))

	// Both rows are created now, and both will be older than the cutoff.
	for _, id := range []store.UUID{stale, active} {
		if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
			t.Fatal(err)
		}
	}

	// Let some time pass, then write to one of them. The cutoff falls between
	// the two moments, so the two documents differ only in whether their log has
	// anything newer than it.
	time.Sleep(1200 * time.Millisecond)
	cutoff := time.Now().Add(-600 * time.Millisecond)
	if _, err := s.Append(ctx, active, [][]byte{{1}}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.DeleteIdle(ctx, cutoff); err != nil {
		t.Fatal(err)
	}
	if n, err := s.UpdateCount(ctx, active); err != nil || n != 1 {
		t.Fatalf("the active document lost its log: %d rows (%v)", n, err)
	}
	if existed, err := s.Delete(ctx, stale); err != nil {
		t.Fatal(err)
	} else if existed {
		t.Fatal("the idle document survived the retention sweep")
	}

	// And a cutoff before everything spares everything.
	if n, err := s.DeleteIdle(ctx, time.Now().Add(-24*time.Hour)); err != nil || n != 0 {
		t.Fatalf("a cutoff a day ago deleted %d documents (%v)", n, err)
	}
}
