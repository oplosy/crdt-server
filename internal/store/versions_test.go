package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/store"
)

// A version is written when it has something new to say, and not otherwise.
// Both halves matter: without the first there is no history, and without the
// second an idle document accumulates identical copies of itself forever.
func TestVersionsAreWrittenOnlyWhenTheyDiffer(t *testing.T) {
	s, ctx := openStore(t)
	id := store.DocumentID(fmt.Sprintf("versions-%d", time.Now().UnixNano()))
	if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	first := store.Version{StateVector: []byte{1}, Payload: []byte{0xaa}}
	// minAge of zero: the age gate is off, so this test is only about the
	// state vector.
	if written, err := saveVersionCurrent(t, s, ctx, id, first, 0); err != nil || !written {
		t.Fatalf("the first version: written=%v err=%v", written, err)
	}
	// The same state vector says nothing new.
	if written, err := saveVersionCurrent(t, s, ctx, id, first, 0); err != nil || written {
		t.Fatalf("an identical version was written: written=%v err=%v", written, err)
	}
	// A different one does.
	second := store.Version{StateVector: []byte{2}, Payload: []byte{0xbb}, Label: "after the edit"}
	if written, err := saveVersionCurrent(t, s, ctx, id, second, 0); err != nil || !written {
		t.Fatalf("a changed version: written=%v err=%v", written, err)
	}

	got, err := s.ListVersions(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("%d versions, want 2", len(got))
	}
	// Newest first, which is the order somebody looking for "yesterday" reads.
	if got[0].Label != "after the edit" {
		t.Errorf("the newest version is %q", got[0].Label)
	}
	// A listing must not carry payloads: twenty versions of a megabyte document
	// is not a listing.
	for _, v := range got {
		if v.Payload != nil {
			t.Errorf("version %d came back with its payload", v.ID)
		}
		if v.Bytes != 1 {
			t.Errorf("version %d reports %d bytes, want 1", v.ID, v.Bytes)
		}
		if v.CreatedAt.IsZero() {
			t.Errorf("version %d has no timestamp", v.ID)
		}
	}
}

func TestWritesRejectARecreatedDocumentIncarnation(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("stale-write-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	owner := store.OwnerID("same-owner")
	if _, err := s.Load(ctx, id, name, owner); err != nil {
		t.Fatal(err)
	}
	old, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, id, name, owner); err != nil {
		t.Fatal(err)
	}
	current, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if current.Incarnation == old.Incarnation {
		t.Fatalf("recreated document reused incarnation %d", old.Incarnation)
	}
	if _, err := s.Append(ctx, id, old.Owner, old.Incarnation, [][]byte{{1, 2}}); !errors.Is(err, store.ErrStaleDocument) {
		t.Fatalf("stale Append error = %v, want ErrStaleDocument", err)
	}
	if written, err := s.SaveVersion(ctx, id, old.Owner, old.Incarnation, store.Version{
		StateVector: []byte{1}, Payload: []byte("old state"),
	}, 0); written || !errors.Is(err, store.ErrStaleDocument) {
		t.Fatalf("stale SaveVersion = %v, %v; want false, ErrStaleDocument", written, err)
	}
	if _, err := saveVersionCurrent(t, s, ctx, id, store.Version{
		StateVector: []byte{2}, Payload: []byte("current state"),
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := saveVersionCurrent(t, s, ctx, id, store.Version{
		StateVector: []byte{3}, Payload: []byte("newer state"),
	}, 0); err != nil {
		t.Fatal(err)
	}
	if removed, err := s.PruneVersions(ctx, id, old.Owner, old.Incarnation, 1); removed != 0 || !errors.Is(err, store.ErrStaleDocument) {
		t.Fatalf("stale PruneVersions = %d, %v; want 0, ErrStaleDocument", removed, err)
	}
	if count, err := s.VersionCount(ctx, id); err != nil || count != 2 {
		t.Fatalf("current version count = %d, %v; want 2", count, err)
	}
}

// Every replica holding a document runs its own timer, so without the age gate
// three replicas produce three versions per interval.
func TestTheAgeGateStopsReplicasDuplicatingAVersion(t *testing.T) {
	s, ctx := openStore(t)
	id := store.DocumentID(fmt.Sprintf("versions-age-%d", time.Now().UnixNano()))
	if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	if written, err := saveVersionCurrent(t, s, ctx, id, store.Version{StateVector: []byte{1}, Payload: []byte{1}}, time.Hour); err != nil || !written {
		t.Fatalf("the first version: written=%v err=%v", written, err)
	}
	// A different document state, but inside the interval: this is the second
	// replica arriving a moment later, and it must not write.
	if written, err := saveVersionCurrent(t, s, ctx, id, store.Version{StateVector: []byte{2}, Payload: []byte{2}}, time.Hour); err != nil || written {
		t.Fatalf("a second replica wrote inside the interval: written=%v err=%v", written, err)
	}
	// With the interval elapsed it writes.
	if written, err := saveVersionCurrent(t, s, ctx, id, store.Version{StateVector: []byte{2}, Payload: []byte{2}}, time.Nanosecond); err != nil || !written {
		t.Fatalf("after the interval: written=%v err=%v", written, err)
	}
	if n, err := s.VersionCount(ctx, id); err != nil || n != 2 {
		t.Fatalf("%d versions, want 2 (err %v)", n, err)
	}
}

// Each version is a whole document, so unbounded history is unbounded storage.
func TestPruningKeepsTheNewest(t *testing.T) {
	s, ctx := openStore(t)
	id := store.DocumentID(fmt.Sprintf("versions-prune-%d", time.Now().UnixNano()))
	if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		v := store.Version{StateVector: []byte{byte(i)}, Payload: []byte{byte(i)}, Label: fmt.Sprint(i)}
		if written, err := saveVersionCurrent(t, s, ctx, id, v, 0); err != nil || !written {
			t.Fatalf("version %d: written=%v err=%v", i, written, err)
		}
	}

	removed, err := pruneVersionsCurrent(t, s, ctx, id, 2)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 4 {
		t.Errorf("pruned %d, want 4", removed)
	}
	got, err := s.ListVersions(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Label != "5" || got[1].Label != "4" {
		t.Fatalf("kept %+v, want the newest two", labels(got))
	}
	// Keeping zero deletes nothing: "keep none" is what deleting the document
	// is for, and a retention setting that empties the history when it is
	// misconfigured is a bad way to find out.
	if removed, err := pruneVersionsCurrent(t, s, ctx, id, 0); err != nil || removed != 0 {
		t.Errorf("PruneVersions(0) removed %d (err %v)", removed, err)
	}
}

// A caller that guesses a number must not be handed another document's history.
func TestAVersionBelongsToItsDocument(t *testing.T) {
	s, ctx := openStore(t)
	mine := store.DocumentID(fmt.Sprintf("versions-mine-%d", time.Now().UnixNano()))
	theirs := store.DocumentID(fmt.Sprintf("versions-theirs-%d", time.Now().UnixNano()))
	for _, id := range []store.UUID{mine, theirs} {
		if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := saveVersionCurrent(t, s, ctx, theirs, store.Version{StateVector: []byte{9}, Payload: []byte("secret")}, 0); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListVersions(ctx, theirs, 0)
	if err != nil || len(list) != 1 {
		t.Fatalf("setup: %d versions (err %v)", len(list), err)
	}

	if _, err := s.LoadVersion(ctx, mine, list[0].ID); !errors.Is(err, store.ErrNoVersion) {
		t.Fatalf("reading another document's version returned %v, want ErrNoVersion", err)
	}
	// And the honest lookup works, payload included.
	v, err := s.LoadVersion(ctx, theirs, list[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(v.Payload) != "secret" {
		t.Errorf("payload is %q", v.Payload)
	}
}

// Deleting a document must take its history with it, or a "delete this
// document" that leaves every past state behind is not a deletion.
func TestDeletingADocumentTakesItsVersions(t *testing.T) {
	s, ctx := openStore(t)
	id := store.DocumentID(fmt.Sprintf("versions-cascade-%d", time.Now().UnixNano()))
	if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	if _, err := saveVersionCurrent(t, s, ctx, id, store.Version{StateVector: []byte{1}, Payload: []byte{1}}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n, err := s.VersionCount(ctx, id); err != nil || n != 0 {
		t.Fatalf("%d versions survived the delete (err %v)", n, err)
	}
}

// A version for a document that does not exist would be a row nothing can ever
// reach, and one the foreign key would refuse anyway.
func TestNoVersionsForAMissingDocument(t *testing.T) {
	s, ctx := openStore(t)
	id := store.DocumentID(fmt.Sprintf("versions-absent-%d", time.Now().UnixNano()))
	written, err := s.SaveVersion(ctx, id, store.NilUUID, 0, store.Version{StateVector: []byte{1}, Payload: []byte{1}}, 0)
	if written || !errors.Is(err, store.ErrStaleDocument) {
		t.Errorf("SaveVersion for missing document = %v, %v; want false, ErrStaleDocument", written, err)
	}
}

func labels(vs []store.Version) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.Label)
	}
	return out
}

func openStore(t *testing.T) (*store.Store, context.Context) {
	t.Helper()
	s := testStore(t)
	return s, context.Background()
}
