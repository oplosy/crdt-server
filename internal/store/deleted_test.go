package store_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/mesutokul/ycollab/internal/store"
)

// What a room that outlived the document it was serving can do to the database.
//
// Deleting a document while another replica still holds it in memory is a real
// sequence: DELETE evicts the room on the replica that served the request and
// nothing tells the others. The question this answers is what the surviving
// room's next write does - and the answer is nothing, because both write paths
// need the row that was deleted. Append's foreign key has no parent, and
// Compact only ever updates a row it expects to find.
//
// That is worth a test rather than an argument, because it is the difference
// between "the deletion held" and "the document came back with its contents".
func TestARoomThatOutlivedItsDocumentCannotBringItBack(t *testing.T) {
	s, ctx := openStore(t)
	id, name := newDoc(t, s, ctx, "deleted-under-a-room")

	// A document with a snapshot and a log, which is what a room that has been
	// open for a while is holding.
	seqs, err := appendCurrent(t, s, ctx, id, [][]byte{{1, 2}, {3, 4}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(ctx, id, base.Owner, base.Incarnation, base.SnapshotGeneration, bytes.Repeat([]byte("a document"), 10), seqs); err != nil {
		t.Fatal(err)
	}

	if deleted, err := s.Delete(ctx, id); err != nil || !deleted {
		t.Fatalf("deleted=%v err=%v", deleted, err)
	}

	// The surviving room appends, as it would on the next keystroke.
	later, err := s.Append(ctx, id, base.Owner, base.Incarnation, [][]byte{{5, 6}})
	if !errors.Is(err, store.ErrStaleDocument) {
		t.Errorf("stale append to deleted document = seqs %v err %v, want ErrStaleDocument", later, err)
	}

	// And compacts, as it would on eviction. This is the one that would matter:
	// a snapshot write that recreated the row would put the whole document back,
	// contents and all. It cannot - Compact only ever updates a row it expects
	// to find, and it reads the key of the snapshot it is replacing first, so it
	// gives up before writing anything.
	if _, err := s.Compact(ctx, id, base.Owner, base.Incarnation, base.SnapshotGeneration, bytes.Repeat([]byte("the whole document"), 10), seqs); err == nil {
		t.Error("compacting a deleted document was accepted")
	}

	// The document is gone and stays gone. A later connection opening the same
	// name creates it again, empty - a new document that shares a name, not the
	// old one coming back.
	doc, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Snapshot != nil || len(doc.Updates) != 0 {
		t.Errorf("%q came back: %d snapshot bytes and %d updates",
			name, len(doc.Snapshot), len(doc.Updates))
	}
}
