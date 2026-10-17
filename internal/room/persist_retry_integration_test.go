package room

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/hook"
	"github.com/mesutokul/ycollab/internal/store"
	pgtestutil "github.com/mesutokul/ycollab/internal/testutil"
)

func TestPersistRetriesLostCommitReplyAndCompacts(t *testing.T) {
	dsn := os.Getenv("YCOLLAB_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("YCOLLAB_TEST_DATABASE_URL is not set")
	}
	proxy, err := pgtestutil.NewCommitReplyProxy(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := store.Open(ctx, proxy.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	sink := &recorder{}
	r := New(Config{
		Name: name, Owner: "retry-test-owner", Store: s,
		FlushInterval: -1, CompactAfter: 1000, Hooks: sink, Logger: quietLogger(),
	})
	if err := r.load(ctx); err != nil {
		t.Fatalf("load room: %v", err)
	}
	go r.persist(r.jobs)
	writerClosed := false
	t.Cleanup(func() {
		if !writerClosed {
			close(r.jobs)
			select {
			case <-r.persistDone:
			case <-time.After(35 * time.Second):
				t.Error("persist writer did not stop")
			}
		}
	})

	update := readFixture(t, "text-insert-single", "state.bin")
	want := crdt.NewDoc(0)
	if err := want.ApplyUpdate(update); err != nil {
		t.Fatal(err)
	}
	if want.PendingCount() != 0 {
		t.Fatal("fixture update has unresolved dependencies")
	}
	if want.Text("text").String() == "" {
		t.Fatal("fixture update did not produce text")
	}
	if err := r.doc.ApplyUpdate(update); err != nil {
		t.Fatal(err)
	}
	proxy.Arm()
	if err := r.recordAndWait(update); err != nil {
		t.Fatalf("persist update after lost commit reply: %v", err)
	}
	r.changed()
	if !proxy.DroppedCommitReply() {
		t.Fatal("proxy did not drop a successful Append COMMIT response")
	}
	stored, err := s.Load(ctx, r.docID, name, store.OwnerID("retry-test-owner"))
	if err != nil {
		t.Fatalf("load after ambiguous append: %v", err)
	}
	if len(stored.Updates) != 2 || !bytes.Equal(stored.Updates[0], update) || !bytes.Equal(stored.Updates[1], update) {
		t.Fatalf("ambiguous append left updates %q, want two identical committed rows", stored.Updates)
	}
	assertDocumentMatches(t, stored.Snapshot, stored.Updates, want)

	close(r.jobs)
	<-r.persistDone
	writerClosed = true
	r.emitHooks(time.Now())
	storeEvents := sink.of(hook.KindStore)
	if len(storeEvents) != 1 || storeEvents[0].Updates != 1 {
		t.Fatalf("store hooks=%v, want one event for one logical update", storeEvents)
	}

	snapshot, err := r.doc.EncodeStateAsUpdate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !r.compact(snapshot) {
		t.Fatal("room compaction failed after retry")
	}
	stored, err = s.Load(ctx, r.docID, name, store.OwnerID("retry-test-owner"))
	if err != nil {
		t.Fatalf("load after compaction: %v", err)
	}
	if len(stored.Updates) != 0 || stored.Snapshot == nil {
		t.Fatalf("compaction left %d log rows or no snapshot", len(stored.Updates))
	}
	assertDocumentMatches(t, stored.Snapshot, nil, want)
}

func assertDocumentMatches(t *testing.T, snapshot []byte, updates [][]byte, want *crdt.Doc) {
	t.Helper()
	got := crdt.NewDoc(0)
	if snapshot != nil {
		if err := got.ApplyUpdate(snapshot); err != nil {
			t.Fatalf("apply snapshot: %v", err)
		}
	}
	for _, update := range updates {
		if err := got.ApplyUpdate(update); err != nil {
			t.Fatalf("apply log update: %v", err)
		}
	}
	if got.PendingCount() != 0 {
		t.Fatalf("reloaded document has %d unresolved dependencies", got.PendingCount())
	}
	if gotText, wantText := docPrint(t, got), docPrint(t, want); gotText != wantText {
		t.Fatalf("reloaded document text %q, want %q", gotText, wantText)
	}
	gotSV, err := got.EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	wantSV, err := want.EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotSV, wantSV) {
		t.Fatalf("reloaded state vector %x, want %x", gotSV, wantSV)
	}
}
