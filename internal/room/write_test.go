package room

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/cluster"
	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/protocol"
)

// A merge into a document people are editing has to reach them. A client that
// is not told keeps building on a version the server no longer holds.
func TestMergeReachesTheConnectedClients(t *testing.T) {
	now := time.Unix(1750000000, 0)
	replicas, _ := newCluster(t, &now, 2, nil)
	c := replicas[0].join(1)

	update := readFixture(t, "text-insert-single", "update-000.bin")
	done := make(chan error, 1)
	go func() { done <- replicas[0].room.Merge(update) }()
	// The room is driven by hand in this package's tests, so the command has to
	// be taken out of the mailbox for the merge to happen.
	replicas[0].room.handle(<-replicas[0].room.mailbox)
	if err := <-done; err != nil {
		t.Fatalf("Merge: %v", err)
	}

	msgs := c.decodeAll(t)
	var got []byte
	for _, m := range msgs {
		if u, ok := m.(protocol.UpdateMessage); ok {
			got = u.Update
		}
	}
	if got == nil {
		t.Fatalf("the connected client was told nothing: %#v", msgs)
	}
	doc := crdt.NewDoc(1)
	if err := doc.ApplyUpdate(got); err != nil {
		t.Fatalf("what the client received would not apply: %v", err)
	}

	// And the other replicas, or a merge would be visible on one node only.
	pump(t, replicas)
	if replicas[0].print() != replicas[1].print() {
		t.Errorf("the merge did not reach the other replica:\n A %s\n B %s",
			replicas[0].print(), replicas[1].print())
	}
}

func TestResidentMergeReturnsUnconfirmedStoreFailure(t *testing.T) {
	update := readFixture(t, "text-insert-single", "update-000.bin")
	fake := &fakeStore{appendErr: errors.New("append failed")}
	r := New(Config{Name: "notes", Store: fake, Logger: quietLogger(), FlushInterval: time.Hour})
	r.jobs = make(chan persistJob, 2)
	r.persistDone = make(chan struct{})
	go r.persist(r.jobs)
	t.Cleanup(func() {
		close(r.jobs)
		<-r.persistDone
	})
	done := make(chan error, 1)
	go func() { done <- r.Merge(update) }()
	r.handle(<-r.mailbox)
	if err := <-done; !errors.Is(err, ErrPersistence) {
		t.Fatalf("Merge error = %v, want ErrPersistence", err)
	}
}

// A body that is not an update is refused, and leaves the document alone.
func TestMergingRubbishChangesNothing(t *testing.T) {
	r := New(Config{Name: "notes", IdleTimeout: time.Hour, Logger: quietLogger()})
	before := docPrint(t, r.doc)

	reply := make(chan error, 1)
	r.handle(mergeCmd{update: []byte("this is not a Yjs update"), reply: reply})
	if err := <-reply; err == nil {
		t.Fatal("a body that is not an update was accepted")
	}
	if docPrint(t, r.doc) != before {
		t.Error("the document changed anyway")
	}
}

// Import is the path for a document no room on this node holds: it writes the
// update to the log, and publishes it so a replica that does hold the document
// sees the restore rather than overwriting it at its next eviction.
func TestImportWritesAndPublishes(t *testing.T) {
	fake := &fakeStore{}
	bus := cluster.NewMemory()
	t.Cleanup(func() { _ = bus.Close() })
	envelopes := make(chan cluster.Envelope, 4)
	sub, err := bus.Subscribe(context.Background(), "notes", func(e cluster.Envelope) {
		envelopes <- e
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	update := readFixture(t, "text-insert-single", "update-000.bin")
	cfg := MergeConfig{Store: fake, Bus: bus, NodeID: 42}
	if err := Import(context.Background(), cfg, "notes", update); err != nil {
		t.Fatalf("Import: %v", err)
	}

	appended, _ := fake.counts()
	if appended != 1 {
		t.Errorf("%d rows were written, want 1", appended)
	}
	select {
	case env := <-envelopes:
		if env.Origin != 42 || env.Kind != cluster.KindUpdate || !env.HasIncarnation || env.Incarnation != 1 {
			t.Errorf("published origin=%d kind=%d has-incarnation=%v incarnation=%d", env.Origin, env.Kind, env.HasIncarnation, env.Incarnation)
		}
		if string(env.Payload) != string(update) {
			t.Error("the published payload is not the update")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was published, so a replica holding this document would never see the restore")
	}
}

// A body that would not apply must be refused before it is written, or the
// document becomes unloadable at its next start.
func TestImportRefusesRubbishWithoutWriting(t *testing.T) {
	fake := &fakeStore{}
	cfg := MergeConfig{Store: fake}
	if err := Import(context.Background(), cfg, "notes", []byte("not an update")); err == nil {
		t.Fatal("Import accepted a body that is not an update")
	}
	if appended, _ := fake.counts(); appended != 0 {
		t.Errorf("%d rows were written anyway", appended)
	}
}
