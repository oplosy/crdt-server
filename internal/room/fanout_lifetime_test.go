package room

import (
	"context"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/cluster"
	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/store"
)

type gatedLoadStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
}

func (s *gatedLoadStore) Load(ctx context.Context, id store.UUID, name string, owner store.UUID) (*store.Document, error) {
	close(s.entered)
	<-s.release
	return s.fakeStore.Load(ctx, id, name, owner)
}

func TestQueuedFanoutChecksLoadedIdentityForEveryKind(t *testing.T) {
	bus := cluster.NewMemory()
	t.Cleanup(func() { _ = bus.Close() })
	base := &fakeStore{doc: store.Document{Exists: true, Owner: store.NilUUID, Incarnation: 0}}
	store := &gatedLoadStore{fakeStore: base, entered: make(chan struct{}), release: make(chan struct{})}
	r := runRoom(t, Config{
		Name: "queued-incarnation", Store: store, Bus: bus, NodeID: 8,
		Tick: time.Hour, AntiEntropy: time.Hour,
	})
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("room did not subscribe and begin loading")
	}

	currentUpdate := readFixture(t, "text-insert-single", "update-000.bin")
	staleUpdate := readFixture(t, "text-three-client-interleaved", "state.bin")
	awareness := readFixture(t, "awareness", "update-single.bin")
	emptySV, err := crdt.NewDoc(90).EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	publish := func(env cluster.Envelope) {
		t.Helper()
		if err := bus.Publish(context.Background(), "queued-incarnation", env); err != nil {
			t.Fatalf("publish queued envelope: %v", err)
		}
	}
	// Incarnation zero is a real persisted legacy identity. A missing marker is
	// the separate no-database identity and must be refused by this room.
	publish(cluster.Envelope{Origin: 10, Kind: cluster.KindUpdate, HasIncarnation: true, Incarnation: 0, Payload: currentUpdate})
	publish(cluster.Envelope{Origin: 11, Kind: cluster.KindUpdate, Payload: staleUpdate})
	publish(cluster.Envelope{Origin: 12, Kind: cluster.KindAwareness, HasIncarnation: true, Incarnation: 0, Payload: awareness})
	publish(cluster.Envelope{Origin: 13, Kind: cluster.KindAwareness, HasIncarnation: true, Incarnation: 1, Payload: awareness})
	publish(cluster.Envelope{Origin: 14, Kind: cluster.KindStateVector, HasIncarnation: true, Incarnation: 0, Payload: emptySV})
	publish(cluster.Envelope{Origin: 15, Kind: cluster.KindStateVector, HasIncarnation: true, Incarnation: 1, Payload: emptySV})
	if queued := len(r.remote); queued != 6 {
		t.Fatalf("queued %d envelopes before load, want 6", queued)
	}
	close(store.release)

	wantDoc := crdt.NewDoc(0)
	if err := wantDoc.ApplyUpdate(currentUpdate); err != nil {
		t.Fatal(err)
	}
	want := docPrint(t, wantDoc)
	eventually(t, "loaded-identity fanout filtering", func() bool {
		got, err := roomDocumentPrint(t, r)
		return err == nil && got == want && r.stats.RemoteAwarenessApplied.Load() == 1 &&
			r.stats.AnsweredStateVector.Load() == 1 && r.stats.RemoteRejected.Load() == 3
	})
}

func TestNoDatabaseRoomsAcceptUnscopedFanout(t *testing.T) {
	r := New(Config{Name: "no-database", Logger: quietLogger()})
	update := readFixture(t, "text-insert-single", "update-000.bin")
	awareness := readFixture(t, "awareness", "update-single.bin")
	emptySV, err := crdt.NewDoc(91).EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	for _, env := range []cluster.Envelope{
		{Origin: 2, Kind: cluster.KindUpdate, Payload: update},
		{Origin: 3, Kind: cluster.KindAwareness, Payload: awareness},
		{Origin: 4, Kind: cluster.KindStateVector, Payload: emptySV},
	} {
		r.remoteEnvelope(env)
	}
	doc := crdt.NewDoc(0)
	if err := doc.ApplyUpdate(update); err != nil {
		t.Fatal(err)
	}
	if got, want := docPrint(t, r.doc), docPrint(t, doc); got != want {
		t.Fatalf("unscoped update was not applied: got %q want %q", got, want)
	}
	if r.aw.Len() != 1 || r.stats.AnsweredStateVector.Load() != 1 || r.stats.RemoteRejected.Load() != 0 {
		t.Fatalf("no-db fanout: awareness=%d answered=%d rejected=%d", r.aw.Len(), r.stats.AnsweredStateVector.Load(), r.stats.RemoteRejected.Load())
	}
}
