package room

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/cluster"
	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/protocol"
	"github.com/mesutokul/ycollab/internal/store"
)

func TestStaleRoomCannotWriteRecreatedDocument(t *testing.T) {
	dbURL := os.Getenv("YCOLLAB_TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("YCOLLAB_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	owner := "lifecycle-test-owner"
	updates := scenarioUpdates(t, "text-three-client-interleaved")
	if len(updates) < 3 {
		t.Fatalf("need at least 3 fixture updates, have %d", len(updates))
	}

	old := runRoom(t, Config{
		Name: name, Owner: owner, Store: s,
		FlushInterval: -1, CompactAfter: 1000,
	})
	conn := &fakeConn{id: 1}
	if err := old.Join(conn); err != nil {
		t.Fatal(err)
	}
	if err := old.Deliver(conn, protocol.WriteUpdate(updates[0])); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Read(nil); err != nil {
		t.Fatalf("read after old-room write: %v", err)
	}

	id := store.DocumentID(name)
	oldDoc, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldDoc.Updates) != 1 {
		t.Fatalf("old room persisted %d updates, want 1", len(oldDoc.Updates))
	}
	deleted, err := s.Delete(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("delete did not remove the old document")
	}
	if _, err := s.Load(ctx, id, name, store.OwnerID(owner)); err != nil {
		t.Fatalf("recreate document for same owner: %v", err)
	}

	if err := old.Deliver(conn, protocol.WriteUpdate(updates[2])); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Read(nil); err != nil {
		t.Fatalf("read after stale-room write: %v", err)
	}

	current, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if current.Snapshot != nil || len(current.Updates) != 0 {
		doc := crdt.NewDoc(0)
		if current.Snapshot != nil {
			if err := doc.ApplyUpdate(current.Snapshot); err != nil {
				t.Fatal(err)
			}
		}
		for _, update := range current.Updates {
			if err := doc.ApplyUpdate(update); err != nil {
				t.Fatal(err)
			}
		}
		t.Fatalf("stale room contaminated recreated document: snapshot=%t updates=%d text=%q", current.Snapshot != nil, len(current.Updates), doc.Text("text").String())
	}
}

func TestRecreatedRoomFiltersOldIncarnationAndSyncsCurrentPeers(t *testing.T) {
	dbURL := os.Getenv("YCOLLAB_TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("YCOLLAB_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	bus := cluster.NewMemory()
	t.Cleanup(func() { _ = bus.Close() })
	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	owner := "lifecycle-test-owner"
	ownerID := store.OwnerID(owner)

	old := runRoom(t, Config{
		Name: name, Owner: owner, Store: s, Bus: bus, NodeID: 101,
		FlushInterval: -1, CompactAfter: 1000, Tick: time.Hour, AntiEntropy: time.Hour,
	})
	oldConn := &fakeConn{id: 1}
	if err := old.Join(oldConn); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Read(nil); err != nil {
		t.Fatalf("load old room: %v", err)
	}
	oldInitial := readFixture(t, "text-insert-single", "update-000.bin")
	if err := old.Deliver(oldConn, protocol.WriteUpdate(oldInitial)); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Read(nil); err != nil {
		t.Fatalf("persist old-room edit: %v", err)
	}
	oldDoc, err := s.LoadAny(ctx, store.DocumentID(name))
	if err != nil {
		t.Fatal(err)
	}
	if !oldDoc.Exists || oldDoc.Owner != ownerID || len(oldDoc.Updates) != 1 {
		t.Fatalf("old document: exists=%t owner=%s updates=%d", oldDoc.Exists, oldDoc.Owner, len(oldDoc.Updates))
	}

	deleted, err := s.Delete(ctx, store.DocumentID(name))
	if err != nil || !deleted {
		t.Fatalf("delete old document: deleted=%t err=%v", deleted, err)
	}
	if _, err := s.Load(ctx, store.DocumentID(name), name, ownerID); err != nil {
		t.Fatalf("recreate document for same owner: %v", err)
	}
	currentDoc, err := s.LoadAny(ctx, store.DocumentID(name))
	if err != nil {
		t.Fatal(err)
	}
	if !currentDoc.Exists || currentDoc.Owner != oldDoc.Owner || currentDoc.Incarnation == oldDoc.Incarnation {
		t.Fatalf("recreated document identity: exists=%t owner=%s old owner=%s incarnation=%d old incarnation=%d", currentDoc.Exists, currentDoc.Owner, oldDoc.Owner, currentDoc.Incarnation, oldDoc.Incarnation)
	}

	currentConfig := func(nodeID uint64) Config {
		return Config{
			Name: name, Owner: owner, Store: s, Bus: bus, NodeID: nodeID,
			FlushInterval: -1, CompactAfter: 1000, Tick: time.Hour, AntiEntropy: time.Hour,
		}
	}
	b := runRoom(t, currentConfig(102))
	c := runRoom(t, currentConfig(103))
	bConn := &fakeConn{id: 2}
	cConn := &fakeConn{id: 3}
	if err := b.Join(bConn); err != nil {
		t.Fatal(err)
	}
	if err := c.Join(cConn); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Read(nil); err != nil {
		t.Fatalf("load current room B: %v", err)
	}
	if _, err := c.Read(nil); err != nil {
		t.Fatalf("load current room C: %v", err)
	}
	if b.incarnation != currentDoc.Incarnation || c.incarnation != currentDoc.Incarnation {
		t.Fatalf("current rooms loaded incarnations B=%d C=%d, want %d", b.incarnation, c.incarnation, currentDoc.Incarnation)
	}

	fresh := [][]byte{
		readFixture(t, "text-three-client-interleaved", "update-000.bin"),
		readFixture(t, "text-three-client-interleaved", "update-002.bin"),
	}
	wantDoc := crdt.NewDoc(0)
	for _, update := range fresh {
		if err := wantDoc.ApplyUpdate(update); err != nil {
			t.Fatal(err)
		}
	}
	want := docPrint(t, wantDoc)
	stale := readFixture(t, "text-three-client-interleaved", "state.bin")
	staleWitness := crdt.NewDoc(0)
	for _, update := range fresh {
		if err := staleWitness.ApplyUpdate(update); err != nil {
			t.Fatal(err)
		}
	}
	if err := staleWitness.ApplyUpdate(stale); err != nil {
		t.Fatal(err)
	}
	if staleWitness.PendingCount() != 0 || docPrint(t, staleWitness) == want {
		t.Fatal("stale fixture does not fully change the fresh-only document")
	}
	for _, update := range fresh {
		if err := c.Deliver(cConn, protocol.WriteUpdate(update)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Read(nil); err != nil {
			t.Fatalf("persist current-room edit: %v", err)
		}
	}
	eventually(t, "same-incarnation update on room B", func() bool {
		got, err := roomDocumentPrint(t, b)
		return err == nil && got == want
	})

	oldSnapshot, err := old.Read(nil)
	if err != nil {
		t.Fatalf("read old state vector: %v", err)
	}
	diff, err := wantDoc.EncodeDiff(oldSnapshot.StateVector)
	if err != nil {
		t.Fatal(err)
	}
	if isEmptyUpdate(diff) {
		t.Fatal("fresh-only reference has no diff for old room; anti-entropy check would be vacuous")
	}
	stateVectorsBefore := b.stats.AnsweredStateVector.Load()
	stateVectorsPublished := old.stats.PublishedStateVector.Load()
	if err := old.send(tickCmd{now: time.Now().Add(2 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "old room to publish anti-entropy state", func() bool {
		return old.stats.PublishedStateVector.Load() > stateVectorsPublished
	})
	oldPublished := old.stats.PublishedUpdate.Load()
	if err := old.Deliver(oldConn, protocol.WriteUpdate(stale)); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Read(nil); err != nil {
		t.Fatalf("process stale-room edit: %v", err)
	}
	eventually(t, "old room to publish its tagged edit", func() bool {
		return old.stats.PublishedUpdate.Load() > oldPublished
	})

	// The repeated current-token update is a FIFO barrier behind the stale
	// update and anti-entropy announcement on each room's inbound channel.
	currentPublished := c.stats.PublishedUpdate.Load()
	remoteApplied := b.stats.RemoteUpdateApplied.Load()
	if err := c.Deliver(cConn, protocol.WriteUpdate(fresh[1])); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(nil); err != nil {
		t.Fatalf("process current-room barrier update: %v", err)
	}
	eventually(t, "current-token barrier update on room B", func() bool {
		return c.stats.PublishedUpdate.Load() > currentPublished && b.stats.RemoteUpdateApplied.Load() > remoteApplied
	})
	if got, err := roomDocumentPrint(t, b); err != nil || got != want {
		t.Fatalf("room B after stale and current-token traffic: state=%q want=%q err=%v", got, want, err)
	}
	if got := b.stats.AnsweredStateVector.Load(); got != stateVectorsBefore {
		t.Fatalf("room B answered %d stale-incarnation state vectors, want none", got-stateVectorsBefore)
	}

	stored, err := s.LoadAny(ctx, store.DocumentID(name))
	if err != nil {
		t.Fatal(err)
	}
	if stored.Owner != ownerID || stored.Incarnation != currentDoc.Incarnation {
		t.Fatalf("stored identity changed: owner=%s incarnation=%d", stored.Owner, stored.Incarnation)
	}
	for _, update := range stored.Updates {
		if bytes.Equal(update, stale) {
			t.Fatal("stale room persisted its update into the recreated document")
		}
	}
}

func roomDocumentPrint(t *testing.T, r *Room) (string, error) {
	snapshot, err := r.Read(nil)
	if err != nil {
		return "", err
	}
	doc := crdt.NewDoc(0)
	if err := doc.ApplyUpdate(snapshot.Update); err != nil {
		return "", err
	}
	return docPrint(t, doc), nil
}
