package room

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/protocol"
	"github.com/mesutokul/ycollab/internal/store"
)

// fakeStore is an in-memory stand-in for internal/store. The real thing is
// exercised by the integration tests; this is here so the room's own behaviour -
// what it writes and when - can be checked without a database.
type fakeStore struct {
	mu        sync.Mutex
	doc       store.Document
	appended  [][]byte
	snapshots [][]byte
	folded    [][]int64
	seq       int64
	loadErr   error
	appendErr error
	// owners is the tenancy boundary: whoever opened a name first owns it.
	owners map[string]store.UUID
}

// Ensure records the owner the first caller claimed and answers every later
// one with it, which is the part of the real store's behaviour the tenancy
// boundary rests on.
func (f *fakeStore) Ensure(_ context.Context, _ store.UUID, name string, owner store.UUID) (store.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return store.NilUUID, f.loadErr
	}
	if f.owners == nil {
		f.owners = make(map[string]store.UUID)
	}
	if found, ok := f.owners[name]; ok {
		return found, nil
	}
	f.owners[name] = owner
	return owner, nil
}

func (f *fakeStore) Load(_ context.Context, _ store.UUID, name string, owner store.UUID) (*store.Document, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	if found, ok := f.owners[name]; ok && found != owner {
		return nil, store.ErrWrongOwner
	}
	doc := f.doc
	doc.Owner = owner
	doc.Exists = true
	return &doc, nil
}

func (f *fakeStore) LoadAny(context.Context, store.UUID) (*store.Document, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	doc := f.doc
	return &doc, nil
}

func (f *fakeStore) Append(_ context.Context, _ store.UUID, payloads [][]byte) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appendErr != nil {
		return nil, f.appendErr
	}
	seqs := make([]int64, 0, len(payloads))
	for range payloads {
		f.seq++
		seqs = append(seqs, f.seq)
	}
	f.appended = append(f.appended, payloads...)
	return seqs, nil
}

func (f *fakeStore) Compact(_ context.Context, _ store.UUID, _ store.UUID, _ int64, generation int64, snapshot []byte, folded []int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots = append(f.snapshots, snapshot)
	f.folded = append(f.folded, folded)
	var watermark int64
	for _, seq := range folded {
		if seq > watermark {
			watermark = seq
		}
	}
	f.doc = store.Document{Exists: true, Snapshot: snapshot, SnapshotSeq: watermark,
		Owner: f.doc.Owner, Incarnation: f.doc.Incarnation, SnapshotGeneration: generation + 1}
	return generation + 1, nil
}

// lastFolded returns the rows the most recent snapshot claimed to cover.
func (f *fakeStore) lastFolded() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.folded) == 0 {
		return nil
	}
	return f.folded[len(f.folded)-1]
}

func (f *fakeStore) counts() (appended, snapshots int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.appended), len(f.snapshots)
}

func (f *fakeStore) lastSnapshot() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.snapshots) == 0 {
		return nil
	}
	return f.snapshots[len(f.snapshots)-1]
}

// eventually polls until cond holds, so the tests do not guess how long a
// background write takes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// runRoom starts a room and stops it when the test ends.
func runRoom(t *testing.T, cfg Config) *Room {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = t.Name()
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = time.Hour
	}
	if cfg.Logger == nil {
		cfg.Logger = quietLogger()
	}
	r := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return r
}

func TestUpdatesAreWritten(t *testing.T) {
	fake := &fakeStore{}
	r := runRoom(t, Config{Store: fake, FlushInterval: 5 * time.Millisecond})

	c := &fakeConn{id: 1}
	if err := r.Join(c); err != nil {
		t.Fatal(err)
	}
	update := readFixture(t, "text-insert-single", "state.bin")
	if err := r.Deliver(c, protocol.WriteUpdate(update)); err != nil {
		t.Fatal(err)
	}

	eventually(t, "the update to be written", func() bool {
		n, _ := fake.counts()
		return n == 1
	})
}

// The brief's threshold: fold the log into a snapshot every 500 updates. The
// snapshot has to be a document, not a delta, or a load would start from
// nothing.
func TestCompactionAfterThreshold(t *testing.T) {
	fake := &fakeStore{}
	// The threshold is the number of updates delivered, so the snapshot is
	// taken on the last one and must equal the whole document. A lower
	// threshold would leave the assertion depending on which fixture updates
	// happen to change the document.
	r := runRoom(t, Config{Store: fake, CompactAfter: 6, FlushInterval: 5 * time.Millisecond})

	c := &fakeConn{id: 1}
	if err := r.Join(c); err != nil {
		t.Fatal(err)
	}
	updates := scenarioUpdates(t, "text-three-client-interleaved")
	if len(updates) < 6 {
		t.Fatalf("need at least 6 updates, have %d", len(updates))
	}
	for _, u := range updates[:6] {
		if err := r.Deliver(c, protocol.WriteUpdate(u)); err != nil {
			t.Fatal(err)
		}
	}

	eventually(t, "a snapshot", func() bool {
		_, n := fake.counts()
		return n >= 1
	})

	// The snapshot must rebuild the document the room was holding.
	reference := crdt.NewDoc(9)
	for _, u := range updates[:6] {
		if err := reference.ApplyUpdate(u); err != nil {
			t.Fatal(err)
		}
	}
	restored := crdt.NewDoc(9)
	if err := restored.ApplyUpdate(fake.lastSnapshot()); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}
	if got, want := docPrint(t, restored), docPrint(t, reference); got != want {
		t.Fatalf("snapshot is not the document\n got %s\nwant %s", got, want)
	}
	// The snapshot has to name the rows it covers, or compaction would delete a
	// range and take rows it never saw with it (C6).
	if got := fake.lastFolded(); len(got) != 6 {
		t.Fatalf("snapshot covers %v, want the 6 rows that were written", got)
	}
}

// Persist-on-evict: an idle room writes a final snapshot before it disappears,
// so the next load is a single row read rather than a replay of the session.
func TestEvictionWritesASnapshot(t *testing.T) {
	fake := &fakeStore{}
	r := runRoom(t, Config{
		Store:         fake,
		Tick:          2 * time.Millisecond,
		IdleTimeout:   time.Nanosecond,
		FlushInterval: 2 * time.Millisecond,
	})

	c := &fakeConn{id: 1}
	if err := r.Join(c); err != nil {
		t.Fatal(err)
	}
	if err := r.Deliver(c, protocol.WriteUpdate(readFixture(t, "text-insert-single", "state.bin"))); err != nil {
		t.Fatal(err)
	}
	if err := r.Leave(c); err != nil {
		t.Fatal(err)
	}

	<-r.Done()

	// Done means persisted: the room writes its snapshot before signalling.
	appended, snapshots := fake.counts()
	if appended != 1 {
		t.Fatalf("%d updates written, want 1", appended)
	}
	if snapshots != 1 {
		t.Fatalf("%d snapshots written, want 1", snapshots)
	}
	restored := crdt.NewDoc(9)
	if err := restored.ApplyUpdate(fake.lastSnapshot()); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}
	reference := crdt.NewDoc(9)
	if err := reference.ApplyUpdate(readFixture(t, "text-insert-single", "state.bin")); err != nil {
		t.Fatal(err)
	}
	if got, want := docPrint(t, restored), docPrint(t, reference); got != want {
		t.Fatalf("evicted snapshot is not the document\n got %s\nwant %s", got, want)
	}
}

// A room comes back with what was on disk, snapshot first and then the log.
func TestRoomLoadsWhatWasPersisted(t *testing.T) {
	state := readFixture(t, "text-three-client-interleaved", "state.bin")
	updates := scenarioUpdates(t, "text-three-client-interleaved")

	reference := crdt.NewDoc(9)
	if err := reference.ApplyUpdate(state); err != nil {
		t.Fatal(err)
	}

	seqs := make([]int64, len(updates)-1)
	for i := range seqs {
		seqs[i] = int64(i + 2)
	}
	fake := &fakeStore{doc: store.Document{
		Snapshot:    updates[0],
		SnapshotSeq: 1,
		Updates:     updates[1:],
		Seqs:        seqs,
		LastSeq:     int64(len(updates)),
	}}
	r := runRoom(t, Config{Store: fake, FlushInterval: 5 * time.Millisecond})

	c := &fakeConn{id: 1}
	if err := r.Join(c); err != nil {
		t.Fatal(err)
	}
	sv, err := crdt.NewDoc(1).EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Deliver(c, protocol.WriteSyncStep1(sv)); err != nil {
		t.Fatal(err)
	}

	eventually(t, "the handshake", func() bool { return len(c.frames()) >= 2 })
	msg, err := protocol.Decode(c.frames()[0])
	if err != nil {
		t.Fatal(err)
	}
	restored := crdt.NewDoc(9)
	if err := restored.ApplyUpdate(msg.(protocol.SyncStep2Message).Update); err != nil {
		t.Fatal(err)
	}
	if got, want := docPrint(t, restored), docPrint(t, reference); got != want {
		t.Fatalf("a reconnecting client got a different document\n got %s\nwant %s", got, want)
	}
}

// If the document cannot be read, the room must refuse to serve it. Handing out
// an empty document under a name that has content would let clients merge into
// the blank one and write that back.
func TestLoadFailureClosesTheRoom(t *testing.T) {
	fake := &fakeStore{loadErr: errors.New("database is on fire")}
	r := New(Config{Name: "doc", Store: fake, IdleTimeout: time.Hour, Logger: quietLogger()})

	c := &fakeConn{id: 1}
	if err := r.Join(c); err != nil {
		t.Fatal(err)
	}
	r.Run(context.Background())

	if closed, code := c.status(); !closed || code != CloseInternalError {
		t.Fatalf("conn closed=%v code=%d, want 1011", closed, code)
	}
	select {
	case <-r.Done():
	default:
		t.Fatal("room did not stop")
	}
}

// A write failure must not take the document down: the clients still hold it,
// and they will push it again when they reconnect.
func TestWriteFailureKeepsServing(t *testing.T) {
	fake := &fakeStore{appendErr: errors.New("no")}
	r := runRoom(t, Config{Store: fake, FlushInterval: 2 * time.Millisecond})

	c := &fakeConn{id: 1}
	if err := r.Join(c); err != nil {
		t.Fatal(err)
	}
	if err := r.Deliver(c, protocol.WriteUpdate(readFixture(t, "text-insert-single", "state.bin"))); err != nil {
		t.Fatal(err)
	}
	sv, err := crdt.NewDoc(1).EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Deliver(c, protocol.WriteSyncStep1(sv)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the room to keep answering", func() bool { return len(c.frames()) >= 2 })
	if closed, _ := c.status(); closed {
		t.Fatal("a failed write closed a client")
	}
}

// quietLogger keeps the expected-error tests from printing alarming logs.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// Durable writes close the flush window: the update is on disk before the room
// relays it, so a crash cannot lose an edit a client believes landed.
func TestDurableWritesLandBeforeTheUpdateIsRelayed(t *testing.T) {
	store := &fakeStore{}
	r := runRoom(t, Config{
		Store: store,
		// Negative means write-through.
		FlushInterval: -1,
		// A flush interval that would otherwise hide the difference.
		CompactAfter: 1000,
	})

	author := &fakeConn{id: 1}
	peer := &fakeConn{id: 2}
	if err := r.Join(author); err != nil {
		t.Fatal(err)
	}
	if err := r.Join(peer); err != nil {
		t.Fatal(err)
	}

	update := readFixture(t, "text-insert-single", "update-000.bin")
	if err := r.Deliver(author, protocol.WriteUpdate(update)); err != nil {
		t.Fatal(err)
	}

	// The peer receiving it is the point at which a user believes the edit
	// exists, so by then it must already be written.
	eventually(t, "the peer to receive the update", func() bool {
		return len(peer.frames()) > 0
	})
	appended, _ := store.counts()
	if appended != 1 {
		t.Fatalf("%d updates were written by the time the peer saw it, want 1", appended)
	}
}

// And the batching default still batches, or durable writes would not be a
// choice at all.
func TestTheDefaultBatchesRatherThanWritingThrough(t *testing.T) {
	store := &fakeStore{}
	r := runRoom(t, Config{Store: store, FlushInterval: time.Hour, CompactAfter: 1000})

	author := &fakeConn{id: 1}
	if err := r.Join(author); err != nil {
		t.Fatal(err)
	}
	if err := r.Deliver(author, protocol.WriteUpdate(readFixture(t, "text-insert-single", "update-000.bin"))); err != nil {
		t.Fatal(err)
	}
	// Give the writer a chance to do the wrong thing.
	time.Sleep(50 * time.Millisecond)
	if appended, _ := store.counts(); appended != 0 {
		t.Fatalf("%d updates were written with an hour-long flush interval", appended)
	}
}
