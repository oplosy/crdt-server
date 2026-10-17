package room

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
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
	mu               sync.Mutex
	doc              store.Document
	appended         [][]byte
	snapshots        [][]byte
	folded           [][]int64
	seq              int64
	loadErr          error
	appendErr        error
	compactConflicts int
	compactCalls     int
	conflictDoc      store.Document
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
	f.doc.Owner = owner
	f.doc.Exists = true
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
	f.doc.Exists = true
	for i, payload := range payloads {
		copyPayload := append([]byte(nil), payload...)
		f.doc.Updates = append(f.doc.Updates, copyPayload)
		f.doc.Seqs = append(f.doc.Seqs, seqs[i])
		if seqs[i] > f.doc.LastSeq {
			f.doc.LastSeq = seqs[i]
		}
	}
	return seqs, nil
}

func (f *fakeStore) Compact(_ context.Context, _ store.UUID, owner store.UUID, incarnation, generation int64, snapshot []byte, folded []int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.compactCalls++
	if f.compactConflicts > 0 {
		f.compactConflicts--
		f.doc = f.conflictDoc
		return generation, store.ErrStaleSnapshot
	}
	if owner != f.doc.Owner || incarnation != f.doc.Incarnation || generation != f.doc.SnapshotGeneration {
		return generation, store.ErrStaleSnapshot
	}
	f.snapshots = append(f.snapshots, snapshot)
	f.folded = append(f.folded, folded)
	var watermark int64
	for _, seq := range folded {
		if seq > watermark {
			watermark = seq
		}
	}
	if f.doc.SnapshotSeq > watermark {
		watermark = f.doc.SnapshotSeq
	}
	covered := make(map[int64]struct{}, len(folded))
	for _, seq := range folded {
		covered[seq] = struct{}{}
	}
	var updates [][]byte
	var seqs []int64
	for i, seq := range f.doc.Seqs {
		if _, ok := covered[seq]; ok {
			continue
		}
		updates = append(updates, f.doc.Updates[i])
		seqs = append(seqs, seq)
	}
	f.doc = store.Document{Exists: true, Snapshot: append([]byte(nil), snapshot...), SnapshotSeq: watermark,
		Updates: updates, Seqs: seqs, Owner: f.doc.Owner, Incarnation: f.doc.Incarnation,
		SnapshotGeneration: generation + 1, LastSeq: f.doc.LastSeq}
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

func TestCompactRebasesAfterGenerationConflict(t *testing.T) {
	updates := scenarioUpdates(t, "text-three-client-interleaved")
	if len(updates) < 7 {
		t.Fatalf("need at least 7 scenario updates, have %d", len(updates))
	}
	x, y, z := updates[0], updates[3], updates[6]
	state := func(updates ...[]byte) []byte {
		t.Helper()
		doc := crdt.NewDoc(0)
		for _, update := range updates {
			if err := doc.ApplyUpdate(update); err != nil {
				t.Fatal(err)
			}
		}
		encoded, err := doc.EncodeStateAsUpdate(nil)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}

	owner := store.NilUUID
	initial := store.Document{Exists: true, Snapshot: x, SnapshotSeq: 1, Owner: owner,
		Incarnation: 11, SnapshotGeneration: 0}
	concurrent := store.Document{Exists: true, Snapshot: state(x, y), SnapshotSeq: 2, Owner: owner,
		Incarnation: 11, SnapshotGeneration: 1}
	fake := &fakeStore{doc: initial, compactConflicts: 1, conflictDoc: concurrent}
	r := New(Config{Name: "rebase", Store: fake, Logger: quietLogger()})
	r.incarnation = initial.Incarnation
	r.folded = []int64{1, 3}

	if !r.compact(state(x, z)) {
		t.Fatal("compaction did not recover from one generation conflict")
	}
	stored, err := fake.LoadAny(context.Background(), r.docID)
	if err != nil {
		t.Fatal(err)
	}
	restored := crdt.NewDoc(0)
	if err := restored.ApplyUpdate(stored.Snapshot); err != nil {
		t.Fatal(err)
	}
	if got, want := restored.Text("text").String(), "zeroonethree"; got != want {
		t.Fatalf("rebased snapshot text %q, want %q", got, want)
	}
	if len(r.folded) != 0 {
		t.Fatalf("successful compaction retained folded rows %v", r.folded)
	}
	if got := r.doc.Text("text").String(); got != "" {
		t.Fatalf("persist goroutine mutated the live actor document to %q", got)
	}
}

func TestCompactDoesNotRebaseAcrossDocumentIncarnation(t *testing.T) {
	fake := &fakeStore{doc: store.Document{Exists: true, Owner: store.NilUUID, Incarnation: 12}}
	r := New(Config{Name: "old-room", Store: fake, Logger: quietLogger()})
	r.incarnation = 11
	r.folded = []int64{1}

	if !r.compact([]byte{1}) {
		t.Fatal("an incarnation mismatch should be terminal for this candidate")
	}
	if _, snapshots := fake.counts(); snapshots != 0 {
		t.Fatalf("stale room wrote %d snapshots into the recreated document", snapshots)
	}
}

func TestCompactKeepsPendingRowsUntilDependenciesArrive(t *testing.T) {
	updates := scenarioUpdates(t, "text-three-client-interleaved")
	var pendingUpdate []byte
	for _, update := range updates {
		doc := crdt.NewDoc(0)
		if err := doc.ApplyUpdate(update); err != nil {
			t.Fatal(err)
		}
		if doc.PendingCount() > 0 {
			pendingUpdate = update
			break
		}
	}
	if pendingUpdate == nil {
		t.Fatal("scenario no longer contains an update that waits for missing structs")
	}

	pendingDoc := crdt.NewDoc(0)
	if err := pendingDoc.ApplyUpdate(pendingUpdate); err != nil {
		t.Fatal(err)
	}
	if pendingDoc.PendingCount() == 0 {
		t.Fatal("selected update did not remain pending")
	}
	unresolved, err := pendingDoc.EncodeStateAsUpdate(nil)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeStore{doc: store.Document{Exists: true, Owner: store.NilUUID,
		Incarnation: 13, Updates: [][]byte{pendingUpdate}, Seqs: []int64{1}, LastSeq: 1}, seq: 1}
	r := New(Config{Name: "pending", Store: fake, Logger: quietLogger()})
	r.incarnation = 13
	r.folded = []int64{1}
	if r.compact(unresolved) {
		t.Fatal("compaction accepted a document with unresolved structs")
	}
	if _, snapshots := fake.counts(); snapshots != 0 {
		t.Fatalf("compacted %d times while a struct was pending", snapshots)
	}
	if len(r.folded) != 1 || r.folded[0] != 1 {
		t.Fatalf("pending compaction discarded folded rows %v", r.folded)
	}

	dependency := false
	var dependencyUpdate []byte
	dependencyIndex := -1
	for i, update := range updates {
		wasPending := pendingDoc.PendingCount() > 0
		if err := pendingDoc.ApplyUpdate(update); err != nil {
			t.Fatal(err)
		}
		if wasPending && pendingDoc.PendingCount() == 0 {
			dependency = true
			dependencyUpdate = update
			dependencyIndex = i
			break
		}
	}
	if !dependency {
		t.Fatal("scenario no longer includes the missing dependency")
	}
	complete, err := pendingDoc.EncodeStateAsUpdate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fake.Append(context.Background(), r.docID, [][]byte{dependencyUpdate}); err != nil {
		t.Fatal(err)
	}
	r.folded = append(r.folded, 2)
	if !r.compact(complete) {
		t.Fatal("compaction did not recover after the missing dependency arrived")
	}
	stored, err := fake.LoadAny(context.Background(), r.docID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Updates) != 0 {
		t.Fatalf("resolved compaction left %d log rows", len(stored.Updates))
	}
	restored := crdt.NewDoc(0)
	if err := restored.ApplyUpdate(stored.Snapshot); err != nil {
		t.Fatal(err)
	}
	reference := crdt.NewDoc(0)
	if err := reference.ApplyUpdate(pendingUpdate); err != nil {
		t.Fatal(err)
	}
	for _, update := range updates[:dependencyIndex+1] {
		if err := reference.ApplyUpdate(update); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := docPrint(t, restored), docPrint(t, reference); got != want {
		t.Fatalf("resolved snapshot differs from merged dependency state\n got %s\nwant %s", got, want)
	}
}

func TestCompactRetainsMalformedPersistedUpdate(t *testing.T) {
	fake := &fakeStore{doc: store.Document{Exists: true, Owner: store.NilUUID, Incarnation: 14,
		Updates: [][]byte{{0xff}}, Seqs: []int64{7}}}
	r := New(Config{Name: "corrupt", Store: fake, Logger: quietLogger()})
	r.incarnation = 14
	r.folded = []int64{7}
	if r.compact([]byte{0x00}) {
		t.Fatal("compaction accepted a malformed persisted row")
	}
	stored, err := fake.LoadAny(context.Background(), r.docID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Updates) != 1 || len(r.folded) != 1 {
		t.Fatalf("malformed row was pruned: updates=%d folded=%v", len(stored.Updates), r.folded)
	}
}

func TestCompactRejectsOwnerMismatch(t *testing.T) {
	fake := &fakeStore{doc: store.Document{Exists: true, Owner: store.OwnerID("other"), Incarnation: 15}}
	r := New(Config{Name: "tenant", Owner: "owner", Store: fake, Logger: quietLogger()})
	r.incarnation = 15
	r.folded = []int64{1}
	if !r.compact([]byte{0}) {
		t.Fatal("owner mismatch should stop this candidate")
	}
	if _, snapshots := fake.counts(); snapshots != 0 {
		t.Fatalf("owner mismatch wrote %d snapshots", snapshots)
	}
}

func TestCompactStopsWhenDocumentIsMissing(t *testing.T) {
	fake := &fakeStore{}
	r := New(Config{Name: "missing", Store: fake, Logger: quietLogger()})
	r.folded = []int64{1}
	if !r.compact([]byte{0}) {
		t.Fatal("missing document should terminate this candidate")
	}
	if _, snapshots := fake.counts(); snapshots != 0 {
		t.Fatalf("missing document wrote %d snapshots", snapshots)
	}
}

func TestPersistRetriesDeferredCompactionAtShutdown(t *testing.T) {
	initial := store.Document{Exists: true, Owner: store.NilUUID, Incarnation: 16}
	fake := &fakeStore{doc: initial, conflictDoc: initial, compactConflicts: 3}
	r := New(Config{Name: "shutdown-retry", Store: fake, FlushInterval: time.Hour, Logger: quietLogger()})
	r.incarnation = initial.Incarnation
	r.folded = []int64{1}
	jobs := make(chan persistJob, 1)
	go r.persist(jobs)
	jobs <- persistJob{snapshot: readFixture(t, "text-insert-single", "state.bin")}
	close(jobs)

	select {
	case <-r.persistDone:
	case <-time.After(2 * time.Second):
		t.Fatal("persist worker did not stop after its bounded shutdown retry")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.compactCalls != 4 {
		t.Fatalf("compaction attempts = %d, want 3 conflicts plus 1 shutdown retry", fake.compactCalls)
	}
	if len(fake.snapshots) != 1 || len(r.folded) != 0 {
		t.Fatalf("shutdown retry result: snapshots=%d folded=%v", len(fake.snapshots), r.folded)
	}
}

func TestCompactKeepsCandidateAfterConflictExhaustion(t *testing.T) {
	initial := store.Document{Exists: true, Owner: store.NilUUID, Incarnation: 17}
	fake := &fakeStore{doc: initial, conflictDoc: initial, compactConflicts: 3}
	r := New(Config{Name: "deferred", Store: fake, Logger: quietLogger()})
	r.incarnation = initial.Incarnation
	r.folded = []int64{1}
	candidate := readFixture(t, "text-insert-single", "state.bin")
	if r.compact(candidate) {
		t.Fatal("compaction unexpectedly succeeded after three forced conflicts")
	}
	if fake.compactCalls != 3 || len(r.folded) != 1 {
		t.Fatalf("after exhaustion: attempts=%d folded=%v", fake.compactCalls, r.folded)
	}
	if !r.compact(candidate) {
		t.Fatal("deferred candidate did not succeed on a later retry")
	}
	if fake.compactCalls != 4 || len(r.folded) != 0 {
		t.Fatalf("after retry: attempts=%d folded=%v", fake.compactCalls, r.folded)
	}
}

func TestCompactRebasesConcurrentReplicaUpdatesInPostgres(t *testing.T) {
	const dbEnv = "YCOLLAB_TEST_DATABASE_URL"
	dbURL := os.Getenv(dbEnv)
	if dbURL == "" {
		t.Skipf("%s is not set; start deploy/docker-compose.yml to run this", dbEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	id := store.DocumentID(name)
	owner := store.NilUUID
	if _, err := s.Load(ctx, id, name, owner); err != nil {
		t.Fatal(err)
	}
	updates := scenarioUpdates(t, "text-three-client-interleaved")
	if len(updates) < 7 {
		t.Fatalf("need at least 7 scenario updates, have %d", len(updates))
	}
	x, y, z := updates[0], updates[3], updates[6]
	xSeqs, err := s.Append(ctx, id, [][]byte{x})
	if err != nil {
		t.Fatal(err)
	}

	// Replica A loads X and later compacts its stale X+Z state.
	a := New(Config{Name: name, Store: s, Logger: quietLogger()})
	if err := a.load(ctx); err != nil {
		t.Fatal(err)
	}
	aDoc := crdt.NewDoc(0)
	if err := aDoc.ApplyUpdate(x); err != nil {
		t.Fatal(err)
	}
	if err := aDoc.ApplyUpdate(z); err != nil {
		t.Fatal(err)
	}
	az, err := aDoc.EncodeStateAsUpdate(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Replica B commits X+Y first, removing X's log row under its snapshot.
	bDoc := crdt.NewDoc(0)
	if err := bDoc.ApplyUpdate(x); err != nil {
		t.Fatal(err)
	}
	if err := bDoc.ApplyUpdate(y); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, id, [][]byte{y}); err != nil {
		t.Fatal(err)
	}
	xy, err := bDoc.EncodeStateAsUpdate(nil)
	if err != nil {
		t.Fatal(err)
	}
	bLoaded, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(ctx, id, owner, bLoaded.Incarnation, bLoaded.SnapshotGeneration, xy, bLoaded.Seqs); err != nil {
		t.Fatalf("replica B compaction: %v", err)
	}
	zSeqs, err := s.Append(ctx, id, [][]byte{z})
	if err != nil {
		t.Fatal(err)
	}
	a.folded = append(a.folded, zSeqs...)

	if !a.compact(az) {
		t.Fatal("replica A failed to rebase and compact")
	}
	stored, err := s.Load(ctx, id, name, owner)
	if err != nil {
		t.Fatal(err)
	}
	restored := crdt.NewDoc(0)
	if stored.Snapshot != nil {
		if err := restored.ApplyUpdate(stored.Snapshot); err != nil {
			t.Fatal(err)
		}
	}
	for _, update := range stored.Updates {
		if err := restored.ApplyUpdate(update); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := restored.Text("text").String(), "zeroonethree"; got != want {
		t.Fatalf("Postgres reload text %q, want merged X/Y/Z state %q", got, want)
	}
	if len(stored.Updates) != 0 {
		t.Fatalf("successful rebase left %d already-folded log rows", len(stored.Updates))
	}
	if len(a.folded) != 0 {
		t.Fatalf("successful compaction retained rows %v (initial seqs %v)", a.folded, xSeqs)
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
