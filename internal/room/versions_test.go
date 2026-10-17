package room

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/protocol"
	"github.com/mesutokul/ycollab/internal/store"
)

// fakeVersions records what the room asked to store, and answers the way the
// real store does: a version is written only when it has something new to say.
type fakeVersions struct {
	mu       sync.Mutex
	saved    []store.Version
	minAges  []time.Duration
	pruned   []int
	saveErr  error
	lastSV   []byte
	lastWhen time.Time
	now      func() time.Time
}

func (f *fakeVersions) SaveVersion(_ context.Context, _ store.UUID, _ store.UUID, _ int64, v store.Version, minAge time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return false, f.saveErr
	}
	f.minAges = append(f.minAges, minAge)
	now := time.Now()
	if f.now != nil {
		now = f.now()
	}
	if string(v.StateVector) == string(f.lastSV) {
		return false, nil
	}
	if minAge > 0 && !f.lastWhen.IsZero() && now.Sub(f.lastWhen) < minAge {
		return false, nil
	}
	f.saved = append(f.saved, v)
	f.lastSV, f.lastWhen = v.StateVector, now
	return true, nil
}

func (f *fakeVersions) PruneVersions(_ context.Context, _ store.UUID, _ store.UUID, _ int64, keep int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruned = append(f.pruned, keep)
	return 0, nil
}

func (f *fakeVersions) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.saved)
}

func (f *fakeVersions) all() []store.Version {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.Version(nil), f.saved...)
}

func (f *fakeVersions) prunes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.pruned...)
}

// A version is taken when the interval has passed and the document changed, and
// the payload has to be a document somebody can actually open.
func TestAVersionIsTakenOnTheInterval(t *testing.T) {
	versions := &fakeVersions{}
	fake := &fakeStore{}
	r := runRoom(t, Config{
		Store: fake, FlushInterval: 5 * time.Millisecond,
		Versions: versions, VersionInterval: 10 * time.Millisecond, VersionKeep: 5,
		Tick: 10 * time.Millisecond,
	})

	c := &fakeConn{id: 1}
	if err := r.Join(c); err != nil {
		t.Fatal(err)
	}
	updates := scenarioUpdates(t, "text-three-client-interleaved")
	for _, u := range updates {
		if err := r.Deliver(c, protocol.WriteUpdate(u)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "a version", func() bool { return versions.count() > 0 })

	got := versions.all()[0]
	if len(got.StateVector) == 0 {
		t.Error("the version carries no state vector")
	}
	doc := crdt.NewDoc(1)
	if err := doc.ApplyUpdate(got.Payload); err != nil {
		t.Fatalf("the version would not apply: %v", err)
	}
	if docPrint(t, doc) != canonical(t, updates) {
		t.Errorf("the version is a different document:\n got %s\nwant %s",
			docPrint(t, doc), canonical(t, updates))
	}
	// The interval is passed to the store rather than enforced here, because
	// that is where it can be applied without a race between replicas.
	if versions.minAges[0] != 10*time.Millisecond {
		t.Errorf("the store was asked with minAge %v", versions.minAges[0])
	}
	// A write is followed by a prune, and only a write: a document nobody edits
	// does not need its history counted every interval.
	eventually(t, "a prune", func() bool { return len(versions.prunes()) > 0 })
	if got := versions.prunes()[0]; got != 5 {
		t.Errorf("pruned to %d, want 5", got)
	}
}

// An idle document must cost nothing: no encode, no round trip, no row.
func TestAnIdleDocumentIsNotVersionedRepeatedly(t *testing.T) {
	versions := &fakeVersions{}
	now := time.Unix(1750000000, 0)
	r := New(Config{
		Name: "notes", IdleTimeout: time.Hour, Store: &fakeStore{},
		Versions: versions, VersionInterval: time.Minute,
		Now: func() time.Time { return now }, Logger: quietLogger(),
	})
	go r.persist(r.jobs)
	t.Cleanup(func() { r.stop(CloseGoingAway, "test") })

	c := &fakeConn{id: 1}
	r.handle(joinCmd{c})
	// Ten ticks, an hour apart, with nothing happening in between.
	for range 10 {
		now = now.Add(time.Hour)
		r.handle(tickCmd{now})
	}
	if got := versions.count(); got != 0 {
		t.Fatalf("%d versions were taken of a document nobody touched", got)
	}
	// The store was not even asked, which is the part that matters: the room
	// skips the encode as well as the round trip.
	versions.mu.Lock()
	asked := len(versions.minAges)
	versions.mu.Unlock()
	if asked != 0 {
		t.Errorf("the store was asked %d times about an unchanged document", asked)
	}

	// One edit, and the next tick takes one.
	r.handle(frameCmd{c, protocol.WriteUpdate(readFixture(t, "text-insert-single", "update-000.bin"))})
	now = now.Add(time.Hour)
	r.handle(tickCmd{now})
	eventually(t, "a version after the edit", func() bool { return versions.count() == 1 })
}

// Asking by hand is a different promise from the timer: the caller waits for
// the answer, and the interval does not apply to them.
func TestTakeVersionWaitsAndIgnoresTheInterval(t *testing.T) {
	versions := &fakeVersions{}
	r := runRoom(t, Config{
		Store: &fakeStore{}, FlushInterval: 5 * time.Millisecond,
		Versions: versions, VersionInterval: time.Hour, Tick: time.Hour,
	})
	c := &fakeConn{id: 1}
	if err := r.Join(c); err != nil {
		t.Fatal(err)
	}
	if err := r.Deliver(c, protocol.WriteUpdate(readFixture(t, "text-insert-single", "update-000.bin"))); err != nil {
		t.Fatal(err)
	}

	written, err := r.TakeVersion("before the migration")
	if err != nil {
		t.Fatalf("TakeVersion: %v", err)
	}
	if !written {
		t.Fatal("TakeVersion reported nothing written")
	}
	// It returned only after the write, so the version is there now rather than
	// eventually.
	got := versions.all()
	if len(got) != 1 {
		t.Fatalf("%d versions, want 1", len(got))
	}
	if got[0].Label != "before the migration" {
		t.Errorf("the label is %q", got[0].Label)
	}
	// minAge zero: they asked, so the interval does not hold them off.
	if versions.minAges[0] != 0 {
		t.Errorf("the store was asked with minAge %v, want 0", versions.minAges[0])
	}

	// Asking again with nothing changed is not an error, and writes nothing.
	written, err = r.TakeVersion("again")
	if err != nil {
		t.Fatalf("the second TakeVersion: %v", err)
	}
	if written {
		t.Error("an unchanged document was versioned twice")
	}
}

// A server with no database keeps no history, and says so rather than
// pretending to have taken one.
func TestTakeVersionWithoutAStore(t *testing.T) {
	r := runRoom(t, Config{Tick: time.Hour})
	if _, err := r.TakeVersion("x"); !errors.Is(err, ErrNoVersioning) {
		t.Fatalf("TakeVersion returned %v, want ErrNoVersioning", err)
	}
}

// A store that is refusing writes must not leave the caller waiting forever.
func TestTakeVersionSurvivesAFailingStore(t *testing.T) {
	versions := &fakeVersions{saveErr: errors.New("the database is down")}
	r := runRoom(t, Config{
		Store: &fakeStore{}, FlushInterval: 5 * time.Millisecond,
		Versions: versions, Tick: time.Hour,
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if written, err := r.TakeVersion("x"); written || !errors.Is(err, versions.saveErr) {
			t.Errorf("TakeVersion = %v, %v; want false, store error", written, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("TakeVersion never returned while the store was failing")
	}
}

func TestVersionCompletionWinsSimultaneousWriterShutdown(t *testing.T) {
	done := make(chan struct{})
	persistDone := make(chan struct{})
	result := make(chan error, 1)
	result <- nil
	close(done)
	close(persistDone)
	writtenValue := true
	if written, err := waitVersion(done, persistDone, result, &writtenValue); !written || err != nil {
		t.Fatalf("completed version during shutdown = %v, %v; want true, nil", written, err)
	}

	unfinished := make(chan struct{})
	close(unfinished)
	if written, err := waitVersion(make(chan struct{}), unfinished, make(chan error, 1), &writtenValue); written || !errors.Is(err, ErrClosed) {
		t.Fatalf("unfinished version during shutdown = %v, %v; want false, ErrClosed", written, err)
	}
}

func TestCommandReplyWinsSimultaneousRoomShutdown(t *testing.T) {
	done := make(chan struct{})
	close(done)
	reply := make(chan error, 1)
	want := errors.New("stale document")
	reply <- want
	if got := waitCommandReply(reply, done); !errors.Is(got, want) {
		t.Fatalf("waitCommandReply() = %v, want %v", got, want)
	}

	if got := waitCommandReply(make(chan error, 1), done); !errors.Is(got, ErrClosed) {
		t.Fatalf("waitCommandReply() without a result = %v, want %v", got, ErrClosed)
	}
}
