package room

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mesutokul/ycollab/internal/cluster"
	"github.com/mesutokul/ycollab/internal/metrics"
	"github.com/mesutokul/ycollab/internal/store"
)

// ErrTooManyRooms means the resident-room cap is reached and every resident
// room has somebody connected to it, so none of them can be evicted to make
// space. The cap bounds worst-case memory: without one, a client per document
// name is enough to hold an unbounded number of documents.
var ErrTooManyRooms = errors.New("room: resident room limit reached")

// ErrManagerClosed means the manager is draining: its context is cancelled and
// Wait has been called, so no new room will be started. A join that arrives
// during shutdown - a WebSocket handler that outlived srv.Shutdown, since a
// hijacked connection is not one Shutdown waits for - gets this instead of
// racing Wait for the room WaitGroup.
var ErrManagerClosed = errors.New("room: manager is shutting down")

// ManagerConfig configures the registry and the rooms it creates.
type ManagerConfig struct {
	// Room is the template every room is created from. Name, OnExit and Logger
	// are filled in per room.
	Room Config
	// MaxRooms caps resident rooms. Zero means unlimited.
	MaxRooms int
	// MaxMemory caps what the resident documents are estimated to cost, in
	// bytes. Zero means unlimited.
	//
	// It is the bound that matters, because a pod's limit is written in bytes
	// and MaxRooms is written in documents. Both can be set; whichever is
	// reached first evicts.
	MaxMemory int64
}

// A Manager owns the set of resident rooms and starts one goroutine per room.
//
// This is the only place in the package with a lock, and it guards nothing but
// the name-to-room map.
type Manager struct {
	cfg ManagerConfig
	ctx context.Context

	mu sync.Mutex
	// closed is set by Wait and checked before a room is started, so that the
	// increment to wg and Wait's read of it are ordered by mu rather than
	// racing. Once true the manager starts no more rooms.
	closed bool
	rooms  map[string]*Room
	// used orders rooms for the LRU cap: the value is a counter, not a clock,
	// so two joins in the same nanosecond still order.
	used  map[string]uint64
	clock uint64
	// pressure names whichever bound last forced an eviction, for the metric.
	// Written and read under mu, on the path that has just decided to evict.
	pressure string
	wg       sync.WaitGroup
}

// NewManager returns a manager whose rooms stop when ctx is cancelled.
//
// Every room it creates shares one Stats and one node id: both describe the
// process, not the document. A node id per room would filter correctly but would
// make the counters meaningless and would multiply the anti-entropy traffic by
// the number of resident rooms.
func NewManager(ctx context.Context, cfg ManagerConfig) *Manager {
	if cfg.Room.Stats == nil {
		cfg.Room.Stats = &Stats{}
	}
	if cfg.Room.Bus != nil && cfg.Room.NodeID == 0 {
		cfg.Room.NodeID = cluster.NewNodeID()
	}
	if cfg.Room.Metrics == nil {
		cfg.Room.Metrics = metrics.Nop()
	}
	m := &Manager{
		cfg:   cfg,
		ctx:   ctx,
		rooms: make(map[string]*Room),
		used:  make(map[string]uint64),
	}
	if cfg.MaxMemory > 0 {
		every := cfg.Room.UsageInterval
		if every <= 0 {
			every = DefaultUsageInterval
		}
		go m.sweepBudget(ctx, every)
	}
	return m
}

// ErrWrongOwner means the document exists and belongs to another tenant. The
// gateway turns it into the same refusal a bad token gets: a client must not be
// able to tell "this document is not yours" from "there is no such document",
// or the boundary becomes a way to enumerate other people's document names.
var ErrWrongOwner = store.ErrWrongOwner

// Join places conn in the named room on behalf of owner, creating and starting
// the room if needed.
//
// owner is the tenant the connection's grant named, empty for a connection that
// claims none. It is settled against the database once, when the document is
// opened, and held on the room - so the second and every later connection is
// answered from memory rather than costing a query.
//
// A room can decide to evict itself at any moment, including between the lookup
// and the handover. The room's shutdown removes itself from this map before it
// starts refusing joins, so observing ErrClosed proves the map no longer holds
// that room and one retry is enough.
func (m *Manager) Join(name string, conn Conn, owner string) (*Room, error) {
	for {
		r, err := m.get(name, owner)
		if err != nil {
			return nil, err
		}
		switch err := r.Join(conn); {
		case err == nil:
			return r, nil
		case errors.Is(err, ErrClosed):
			continue
		default:
			return nil, err
		}
	}
}

func (m *Manager) get(name, owner string) (*Room, error) {
	for {
		r, candidates, err := m.tryGet(name, owner)
		if err != nil {
			return nil, err
		}
		if r != nil {
			return r, nil
		}
		// At a cap - either the room count or the memory budget. Evict the least
		// recently used idle room and try again; with persistence in place that
		// costs a snapshot write, not the document. Done outside the lock,
		// because evicting waits for the room to write itself out.
		//
		// When nothing can be evicted, every resident room has somebody
		// connected to it, and the honest answer is to refuse rather than to
		// exceed the bound quietly.
		if !m.evictOne(candidates) {
			return nil, ErrTooManyRooms
		}
	}
}

// tryGet returns the room, or the eviction candidates in least-recently-used
// order when the cap is in the way.
func (m *Manager) tryGet(name, owner string) (*Room, []*Room, error) {
	want := store.OwnerID(owner)

	// A document that is already open has already been settled, so the common
	// case - every reconnect, every second tab - is a map lookup and a
	// comparison rather than a query. Compared as ids, not as the names they
	// came from: a tenant may be named by its slug in one token and by its UUID
	// in another, and both are the same owner.
	if r, resident, err := m.resident(name, want); resident {
		return r, nil, err
	}

	// Not open here. The database is the authority on whose it is, and it is
	// asked outside the lock: this lock is the one every join in the process
	// shares, and a round trip under it would serialise the whole node behind
	// one document being opened.
	if err := m.settleOwner(name, want); err != nil {
		return nil, nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Somebody may have opened it while the query was in flight. They settled
	// the same question against the same row, so agreeing with them is right -
	// but the comparison still happens, because "somebody" may be another
	// tenant who was refused and a third who was not.
	if r, ok := m.rooms[name]; ok {
		if r.owner != want {
			return nil, nil, fmt.Errorf("%w: %s", ErrWrongOwner, name)
		}
		m.touch(name)
		return r, nil, nil
	}
	// Past here a room is created and wg is incremented. If Wait has run, that
	// increment would race its read of wg, so refuse instead - the caller is a
	// connection that arrived while the process was already going down.
	if m.closed {
		return nil, nil, ErrManagerClosed
	}
	if m.cfg.MaxRooms > 0 && len(m.rooms) >= m.cfg.MaxRooms {
		m.pressure = "cap"
		return nil, m.byLeastRecentlyUsed(), nil
	}
	// The byte budget is checked before the room is created rather than after,
	// so the document about to be opened is never a candidate to evict.
	if over, candidates := m.overBudget(); over {
		m.pressure = "budget"
		return nil, candidates, nil
	}
	m.touch(name)
	cfg := m.cfg.Room
	cfg.Name = name
	cfg.Owner = owner
	r := New(cfg)
	// The room's exit hook removes this exact room, never whatever happens to
	// be registered under the name later. Set after New and before Run, so no
	// goroutine has looked at the config yet.
	r.cfg.OnExit = func(string) { m.forget(name, r) }
	m.rooms[name] = r
	m.cfg.Room.Metrics.RoomsStarted.Inc()
	m.cfg.Room.Metrics.RoomsResident.Set(float64(len(m.rooms)))
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		r.Run(m.ctx)
	}()
	return r, nil, nil
}

// resident answers from the rooms already open, reporting whether it could.
func (m *Manager) resident(name string, want store.UUID) (*Room, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[name]
	if !ok {
		return nil, false, nil
	}
	if r.owner != want {
		return nil, true, fmt.Errorf("%w: %s", ErrWrongOwner, name)
	}
	m.touch(name)
	return r, true, nil
}

// settleOwner asks the database who owns this document, and refuses the caller
// if it is not them.
//
// The row is created if it is not there and its owner is read back, in one
// transaction, so two connections racing to open a new document both learn the
// same answer rather than each inserting their own.
//
// Without a store there is nothing durable to consult and the caller's word
// stands. That is not a hole: the room keeps what it was told, and every later
// connection is compared against it, so a server with no database still refuses
// a second tenant a document the first one opened. It just forgets when the
// room does, which is what a server with no database does about everything.
func (m *Manager) settleOwner(name string, want store.UUID) error {
	p := m.cfg.Room.Store
	if p == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(m.ctx, settleTimeout)
	defer cancel()
	found, _, err := p.Ensure(ctx, store.DocumentID(name), name, want)
	if err != nil {
		return err
	}
	if found != want {
		return fmt.Errorf("%w: %s", ErrWrongOwner, name)
	}
	return nil
}

// settleTimeout bounds the one query a join makes. A client is waiting on it
// with an open socket, so it is short: a database that cannot answer this in ten
// seconds is a database that is down, and the connection should be told so
// rather than held.
const settleTimeout = 10 * time.Second

// byLeastRecentlyUsed lists the resident rooms, oldest use first. Called with
// the lock held. Ordering is by touch counter, not by wall time, so two
// joins in the same tick still have a deterministic LRU order.
func (m *Manager) byLeastRecentlyUsed() []*Room {
	names := make([]string, 0, len(m.rooms))
	for n := range m.rooms {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool { return m.used[names[i]] < m.used[names[j]] })
	rooms := make([]*Room, 0, len(names))
	for _, n := range names {
		rooms = append(rooms, m.rooms[n])
	}
	return rooms
}

// touch records that a room was just used. Called with the lock held.
// clock is monotonic and never wraps in practice; used is compared only
// for ordering, not for absolute time.
func (m *Manager) touch(name string) {
	m.clock++
	m.used[name] = m.clock
}

// evictOne stops the first idle room in the list, reporting whether any went.
func (m *Manager) evictOne(candidates []*Room) bool {
	m.mu.Lock()
	reason := m.pressure
	m.mu.Unlock()
	if reason == "" {
		reason = "cap"
	}
	for _, r := range candidates {
		if r.EvictIfIdle() {
			<-r.Done()
			m.cfg.Room.Metrics.RoomsEvicted.WithLabelValues(reason).Inc()
			return true
		}
	}
	return false
}

// forget is called by a room's own goroutine as it stops.
func (m *Manager) forget(name string, r *Room) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rooms[name] == r {
		delete(m.rooms, name)
		delete(m.used, name)
		m.cfg.Room.Metrics.RoomsResident.Set(float64(len(m.rooms)))
	}
}

// Evict stops the named room if it is resident and nobody is connected to it,
// and reports whether the document is now out of memory.
//
// A room with connections is left alone and reports false: the caller wants the
// document gone, and disconnecting people who are editing to achieve that is a
// decision for whoever asked, not for the registry. Not being resident at all
// counts as success.
func (m *Manager) Evict(name string) bool {
	m.mu.Lock()
	r, ok := m.rooms[name]
	m.mu.Unlock()
	if !ok {
		return true
	}
	if !r.EvictIfIdle() {
		return false
	}
	// The room writes itself out as it stops, so waiting here means a caller
	// that deletes afterwards is not racing a snapshot.
	<-r.Done()
	return true
}

// Stats returns the counters shared by every room this manager owns.
func (m *Manager) Stats() *Stats { return m.cfg.Room.Stats }

// NodeID reports the id this process publishes under.
func (m *Manager) NodeID() uint64 { return m.cfg.Room.NodeID }

// Len reports how many rooms are resident.
func (m *Manager) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}

// Wait blocks until every room goroutine has returned. Callers cancel the
// context first; this is the drain half of a graceful shutdown.
//
// Marking the manager closed under mu before reading wg is what makes a join
// racing shutdown safe: tryGet increments wg under the same mu and only when
// closed is false, so either the increment is ordered before this read or it
// never happens.
func (m *Manager) Wait() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.wg.Wait()
}
