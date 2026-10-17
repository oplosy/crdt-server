// Package room owns documents. One goroutine per document, and that goroutine
// is the only thing that touches the document's CRDT state, so there is no
// locking anywhere in this package's hot path - which is exactly the promise
// internal/crdt's Doc was written against.
//
// The room speaks the y-websocket message flow described in
// y-protocols/sync.js:23-28: the client opens with SyncStep1, the server
// answers with SyncStep2 and then its own SyncStep1, and everything after that
// is Update messages and awareness.
package room

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mesutokul/ycollab/internal/cluster"
	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/hook"
	"github.com/mesutokul/ycollab/internal/metrics"
	"github.com/mesutokul/ycollab/internal/protocol"
	"github.com/mesutokul/ycollab/internal/store"
)

// ErrClosed means the room stopped between the moment a caller looked it up and
// the moment it tried to hand over a connection. The manager retries with a
// fresh room; see registry.go.
var ErrClosed = errors.New("room: closed")

// ErrNoVersioning means the server is not keeping history, which is what a
// server without a database is doing.
var ErrNoVersioning = errors.New("room: no version history is kept")

// Defaults chosen in the brief.
const (
	// DefaultMailbox bounds how many commands can be waiting for the room
	// goroutine. Producers block when it is full, which pushes back on the
	// connections that are shouting, not on the document.
	DefaultMailbox = 256
	// DefaultTick is how often the room checks awareness timeouts and its own
	// idleness. Well below the 30 s awareness timeout so a ghost cursor is
	// never more than a tick late.
	DefaultTick = 5 * time.Second
	// DefaultIdleTimeout is how long an empty room stays resident.
	DefaultIdleTimeout = 5 * time.Minute
)

// Config configures one room.
type Config struct {
	Name string

	// Owner is the tenant this room's document belongs to, as the manager
	// settled it against the database. Empty means the document is owned by
	// nobody, which is what a server without multi-tenancy writes.
	//
	// It is on the room rather than looked up per connection so that the second
	// and every later connection to a resident document is answered from
	// memory: the boundary is checked once, when the document is opened.
	Owner string

	// UsageInterval is how often the room re-measures its document. Zero uses
	// DefaultUsageInterval.
	UsageInterval time.Duration

	// ServerClientID is the client id the room's Doc is created with. The
	// server never authors content, so this only matters if a later phase makes
	// it edit documents itself.
	ServerClientID crdt.ClientID

	Mailbox      int
	Tick         time.Duration
	IdleTimeout  time.Duration
	AwarenessTTL time.Duration
	// AwarenessLimits bound how much cursor state a room will hold. The zero
	// value applies the defaults.
	AwarenessLimits protocol.Limits

	// Store persists the document. Nil keeps everything in memory, which is
	// what the room's own unit tests want and what the server does when it is
	// started without a database.
	Store Persistence
	// CompactAfter is how many updates accumulate before the log is folded into
	// a new snapshot.
	CompactAfter int
	// FlushInterval bounds how long an update sits in memory before it is
	// written.
	FlushInterval time.Duration

	// Versions keeps the document's history. Nil means no history is kept, and
	// then nothing is encoded for it.
	Versions Versioning
	// VersionInterval is how often a changed document has a version taken.
	// Zero or less means only the ones somebody asks for by hand.
	VersionInterval time.Duration
	// VersionKeep is how many versions a document holds; older ones are
	// deleted after a write. Zero applies the default, negative keeps
	// everything - which is unbounded storage, since each version is a whole
	// document.
	VersionKeep int

	// Bus relays this document's traffic to the other replicas holding it. Nil
	// makes the room a single-node room, which is what Phase 2 and 3 were and
	// what the room's own unit tests mostly still are.
	Bus cluster.Bus
	// NodeID identifies this process on the bus. Every room on a node shares it,
	// because it is what tells our own envelopes apart from everyone else's.
	NodeID uint64
	// AntiEntropy is how often the room announces its state vector on the bus.
	AntiEntropy time.Duration
	// Stats counts cluster traffic, shared across the node's rooms.
	Stats *Stats

	// Hooks receives document events, coalesced to at most one of each per
	// tick. Nil means nothing is emitted and, more to the point, nothing is
	// encoded for it - so a server without hooks pays nothing for them.
	Hooks hook.Sink
	// HookState makes every event carry the whole document as a Yjs update.
	// Off by default: it is the difference between a notification and a copy of
	// the document on every tick of every changed room.
	HookState bool

	// Metrics is where this room reports what it does. Nil becomes a set
	// registered nowhere.
	Metrics *metrics.Metrics

	Logger *slog.Logger

	// Now is the clock, injectable so tests do not have to sleep.
	Now func() time.Time

	// OnEvict runs when an idle room is about to disappear, before it stops and
	// before its final snapshot is written. Persistence does not go through it -
	// the room writes its own snapshot - so this is a hook for observers.
	OnEvict func(name string, doc *crdt.Doc)

	// OnExit removes the room from whatever registry owns it. It must have
	// completed before the room accepts no further joins; the manager relies on
	// that ordering to avoid losing a connection to a room that is going away.
	OnExit func(name string)
}

func (c *Config) setDefaults() {
	if c.Mailbox <= 0 {
		c.Mailbox = DefaultMailbox
	}
	if c.Tick <= 0 {
		c.Tick = DefaultTick
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	if c.AwarenessTTL <= 0 {
		c.AwarenessTTL = protocol.DefaultTimeout
	}
	if c.CompactAfter <= 0 {
		c.CompactAfter = DefaultCompactAfter
	}
	if c.VersionKeep == 0 {
		c.VersionKeep = DefaultVersionKeep
	}
	// A negative interval means durable writes, so only zero - the unset value -
	// takes the default.
	if c.FlushInterval == 0 {
		c.FlushInterval = DefaultFlushInterval
	}
	if c.AntiEntropy <= 0 {
		c.AntiEntropy = DefaultAntiEntropy
	}
	if c.Stats == nil {
		c.Stats = &Stats{}
	}
	if c.Metrics == nil {
		c.Metrics = metrics.Nop()
	}
	if c.Bus != nil && c.NodeID == 0 {
		// A node id of zero would filter nothing, and worse, would agree with
		// every other misconfigured node. Generating one is safe: its only job is
		// to be unique.
		c.NodeID = cluster.NewNodeID()
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// A Room is one document and the connections currently editing it.
type Room struct {
	cfg Config
	log *slog.Logger

	doc   *crdt.Doc
	aw    *protocol.Awareness
	docID store.UUID
	owner store.UUID
	// incarnation is fixed at load; snapshotGeneration advances only when this
	// room's writer confirms a compaction.
	incarnation        int64
	snapshotGeneration int64
	// usage is what the document costs, measured on the tick and published for
	// the manager's memory budget. See memory.go.
	usage usage

	// jobs carries work to the persist goroutine; nil when there is no store.
	// Only the room goroutine touches it.
	jobs        chan persistJob
	persistDone chan struct{}
	// sinceSnapshot counts updates queued since the last compaction (room
	// goroutine); folded holds the log rows the next snapshot will cover
	// (persist goroutine).
	sinceSnapshot int
	folded        []int64

	// stats is cfg.Stats, hoisted because every counter touch reads it; metrics
	// is cfg.Metrics for the same reason.
	stats   *Stats
	metrics *metrics.Metrics

	// hooks is cfg.Hooks, nil when nothing is listening; changes counts local
	// edits since the last event (room goroutine); storedRows counts rows
	// written since the last one (persist goroutine, hence atomic). See hooks.go.
	hooks      hook.Sink
	changes    uint64
	storedRows atomic.Uint64

	// versions is cfg.Versions, nil when no history is kept; versionDirty says
	// whether anything has changed since the last attempt, and lastVersion when
	// that was. All three are the room goroutine's. See versions.go.
	versions     Versioning
	versionDirty bool
	lastVersion  time.Time

	// remote carries envelopes from the bus into the room goroutine; pub carries
	// ours out. Both are nil without a bus. Neither is ever blocked on: the room
	// must not wait for the network, and the network must not wait for the room.
	sub     cluster.Subscription
	remote  chan cluster.Envelope
	pub     chan pubJob
	pubDone chan struct{}
	// pubRunning says whether the publisher goroutine was ever started, which the
	// shutdown needs to know before it waits for it.
	pubRunning bool
	// lastAnnounce is when we last published our state vector.
	lastAnnounce time.Time

	conns map[Conn]*connState

	mailbox chan command
	done    chan struct{}

	// sendMu and closed serialise the shutdown handover; see send and stop.
	sendMu sync.Mutex
	closed bool

	// lastEmpty is when the room last had no connections, or the zero time
	// while somebody is connected.
	lastEmpty time.Time
}

// connState is what the room remembers about a connection beyond its identity.
type connState struct {
	// clients are the awareness client ids this connection has published under.
	// A connection is normally one Yjs client, but nothing in the protocol says
	// so, and a departing connection has to retract everything it announced.
	clients map[uint64]struct{}
}

// New creates a room. It does not start it; call Run.
func New(cfg Config) *Room {
	cfg.setDefaults()
	r := &Room{
		cfg:      cfg,
		log:      cfg.Logger.With("room", cfg.Name),
		doc:      crdt.NewDoc(cfg.ServerClientID),
		aw:       protocol.NewAwarenessWithLimits(cfg.AwarenessLimits),
		docID:    store.DocumentID(cfg.Name),
		owner:    store.OwnerID(cfg.Owner),
		stats:    cfg.Stats,
		metrics:  cfg.Metrics,
		hooks:    cfg.Hooks,
		versions: cfg.Versions,
		conns:    make(map[Conn]*connState),
		mailbox:  make(chan command, cfg.Mailbox),
		done:     make(chan struct{}),
	}
	if cfg.Store != nil {
		r.jobs = make(chan persistJob, persistQueue)
		r.persistDone = make(chan struct{})
	}
	if cfg.Bus != nil {
		r.remote = make(chan cluster.Envelope, remoteQueue)
		r.pub = make(chan pubJob, publishQueue)
		r.pubDone = make(chan struct{})
	}
	if cfg.Versions != nil && cfg.Store == nil {
		// Versions are written through the persist goroutine, which only exists
		// when there is a store, so this combination keeps no history at all.
		// Saying so beats a server that looks configured and records nothing.
		r.log.Error("version history is configured without a store, so no versions will be kept")
		r.versions = nil
	}
	r.lastEmpty = cfg.Now()
	return r
}

// Name reports the room's document name.
func (r *Room) Name() string { return r.cfg.Name }

// Done is closed once the room has stopped.
func (r *Room) Done() <-chan struct{} { return r.done }

type command interface{ isCommand() }

type joinCmd struct{ conn Conn }

// evictCmd asks the room to shut down if nobody is connected. The reply says
// whether it did, so the registry can move on to the next candidate instead of
// guessing.
type evictCmd struct{ evicted chan bool }

type leaveCmd struct{ conn Conn }
type frameCmd struct {
	conn  Conn
	frame []byte
}
type tickCmd struct{ now time.Time }

func (joinCmd) isCommand()  {}
func (evictCmd) isCommand() {}
func (leaveCmd) isCommand() {}
func (frameCmd) isCommand() {}
func (tickCmd) isCommand()  {}

// Join hands a connection to the room.
func (r *Room) Join(conn Conn) error { return r.send(joinCmd{conn}) }

// Leave tells the room a connection is gone. The gateway calls it exactly once
// per connection, whatever the reason for the disconnect.
func (r *Room) Leave(conn Conn) error { return r.send(leaveCmd{conn}) }

// Deliver hands one client frame to the room. frame must not be reused by the
// caller: the room keeps slices of it and broadcasts them unchanged.
func (r *Room) Deliver(conn Conn, frame []byte) error { return r.send(frameCmd{conn, frame}) }

// send blocks while the mailbox is full. That is deliberate: it stalls the
// connection that is producing faster than the document can integrate, which is
// the only party that should pay for it.
//
// sendMu is not there to protect the channel - channels do not need that - but
// to make the handover at shutdown total. A command either reaches the room, or
// is drained by stop, or is refused with ErrClosed; it is never accepted and
// then dropped on the floor. See stop for the other half of the argument.
func (r *Room) send(c command) error {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	if r.closed {
		return ErrClosed
	}
	select {
	case r.mailbox <- c:
		return nil
	case <-r.done:
		// The room stopped while we were waiting for mailbox space.
		return ErrClosed
	}
}

// Run is the room's goroutine. It returns when the room evicts itself or ctx is
// cancelled.
func (r *Room) Run(ctx context.Context) {
	// The bus comes first, so nothing published while we are loading is lost.
	if err := r.startFanout(ctx); err != nil {
		// A room that cannot reach the bus would serve its clients a document
		// that silently ignores everyone on the other replicas. Refusing is the
		// honest answer; the client reconnects, probably to another replica.
		r.log.Error("could not join the cluster", "err", err)
		// The persist goroutine was never started, so there is nothing for the
		// shutdown to wait on.
		r.abandonPersisting()
		r.stop(CloseInternalError, "could not join the cluster")
		return
	}
	if r.cfg.Store != nil {
		if err := r.load(ctx); err != nil {
			// Serving an empty document under a name that has content would let
			// clients merge their state into a blank one and write it back. A
			// closed connection is recoverable; that is not.
			r.log.Error("could not load document", "err", err)
			r.abandonPersisting()
			r.stop(CloseInternalError, "could not load document")
			return
		}
		jobs := r.jobs
		go r.persist(jobs)
	}

	ticker := time.NewTicker(r.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.stop(CloseGoingAway, "server shutting down")
			return
		case c := <-r.mailbox:
			if evicted := r.handle(c); evicted {
				return
			}
		case env := <-r.remote:
			// A nil channel blocks forever, so a room without a bus simply never
			// selects this case.
			r.remoteEnvelope(env)
		case <-ticker.C:
			if evicted := r.handle(tickCmd{r.cfg.Now()}); evicted {
				return
			}
		}
	}
}

// handle applies one command. It reports whether the room evicted itself.
//
// Tests drive this directly, so the actor's behaviour is checked without a
// goroutine, a real clock or a sleep anywhere.
func (r *Room) handle(c command) bool {
	switch v := c.(type) {
	case joinCmd:
		r.join(v.conn)
	case leaveCmd:
		r.leave(v.conn)
	case frameCmd:
		r.frame(v.conn, v.frame)
	case tickCmd:
		return r.tick(v.now)
	case readCmd:
		r.read(v)
	case mergeCmd:
		r.merge(v)
	case versionCmd:
		r.takeVersion(v)
	case evictCmd:
		if len(r.conns) > 0 {
			v.evicted <- false
			return false
		}
		r.evict()
		v.evicted <- true
		return true
	}
	return false
}

// EvictIfIdle asks the room to stop, and reports whether it did. A room with
// connections refuses: freeing memory by disconnecting people who are editing
// is not a trade the registry is allowed to make.
func (r *Room) EvictIfIdle() bool {
	reply := make(chan bool, 1)
	if err := r.send(evictCmd{reply}); err != nil {
		// Already gone, which is what the caller wanted.
		return true
	}
	select {
	case ok := <-reply:
		return ok
	case <-r.done:
		return true
	}
}

func (r *Room) join(conn Conn) {
	if _, ok := r.conns[conn]; ok {
		return
	}
	r.conns[conn] = &connState{clients: make(map[uint64]struct{})}
	r.lastEmpty = time.Time{}
	// Nothing is sent here. The protocol is client-initiated (sync.js:23-28):
	// the client opens with SyncStep1 and we answer it. Sending first would
	// work with y-websocket but would be us inventing a handshake.
}

func (r *Room) leave(conn Conn) {
	state, ok := r.conns[conn]
	if !ok {
		return
	}
	delete(r.conns, conn)
	if len(r.conns) == 0 {
		r.lastEmpty = r.cfg.Now()
	}
	// Retract whatever this connection announced, so its cursor disappears now
	// rather than after the awareness timeout.
	if len(state.clients) > 0 {
		ids := make([]uint64, 0, len(state.clients))
		for id := range state.clients {
			ids = append(ids, id)
		}
		_, payload, err := r.aw.RemoveClients(ids, r.cfg.Now())
		if err != nil {
			r.log.Error("encode awareness removal", "err", err)
		} else if payload != nil {
			r.broadcast(protocol.WriteAwareness(payload), nil)
			// Tell the other replicas too, so the cursor disappears there now
			// rather than when their own awareness timeout notices.
			r.publish(cluster.KindAwareness, payload, &r.stats.PublishedAwareness)
		}
	}
}

func (r *Room) frame(conn Conn, frame []byte) {
	if _, ok := r.conns[conn]; !ok {
		return // raced with a disconnect
	}
	msg, err := protocol.Decode(frame)
	if err != nil {
		r.log.Warn("undecodable frame", "conn", conn.ID(), "err", err)
		r.drop(conn, CloseProtocolError, "undecodable message")
		return
	}
	switch v := msg.(type) {
	case protocol.SyncStep1Message:
		r.metrics.MessagesReceived.WithLabelValues("sync_step1").Inc()
		r.syncStep1(conn, v.StateVector)
	case protocol.SyncStep2Message:
		r.metrics.MessagesReceived.WithLabelValues("sync_step2").Inc()
		r.update(conn, v.Update)
	case protocol.UpdateMessage:
		r.metrics.MessagesReceived.WithLabelValues("update").Inc()
		r.update(conn, v.Update)
	case protocol.AwarenessMessage:
		r.metrics.MessagesReceived.WithLabelValues("awareness").Inc()
		r.awareness(conn, v.Payload)
	case protocol.QueryAwarenessMessage:
		r.metrics.MessagesReceived.WithLabelValues("query_awareness").Inc()
		r.sendAll(conn)
	case protocol.PermissionDeniedMessage:
		// Server to client only. A client sending one is confused, but it is
		// harmless, so log and carry on rather than dropping a live editor.
		r.log.Warn("client sent an auth message", "conn", conn.ID(), "reason", v.Reason)
	}
}

// syncStep1 answers a client's state vector with what it is missing, then asks
// for what we are missing (sync.js:23-28).
func (r *Room) syncStep1(conn Conn, sv []byte) {
	diff, err := r.doc.EncodeDiff(sv)
	if err != nil {
		r.log.Warn("bad state vector", "conn", conn.ID(), "err", err)
		r.drop(conn, CloseProtocolError, "bad state vector")
		return
	}
	if !r.deliver(conn, protocol.WriteSyncStep2(diff)) {
		return
	}
	ours, err := r.doc.EncodeStateVector()
	if err != nil {
		r.log.Error("encode state vector", "err", err)
		return
	}
	if !r.deliver(conn, protocol.WriteSyncStep1(ours)) {
		return
	}
	// Hand over the cursors that are already in the room. Without this a
	// joining client sees nobody until they next move.
	if r.aw.Len() > 0 {
		r.sendAll(conn)
	}
}

func (r *Room) sendAll(conn Conn) {
	payload, err := r.aw.EncodeAll()
	if err != nil {
		r.log.Error("encode awareness", "err", err)
		return
	}
	r.deliver(conn, protocol.WriteAwareness(payload))
}

func (r *Room) update(conn Conn, update []byte) {
	if !conn.CanWrite() {
		// A read-only client still answers our SyncStep1, and once it has the
		// document that answer is an update carrying nothing. Treating an empty
		// update as an attempt to write would disconnect every well-behaved
		// read-only client on the second message.
		if isEmptyUpdate(update) {
			return
		}
		// Anything else is a real edit. It is refused out loud rather than
		// dropped: a client whose edits vanish silently shows its user a document
		// that will not survive a reload, which is worse than being told.
		r.metrics.Denied.WithLabelValues("read_only").Inc()
		r.log.Warn("update from a read-only connection", "conn", conn.ID())
		conn.Send(protocol.WritePermissionDenied("read-only: this token does not grant write access"))
		r.drop(conn, ClosePolicyViolation, "read-only")
		return
	}
	started := time.Now()
	r.touchDocument()
	err := r.doc.ApplyUpdate(update)
	metrics.Observe(r.metrics.ApplyDuration, started)
	r.metrics.UpdateBytes.Observe(float64(len(update)))
	if err != nil {
		r.metrics.ApplyFailed.Inc()
		r.log.Warn("bad update", "conn", conn.ID(), "err", err)
		r.drop(conn, CloseProtocolError, "bad update")
		return
	}
	r.record(update)
	r.versionDirty = true
	// Only local edits raise a change event. An update that arrived on the bus
	// was already reported by the replica whose client made it, and counting it
	// again here would turn one edit into one webhook per replica holding the
	// document.
	r.changed()
	// Relay the sender's bytes rather than re-encoding the document's delta.
	// Peers then receive exactly what the author produced, and updates for
	// structs we are still missing propagate instead of being stuck behind our
	// own integration state.
	r.broadcast(protocol.WriteUpdate(update), conn)
	// And to the clients on the other replicas. Only updates from a local
	// connection are published: an update that arrived on the bus is never sent
	// back, which is why a message cannot circulate.
	r.publish(cluster.KindUpdate, update, &r.stats.PublishedUpdate)
}

func (r *Room) awareness(conn Conn, payload []byte) {
	changed, err := r.aw.ApplyUpdate(payload, r.cfg.Now())
	if err != nil {
		// A state that is too large, or a connection inventing client ids, is a
		// policy violation rather than a malformed message: it is refused with
		// the same code backpressure and read-only writes use, and the client is
		// told which rule it broke rather than being dropped in silence.
		if errors.Is(err, protocol.ErrStateTooLarge) || errors.Is(err, protocol.ErrTooManyClients) {
			r.metrics.Denied.WithLabelValues("awareness_limit").Inc()
			r.log.Warn("awareness update refused", "conn", conn.ID(), "err", err)
			conn.Send(protocol.WritePermissionDenied(err.Error()))
			r.drop(conn, ClosePolicyViolation, "awareness limit")
			return
		}
		r.log.Warn("bad awareness update", "conn", conn.ID(), "err", err)
		r.drop(conn, CloseProtocolError, "bad awareness update")
		return
	}
	if len(changed) == 0 {
		return
	}
	if state, ok := r.conns[conn]; ok {
		for _, id := range changed {
			state.clients[id] = struct{}{}
		}
	}
	out, err := r.aw.Encode(changed)
	if err != nil {
		r.log.Error("encode awareness", "err", err)
		return
	}
	r.broadcast(protocol.WriteAwareness(out), conn)
	// The other replicas get the entries as we now hold them, not the client's
	// raw payload: what we send has passed our clock rules, so a stale
	// announcement is not forwarded to be rejected three more times.
	r.publish(cluster.KindAwareness, out, &r.stats.PublishedAwareness)
}

// tick sweeps stale awareness states and evicts the room if it has been empty
// long enough. It reports whether the room evicted itself.
func (r *Room) tick(now time.Time) bool {
	// Before the idle check, so a room that is about to evict itself has still
	// reported what it weighed - otherwise the gauge would drift down only when
	// rooms happened to be measured.
	r.measure(now)

	if _, payload, err := r.aw.Sweep(now, r.cfg.AwarenessTTL); err != nil {
		r.log.Error("sweep awareness", "err", err)
	} else if payload != nil {
		// Not published. A timeout is this replica's own conclusion from not
		// having heard anything, and the replica the client is connected to is
		// the one that knows whether it is still there. Publishing it would let a
		// node with a slow bus connection delete live cursors everywhere.
		r.broadcast(protocol.WriteAwareness(payload), nil)
	}
	r.announce(now)
	r.emitHooks(now)
	r.versionTick(now)
	if len(r.conns) > 0 || r.lastEmpty.IsZero() {
		return false
	}
	if now.Sub(r.lastEmpty) < r.cfg.IdleTimeout {
		return false
	}
	// The ordinary case: nobody has been here for -idle-timeout. Counted apart
	// from the two that mean a bound was reached, because a server evicting on
	// idleness has headroom and one evicting on a bound does not.
	r.metrics.RoomsEvicted.WithLabelValues("idle").Inc()
	r.evict()
	return true
}

// evict writes the document out and shuts the room down.
func (r *Room) evict() {
	r.log.Info("evicting room", "bytes", r.Bytes(), "structs", r.Structs())
	if r.cfg.OnEvict != nil {
		r.cfg.OnEvict(r.cfg.Name, r.doc)
	}
	r.stop(CloseGoingAway, "room evicted")
}

// broadcast sends a frame to every connection except except.
func (r *Room) broadcast(frame []byte, except Conn) {
	var slow []Conn
	for conn := range r.conns {
		if conn == except {
			continue
		}
		if conn.Send(frame) {
			r.metrics.FramesSent.Inc()
		} else {
			r.metrics.FramesDropped.Inc()
			slow = append(slow, conn)
		}
	}
	// Dropping mutates r.conns, so it happens after the iteration.
	for _, conn := range slow {
		r.dropSlow(conn)
	}
}

// deliver sends one frame to one connection, reporting whether it went out.
func (r *Room) deliver(conn Conn, frame []byte) bool {
	if conn.Send(frame) {
		r.metrics.FramesSent.Inc()
		return true
	}
	r.metrics.FramesDropped.Inc()
	r.dropSlow(conn)
	return false
}

func (r *Room) dropSlow(conn Conn) {
	r.log.Warn("dropping slow connection", "conn", conn.ID())
	r.drop(conn, ClosePolicyViolation, "outbound buffer full")
}

// drop removes a connection and closes it. The gateway will still call Leave
// when its pumps notice; leave is idempotent.
func (r *Room) drop(conn Conn, code int, reason string) {
	r.leave(conn)
	conn.Close(code, reason)
}

// stop closes every connection and shuts the room down.
//
// The order is the whole correctness argument:
//
//  1. OnExit removes the room from the registry, so no new caller can find it.
//  2. done is closed, which releases any sender blocked on a full mailbox.
//  3. sendMu is taken and closed is set, which stops senders that had already
//     got past the registry. Taking the lock has to come after step 2, or a
//     sender blocked on the mailbox while holding it would deadlock us.
//  4. Whatever is still queued is drained. Any sender that enqueued before
//     step 3 held the lock, so its command is in the mailbox by now and this
//     drain sees it. A joining connection is closed with 1001 rather than
//     silently forgotten: the client reconnects and lands in a fresh room.
func (r *Room) stop(code int, reason string) {
	// Before the connections go, so the last change event still says how many
	// people were on the document when it happened.
	r.emitHooks(r.cfg.Now())
	for conn := range r.conns {
		conn.Close(code, reason)
	}
	r.conns = make(map[Conn]*connState)
	if r.cfg.OnExit != nil {
		r.cfg.OnExit(r.cfg.Name)
	}
	// Leave the cluster before writing out: an envelope that arrives after this
	// point has nobody to apply it, and one we still owe - the retraction from
	// the last connection leaving - is drained here.
	r.stopFanout()
	// Persist before signalling that we are gone, so a caller that waits for
	// the room knows the document is on disk.
	r.finishPersisting()
	// The final flush wrote rows nobody has been told about yet. The writer has
	// stopped by now, so this is the last thing that can raise a store event
	// for this document.
	r.emitHooks(r.cfg.Now())
	close(r.done)
	r.sendMu.Lock()
	r.closed = true
	r.sendMu.Unlock()
	for {
		select {
		case c := <-r.mailbox:
			if j, ok := c.(joinCmd); ok {
				// Same code the connected clients got: a joiner that arrived
				// during the handover deserves the same explanation, not a
				// generic "going away" when the real reason was that the
				// document could not be loaded.
				j.conn.Close(code, reason)
			}
		default:
			return
		}
	}
}
