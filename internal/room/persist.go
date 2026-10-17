package room

import (
	"context"
	"errors"
	"time"

	"github.com/mesutokul/ycollab/internal/metrics"
	"github.com/mesutokul/ycollab/internal/store"
)

// Persistence is the part of internal/store a room needs. It is an interface so
// the room's own tests can run without a database; the integration tests use the
// real thing.
type Persistence interface {
	// Load reads the document for a connection that claims owner, creating the
	// row if this is the first time anybody asked. It refuses with
	// store.ErrWrongOwner if the row exists and belongs to somebody else.
	Load(ctx context.Context, id store.UUID, name string, owner store.UUID) (*store.Document, error)
	// LoadAny reads without checking the owner and without creating the row.
	// It is for the administrative surface, which is above tenancy; every call
	// site is on that listener and is audited.
	LoadAny(ctx context.Context, id store.UUID) (*store.Document, error)
	// Ensure creates the document row and reports who owns it. Import needs it
	// because a restore writes to the log without reading first, and the log's
	// foreign key wants a parent; the manager needs it to settle who may open a
	// document before a room exists.
	Ensure(ctx context.Context, id store.UUID, name string, owner store.UUID) (store.UUID, error)
	Append(ctx context.Context, id store.UUID, payloads [][]byte) ([]int64, error)
	Compact(ctx context.Context, id, owner store.UUID, incarnation, generation int64, snapshot []byte, folded []int64) (int64, error)
}

// Versioning is the part of internal/store a room needs to keep history. It is
// separate from Persistence because a room can persist without versioning, and
// because every test that only cares about writes should not have to implement
// three more methods.
type Versioning interface {
	SaveVersion(ctx context.Context, id store.UUID, v store.Version, minAge time.Duration) (bool, error)
	PruneVersions(ctx context.Context, id store.UUID, keep int) (int64, error)
}

const (
	// DefaultCompactAfter is the brief's threshold: fold the log into a new
	// snapshot once this many updates have accumulated.
	DefaultCompactAfter = 500
	// DefaultFlushInterval bounds how long an update can sit in memory before
	// it is written. It also bounds what a crash can lose - see the note on
	// durability in persist.
	DefaultFlushInterval = 200 * time.Millisecond
	// persistQueue is how many updates can be waiting to be written.
	persistQueue = 4096
	// maxBatch caps one INSERT.
	maxBatch = 256
	// persistTimeout bounds a single database call.
	persistTimeout = 30 * time.Second
)

type persistJob struct {
	// payload is an update to append, or nil for a compaction request.
	payload []byte
	// snapshot is the full document state to write, when this is a compaction.
	snapshot []byte
	// version, when set, is a version to record. It goes through this queue
	// like everything else so the room goroutine never waits for a database
	// call, and so writes for one document stay in order.
	version *versionJob
	// done, when set, is closed once this job has been written. It is what
	// makes durable writes durable: the room waits on it before relaying the
	// update, so a client is never told about an edit that is not on disk.
	done chan struct{}
}

// load restores the document from the database: the snapshot, then every log
// row that is still there.
func (r *Room) load(ctx context.Context) error {
	started := time.Now()
	doc, err := r.cfg.Store.Load(ctx, r.docID, r.cfg.Name, r.owner)
	if err != nil {
		r.metrics.StoreFailed.WithLabelValues("load").Inc()
		return err
	}
	r.incarnation = doc.Incarnation
	r.snapshotGeneration = doc.SnapshotGeneration
	defer func() { metrics.Observe(r.metrics.LoadDuration, started) }()
	metrics.Observe(r.metrics.StoreDuration.WithLabelValues("load"), started)
	if doc.Snapshot != nil {
		if err := r.doc.ApplyUpdate(doc.Snapshot); err != nil {
			return err
		}
	}
	for _, u := range doc.Updates {
		if err := r.doc.ApplyUpdate(u); err != nil {
			// One corrupt row must not make the document unopenable: the rest
			// of the log still applies, and the clients still hold their own
			// copies. Losing an update is bad; refusing to serve the document
			// at all is worse.
			r.log.Error("skipping an update that would not apply", "err", err)
		}
	}
	r.sinceSnapshot = len(doc.Updates)
	// The rows just folded into the document are the first thing a new snapshot
	// would cover, so the writer starts holding them. Without this a room that
	// loads and then evicts would write a snapshot that deletes nothing, and
	// the log would never be consolidated.
	r.folded = append(r.folded, doc.Seqs...)
	// Measured here rather than waiting for the first tick: a room that is
	// loaded and never edited would otherwise weigh nothing to the budget for
	// its first interval, which is exactly when a wave of them is being opened.
	r.touchDocument()
	r.usage.measured = time.Time{}
	r.measure(r.cfg.Now())
	r.log.Info("loaded document",
		"snapshot", len(doc.Snapshot), "updates", len(doc.Updates),
		"pending", r.doc.PendingCount(), "bytes", r.Bytes(), "structs", r.Structs())
	return nil
}

// record queues an update to be written.
//
// It blocks when the queue is full, which stalls the room, which stalls the
// connections feeding it. That is the honest behaviour: the alternative is
// dropping updates that clients believe are saved. In practice the queue only
// fills if the database has been unreachable for a while, and at that point
// slowing down is the correct response.
func (r *Room) record(update []byte) {
	if r.jobs == nil {
		return
	}
	// The room broadcasts this slice too, and the writer keeps it until the
	// batch is flushed, so it is copied rather than shared.
	payload := make([]byte, len(update))
	copy(payload, update)

	if r.cfg.FlushInterval < 0 {
		// Durable writes: hand the update over and wait for it to be written
		// before returning, so it is on disk before anybody is told about it.
		// This blocks the room, which is the whole cost of the option - every
		// keystroke now pays a database round trip.
		done := make(chan struct{})
		r.jobs <- persistJob{payload: payload, done: done}
		<-done
	} else {
		r.jobs <- persistJob{payload: payload}
	}

	r.sinceSnapshot++
	if r.sinceSnapshot >= r.cfg.CompactAfter {
		r.requestCompaction()
	}
}

// requestCompaction encodes the document and hands it to the writer.
//
// The room does the encoding because it owns the document; the writer names the
// rows, because it is the only thing that knows which of them have actually
// been written. The snapshot can legitimately contain more than those rows - the
// extra updates stay in the log and are applied again on the next load, which
// costs nothing.
func (r *Room) requestCompaction() {
	if r.jobs == nil {
		return
	}
	snapshot, err := r.doc.EncodeStateAsUpdate(nil)
	if err != nil {
		r.log.Error("encode snapshot", "err", err)
		return
	}
	r.jobs <- persistJob{snapshot: snapshot}
	r.sinceSnapshot = 0
}

// persist is the room's database goroutine. It is the only thing that talks to
// the store, so writes for one document stay in order. The channel is passed in
// rather than read from the room, which lets the room drop its reference when
// it closes it.
//
// It deliberately does not take the server's context: on shutdown it has to
// finish flushing what it already accepted, and a cancelled context would turn
// a clean stop into the data loss the stop was trying to avoid. Each call is
// bounded by its own timeout instead.
func (r *Room) persist(jobs <-chan persistJob) {
	defer close(r.persistDone)

	var batch [][]byte
	// waiters are the durable-write jobs in the current batch; each is released
	// once the batch is on disk, whether the write succeeded or not - a caller
	// that waited forever on a database that is down would take the document
	// with it, and the error is already logged loudly.
	var waiters []chan struct{}
	flush := func() {
		defer func() {
			for _, done := range waiters {
				close(done)
			}
			waiters = waiters[:0]
		}()
		if len(batch) == 0 {
			return
		}
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
		seqs, err := r.cfg.Store.Append(ctx, r.docID, batch)
		cancel()
		metrics.Observe(r.metrics.StoreDuration.WithLabelValues("append"), started)
		if err != nil {
			r.metrics.StoreFailed.WithLabelValues("append").Inc()
			// The updates stay in memory and in every connected client. Say so
			// loudly rather than pretending the write happened.
			r.log.Error("could not write updates", "count", len(batch), "err", err)
		} else {
			r.folded = append(r.folded, seqs...)
			// The store event is raised here rather than in the room goroutine
			// because this is the only place that knows the write actually
			// happened. It is a counter the room drains on its next tick, so a
			// document being written every 200 ms still produces one event a
			// tick rather than five a second.
			r.stored(len(batch))
		}
		batch = batch[:0]
	}

	// In durable-write mode every job flushes itself, so the timer only exists
	// to keep the loop's shape; it is set long enough never to matter.
	interval := r.cfg.FlushInterval
	if interval < 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				flush()
				return
			}
			if job.version != nil {
				// Before the flush, deliberately: a version is a statement
				// about the document as the room had it when it asked, and
				// the payload already in hand says exactly that.
				r.saveVersion(*job.version)
				continue
			}
			if job.snapshot != nil {
				// Everything queued before the snapshot request has to be on
				// disk first, or the watermark would cover rows that are not
				// there yet.
				flush()
				r.compact(job.snapshot)
				continue
			}
			batch = append(batch, job.payload)
			if job.done != nil {
				waiters = append(waiters, job.done)
			}
			// A batch is flushed when it is full, or immediately when somebody is
			// waiting for it: durable writes are a promise about this update, not
			// about the next one.
			if len(batch) >= maxBatch || len(waiters) > 0 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (r *Room) compact(snapshot []byte) {
	if len(r.folded) == 0 {
		return
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	generation, err := r.cfg.Store.Compact(ctx, r.docID, r.owner, r.incarnation, r.snapshotGeneration, snapshot, r.folded)
	metrics.Observe(r.metrics.StoreDuration.WithLabelValues("compact"), started)
	switch {
	case err == nil:
		r.snapshotGeneration = generation
		r.metrics.Compactions.Inc()
		r.log.Info("compacted", "rows", len(r.folded), "snapshot", len(snapshot))
		// Those rows are gone; a later snapshot must not try to delete them
		// again, and must not claim to cover them.
		r.folded = r.folded[:0]
	case errors.Is(err, store.ErrStaleSnapshot):
		// Another replica got there first with a newer snapshot, which contains
		// everything ours does. Our rows are covered by theirs, so drop them.
		r.log.Info("skipped compaction, a newer snapshot exists")
		r.folded = r.folded[:0]
	default:
		// Keep the rows: the next compaction should still cover them.
		r.metrics.StoreFailed.WithLabelValues("compact").Inc()
		r.log.Error("could not compact", "err", err)
	}
}

// abandonPersisting gives up on writing anything, for the case where the room
// fails before its writer goroutine ever started. Without it the shutdown would
// wait for a goroutine that does not exist.
func (r *Room) abandonPersisting() {
	if r.jobs == nil {
		return
	}
	r.jobs = nil
	close(r.persistDone)
}

// finishPersisting flushes what is queued and waits for the writer to stop.
// Called from the room goroutine as it shuts down, which is the persist half of
// persist-on-evict.
func (r *Room) finishPersisting() {
	if r.jobs == nil {
		return
	}
	// A final snapshot means the next load is one row read rather than a replay
	// of the whole session.
	r.requestCompaction()
	close(r.jobs)
	r.jobs = nil
	<-r.persistDone
}
