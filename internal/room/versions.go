package room

import (
	"context"
	"errors"
	"time"

	"github.com/mesutokul/ycollab/internal/metrics"
	"github.com/mesutokul/ycollab/internal/store"
)

// A version is the document as it stood, kept so somebody can answer "what did
// this say before". The update log cannot answer it: a CRDT log records what was
// added, and compaction folds it away by design.
//
// The room's part is small. On its tick, if the interval has passed and anything
// has changed since the last attempt, it encodes the document and hands it to
// the persist goroutine. Whether a row is actually written is the store's
// decision, because that is where it can be made without a race between
// replicas - see store.SaveVersion.

// DefaultVersionKeep is how many versions a document holds when versioning is
// on and nothing says otherwise. Hourly versions for a day is the shape of the
// question people actually ask.
const DefaultVersionKeep = 24

// versionJob is one version on its way to the database.
type versionJob struct {
	payload     []byte
	stateVector []byte
	label       string
	// minAge is the interval the store enforces between versions. Zero on a
	// version somebody asked for by hand: they asked, so they get one.
	minAge time.Duration
	// done, when set, is closed once the attempt is over, and written is set
	// beforehand. Only the on-demand path waits.
	done    chan struct{}
	written *bool
	result  chan error
}

// versionTick takes a version if one is due. Called from the room goroutine.
func (r *Room) versionTick(now time.Time) {
	if r.versions == nil || r.cfg.VersionInterval <= 0 {
		return
	}
	if !r.versionDirty {
		// Nothing has changed, so the store would refuse it anyway. Skipping
		// here means an idle document costs no encode rather than a wasted one
		// every interval.
		return
	}
	if !r.lastVersion.IsZero() && now.Sub(r.lastVersion) < r.cfg.VersionInterval {
		return
	}
	r.lastVersion = now
	// Cleared optimistically: if the store refuses this one, the next edit sets
	// it again, and retrying an encode the store just rejected buys nothing.
	r.versionDirty = false
	r.queueVersion(versionJob{minAge: r.cfg.VersionInterval})
}

// TakeVersion records a version now, whatever the timer thinks, and reports
// whether a row was written. label is what a person will read in the listing.
//
// It waits for the write, because the caller is an operator or an application
// asking "save this before I do something risky" and a queued maybe is not an
// answer to that.
func (r *Room) TakeVersion(label string) (bool, error) {
	written := false
	done := make(chan struct{})
	result := make(chan error, 1)
	reply := make(chan error, 1)
	if err := r.send(versionCmd{label: label, done: done, written: &written, result: result, err: reply}); err != nil {
		return false, err
	}
	if err := waitCommandReply(reply, r.done); err != nil {
		return false, err
	}
	return waitVersion(done, r.persistDone, result, &written)
}

func waitVersion(done, persistDone <-chan struct{}, result <-chan error, written *bool) (bool, error) {
	select {
	case <-done:
		if err := <-result; err != nil {
			return false, err
		}
		return *written, nil
	case <-persistDone:
		select {
		case <-done:
			if err := <-result; err != nil {
				return false, err
			}
			return *written, nil
		default:
			return false, ErrClosed
		}
	}
}

// versionCmd asks the room to encode a version and queue it.
type versionCmd struct {
	label   string
	done    chan struct{}
	written *bool
	err     chan error
	result  chan error
}

func (versionCmd) isCommand() {}

func (r *Room) takeVersion(c versionCmd) {
	if r.versions == nil {
		c.err <- ErrNoVersioning
		return
	}
	// minAge zero: somebody asked by hand, so the interval does not apply.
	// The state-vector check in the store still does, which is what stops a
	// script from filling the history with copies of an unchanged document.
	r.versionDirty = false
	r.lastVersion = r.cfg.Now()
	r.queueVersion(versionJob{label: c.label, done: c.done, written: c.written, result: c.result})
	c.err <- nil
}

// queueVersion encodes the document and hands it over. The encode is on the
// room goroutine because the room owns the document; everything after it is on
// the writer.
func (r *Room) queueVersion(job versionJob) {
	payload, err := r.doc.EncodeStateAsUpdate(nil)
	if err != nil {
		r.log.Error("encode a version", "err", err)
		r.failVersion(job)
		return
	}
	sv, err := r.doc.EncodeStateVector()
	if err != nil {
		r.log.Error("encode a version's state vector", "err", err)
		r.failVersion(job)
		return
	}
	job.payload, job.stateVector = payload, sv
	if r.jobs == nil {
		r.failVersion(job)
		return
	}
	r.jobs <- persistJob{version: &job}
}

// failVersion releases a waiter when the version never got as far as the queue.
func (r *Room) failVersion(job versionJob) {
	if job.result != nil {
		job.result <- errors.New("room: could not encode version")
	}
	if job.done != nil {
		close(job.done)
	}
}

// saveVersion runs on the persist goroutine.
func (r *Room) saveVersion(job versionJob) {
	if job.done != nil {
		defer close(job.done)
	}
	var resultErr error
	if job.result != nil {
		defer func() { job.result <- resultErr }()
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	written, err := r.versions.SaveVersion(ctx, r.docID, r.owner, r.incarnation, store.Version{
		StateVector: job.stateVector,
		Payload:     job.payload,
		Label:       job.label,
	}, job.minAge)
	metrics.Observe(r.metrics.StoreDuration.WithLabelValues("version"), started)
	if err != nil {
		resultErr = err
		r.metrics.StoreFailed.WithLabelValues("version").Inc()
		r.log.Error("could not save a version", "err", err)
		return
	}
	if job.written != nil {
		*job.written = written
	}
	if !written {
		// Another replica got there first, or the document has not changed.
		// Both are the design working, not a failure.
		return
	}
	r.metrics.Versions.Inc()
	r.log.Info("saved a version", "bytes", len(job.payload), "label", job.label)

	// Pruning follows a write and only a write: a document nobody edits does
	// not need its history counted every interval.
	if r.cfg.VersionKeep <= 0 {
		return
	}
	removed, err := r.versions.PruneVersions(ctx, r.docID, r.owner, r.incarnation, r.cfg.VersionKeep)
	if err != nil {
		resultErr = err
		r.metrics.StoreFailed.WithLabelValues("prune_versions").Inc()
		r.log.Error("could not prune versions", "err", err)
		return
	}
	if removed > 0 {
		r.log.Info("pruned versions", "removed", removed, "keep", r.cfg.VersionKeep)
	}
}
