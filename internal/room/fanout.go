package room

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/mesutokul/ycollab/internal/cluster"
	"github.com/mesutokul/ycollab/internal/protocol"
)

const (
	// DefaultAntiEntropy is how often a room with connections announces its state
	// vector on the bus. Redis Pub/Sub is at-most-once, so a dropped message
	// would otherwise be a permanent divergence between two replicas that both
	// believe they are up to date; this bounds how long that can last. Fifteen
	// seconds is small next to how long a person tolerates a stale document and
	// large next to the cost, which is one short message per room per replica.
	DefaultAntiEntropy = 15 * time.Second
	// remoteQueue is how many envelopes can wait for the room goroutine.
	remoteQueue = 1024
	// publishQueue is how many envelopes can wait to go out on the bus.
	publishQueue = 1024
	// busTimeout bounds one publish.
	busTimeout = 10 * time.Second
)

// emptyUpdate is what a v1 update with no structs and no deletions encodes to:
// a zero client count followed by an empty delete set. EncodeDiff returns it
// when the peer is not missing anything, and publishing that would be pure
// noise. TestEmptyDiffIsRecognised pins the bytes.
var emptyUpdate = []byte{0, 0}

func isEmptyUpdate(update []byte) bool {
	return len(update) == 0 || bytes.Equal(update, emptyUpdate)
}

// pubJob is one envelope waiting to go out, together with the counter that
// describes it. The kind alone is not enough: an update we relay for a client
// and a diff we compute in answer to a state vector are the same kind of
// envelope but very different facts about the node.
type pubJob struct {
	env     cluster.Envelope
	counter *atomic.Uint64
}

// startFanout subscribes to the document's channel and starts the publisher.
//
// It runs before the document is loaded, deliberately: envelopes that arrive
// during the load queue up in r.remote and are applied afterwards. Subscribing
// after the load would silently lose everything published while we were reading
// from Postgres, which is the window a restarting replica spends.
func (r *Room) startFanout(ctx context.Context) error {
	if err := r.joinBus(ctx); err != nil {
		return err
	}
	if r.pub == nil {
		return nil
	}
	pub := r.pub
	r.pubRunning = true
	go r.publishLoop(pub)
	return nil
}

// joinBus subscribes to the document's channel. It is separate from starting the
// publisher goroutine so the fanout tests can drive both directions
// synchronously, the way the rest of this package's tests drive the actor.
func (r *Room) joinBus(ctx context.Context) error {
	if r.cfg.Bus == nil {
		return nil
	}
	sub, err := r.cfg.Bus.Subscribe(ctx, r.cfg.Name, func(env cluster.Envelope) {
		r.stats.Received.Add(1)
		// Our own message, delivered back to us because Pub/Sub has no notion of
		// a publisher. Dropping it here is the whole of the loop prevention: an
		// update that came from the bus is never republished, so a message
		// crosses the cluster exactly once.
		if env.Origin == r.cfg.NodeID {
			r.stats.SelfFiltered.Add(1)
			return
		}
		select {
		case r.remote <- env:
		default:
			// The bus must not block on one busy document. Losing an envelope is
			// recoverable - the next anti-entropy round repairs it - whereas
			// stalling the bus would stall every other room on the node.
			r.stats.RemoteDropped.Add(1)
		}
	})
	if err != nil {
		return err
	}
	r.sub = sub
	return nil
}

// publish queues an envelope. It never blocks the room goroutine: the bus is
// the network, and a document must not wait for it.
func (r *Room) publish(kind cluster.Kind, payload []byte, counter *atomic.Uint64) {
	if r.pub == nil {
		return
	}
	job := pubJob{
		env:     cluster.Envelope{Origin: r.cfg.NodeID, Kind: kind, HasIncarnation: r.hasIncarnation, Incarnation: r.incarnation, Payload: payload},
		counter: counter,
	}
	select {
	case r.pub <- job:
	default:
		r.stats.PublishDropped.Add(1)
	}
}

// publishLoop is the room's bus goroutine. Like the persist goroutine it does
// not take the server's context: an envelope that has been accepted is sent,
// and each call is bounded by its own timeout instead.
func (r *Room) publishLoop(pub <-chan pubJob) {
	defer close(r.pubDone)
	for job := range pub {
		ctx, cancel := context.WithTimeout(context.Background(), busTimeout)
		err := r.cfg.Bus.Publish(ctx, r.cfg.Name, job.env)
		cancel()
		if err != nil {
			r.stats.PublishFailed.Add(1)
			r.log.Error("could not publish to the cluster", "kind", job.env.Kind, "err", err)
			continue
		}
		job.counter.Add(1)
	}
}

// remoteEnvelope applies one envelope from another replica.
func (r *Room) remoteEnvelope(env cluster.Envelope) {
	if env.HasIncarnation != r.hasIncarnation || (r.hasIncarnation && env.Incarnation != r.incarnation) {
		r.stats.RemoteRejected.Add(1)
		return
	}
	switch env.Kind {
	case cluster.KindUpdate:
		r.remoteUpdate(env.Payload)
	case cluster.KindAwareness:
		r.remoteAwareness(env.Payload)
	case cluster.KindStateVector:
		r.answerStateVector(env.Payload)
	}
}

// remoteUpdate integrates an update another replica's client authored and hands
// it to every local client.
//
// It is not written to the database. Every update is persisted by the replica
// whose client produced it, so recording it here would put a second copy of the
// same bytes in the log for no gain. The compaction that folds the log is safe
// either way, because it deletes the rows it actually wrote rather than a range
// (see DECISIONS C6) - which is the part of Phase 3 that only starts to matter
// here.
func (r *Room) remoteUpdate(update []byte) {
	r.touchDocument()
	if err := r.doc.ApplyUpdate(update); err != nil {
		// There is no connection to blame and nothing to disconnect. Say so and
		// carry on: the document is still consistent, and the next anti-entropy
		// round will offer the update again.
		r.stats.RemoteRejected.Add(1)
		r.log.Warn("remote update would not apply", "err", err)
		return
	}
	r.stats.RemoteUpdateApplied.Add(1)
	// The document changed, so a version taken here would say something new.
	// This does not duplicate the origin replica's version: the store's age
	// gate lets one through per interval whoever offers it, and this replica
	// may be the only one still holding the document by the time it is due.
	r.versionDirty = true
	// Relay the author's bytes, exactly as the local path does, so a client on
	// this replica receives what a client on the origin replica received.
	r.broadcast(protocol.WriteUpdate(update), nil)
}

func (r *Room) remoteAwareness(payload []byte) {
	changed, err := r.aw.ApplyUpdate(payload, r.cfg.Now())
	if err != nil {
		r.stats.RemoteRejected.Add(1)
		r.log.Warn("remote awareness update would not apply", "err", err)
		return
	}
	if len(changed) == 0 {
		return
	}
	r.stats.RemoteAwarenessApplied.Add(1)
	// These client ids belong to connections on another replica, so no local
	// connState claims them: when that replica's client leaves, that replica
	// publishes the retraction, and if it disappears without doing so the
	// awareness timeout removes the cursor here too.
	out, err := r.aw.Encode(changed)
	if err != nil {
		r.log.Error("encode awareness", "err", err)
		return
	}
	r.broadcast(protocol.WriteAwareness(out), nil)
}

// answerStateVector replies to another replica's announcement with whatever it
// is missing. This is the same exchange the client handshake performs, run
// between replicas on a timer, and it is what makes an at-most-once bus safe to
// build on.
func (r *Room) answerStateVector(sv []byte) {
	diff, err := r.doc.EncodeDiff(sv)
	if err != nil {
		r.stats.RemoteRejected.Add(1)
		r.log.Warn("bad state vector on the bus", "err", err)
		return
	}
	if isEmptyUpdate(diff) {
		return
	}
	r.stats.AnsweredStateVector.Add(1)
	r.publish(cluster.KindUpdate, diff, &r.stats.PublishedDiff)
}

// announce publishes our state vector, so a replica that is behind - because a
// message was lost, or because it was restarting - finds out and asks for the
// difference.
//
// Only rooms with connections announce. An empty room has nobody to keep in
// sync and is about to be evicted anyway, and a cluster where every replica
// announces every document it has ever held is a cluster that gets slower the
// longer it runs.
func (r *Room) announce(now time.Time) {
	if r.pub == nil || len(r.conns) == 0 {
		return
	}
	if now.Sub(r.lastAnnounce) < r.cfg.AntiEntropy {
		return
	}
	sv, err := r.doc.EncodeStateVector()
	if err != nil {
		r.log.Error("encode state vector", "err", err)
		return
	}
	r.lastAnnounce = now
	r.publish(cluster.KindStateVector, sv, &r.stats.PublishedStateVector)
}

// stopFanout leaves the channel and drains what we have already accepted.
//
// Unsubscribing first means no envelope arrives after we have stopped being able
// to act on it; draining afterwards means an awareness retraction queued by the
// last connection leaving still reaches the other replicas.
func (r *Room) stopFanout() {
	if r.sub != nil {
		if err := r.sub.Close(); err != nil {
			r.log.Warn("could not unsubscribe", "err", err)
		}
		r.sub = nil
	}
	if r.pub == nil {
		return
	}
	close(r.pub)
	r.pub = nil
	// A room that failed before its publisher started - because it could not join
	// the bus at all - has nothing to wait for, and waiting would hang the
	// shutdown it is part of.
	if r.pubRunning {
		<-r.pubDone
		r.pubRunning = false
	}
}
