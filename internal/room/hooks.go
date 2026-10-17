package room

import (
	"time"

	"github.com/mesutokul/ycollab/internal/hook"
)

// Hook events are coalesced rather than sent per update. A person typing
// produces tens of updates a second and a webhook per keystroke is not a
// feature, it is an outage at the receiving end. The room therefore records
// that something happened and emits at most one event per tick, which costs a
// bool on the hot path and nothing else - no second timer per document, and no
// work at all in a server with no hooks configured.
//
// The consequence to know about is that the delay between an edit and its
// webhook is bounded by -tick, not by a knob of its own. Lowering -tick lowers
// it; the tick's other jobs, sweeping awareness and checking idleness, do not
// mind being done more often.

// hooksEnabled reports whether anything is listening. Every call site is behind
// it, so a server without hooks does not encode a state vector on every tick.
func (r *Room) hooksEnabled() bool { return r.hooks != nil }

// changed records a local edit for the next hook event.
func (r *Room) changed() {
	if !r.hooksEnabled() {
		return
	}
	r.changes++
}

// stored records logical updates in confirmed append batches. An ambiguous
// commit followed by a retry may leave duplicate physical rows. This runs in
// the persist goroutine, so its counter is atomic.
func (r *Room) stored(rows int) {
	if !r.hooksEnabled() || rows <= 0 {
		return
	}
	r.storedRows.Add(uint64(rows))
}

// emitHooks sends whatever has accumulated. Called from the room goroutine on
// every tick and once more as the room shuts down.
func (r *Room) emitHooks(now time.Time) {
	if !r.hooksEnabled() {
		return
	}
	if r.changes > 0 {
		r.hooks.Emit(r.event(hook.KindChange, now, r.changes))
		r.changes = 0
	}
	// Swap rather than read-then-clear: the persist goroutine adds to this
	// while we are here, and losing those rows would mean an event that never
	// arrives rather than one that arrives late.
	if rows := r.storedRows.Swap(0); rows > 0 {
		r.hooks.Emit(r.event(hook.KindStore, now, rows))
	}
}

// event fills in what every event carries.
func (r *Room) event(kind hook.Kind, now time.Time, updates uint64) hook.Event {
	e := hook.Event{
		Doc:     r.cfg.Name,
		Kind:    kind,
		At:      now,
		Node:    r.cfg.NodeID,
		Clients: len(r.conns),
		Updates: updates,
	}
	sv, err := r.doc.EncodeStateVector()
	if err != nil {
		// The event is still worth sending: a receiver that only wanted to know
		// the document was touched does not need the vector.
		r.log.Error("encode state vector for a hook", "err", err)
	} else {
		e.StateVector = sv
	}
	if r.cfg.HookState {
		state, err := r.doc.EncodeStateAsUpdate(nil)
		if err != nil {
			r.log.Error("encode state for a hook", "err", err)
		} else {
			e.State = state
		}
	}
	return e
}
