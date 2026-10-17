// Package hook reports what happened to a document to something outside this
// server: a backend that wants to index the text, mirror it into its own
// database, or just know that the document was touched.
//
// Everything here is an observer. A hook cannot refuse an edit, cannot delay
// one, and cannot make a room wait: a document that stops accepting keystrokes
// because somebody's webhook receiver is slow is a worse outcome than a missed
// notification, every time. Delivery is therefore best effort, and the events
// say so - they carry a timestamp and a state vector so a receiver that missed
// one can tell, and read the document back over the admin API if it cares.
package hook

import (
	"time"
)

// Kind names an event. The values are what appears in the JSON body and in the
// X-Ycollab-Event header, so they are part of the interface a receiver is
// written against.
type Kind string

const (
	// KindChange means clients connected to this node changed the document.
	// Updates that arrived from another replica do not raise it; see the note in
	// room.update.
	KindChange Kind = "document.change"
	// KindStore means updates were written to the database.
	KindStore Kind = "document.store"
)

// Kinds are every event this server emits, in the order they are documented.
var Kinds = []Kind{KindChange, KindStore}

// ParseKind turns a configured name into a Kind, reporting whether it is one.
func ParseKind(s string) (Kind, bool) {
	for _, k := range Kinds {
		if string(k) == s {
			return k, true
		}
	}
	return "", false
}

// An Event is one thing that happened to one document.
//
// The counts are per event rather than cumulative: a receiver that missed the
// previous one cannot subtract, and a room that was evicted and reloaded would
// restart a cumulative counter anyway.
type Event struct {
	// Doc is the document name, which is the URL path clients connect to.
	Doc string
	// Kind is what happened.
	Kind Kind
	// At is when the room decided to emit this, not when it was delivered.
	At time.Time
	// Node is the id this replica publishes under on the cluster bus, so a
	// receiver can tell three replicas' events apart. Zero on a single node.
	Node uint64
	// Clients is how many connections this node had on the document.
	Clients int
	// Updates is how many updates this event covers: edits since the last
	// change event, or logical updates in confirmed appends since the last store
	// event. Ambiguous commit retries can create duplicate physical rows.
	Updates uint64
	// StateVector is the document's state vector at the moment of the event,
	// which is what a receiver compares against its own copy to decide whether
	// it is behind.
	StateVector []byte
	// State is the whole document as a Yjs update, present only when the server
	// is configured to include it. It is the same bytes a client would get from
	// a sync with an empty state vector, so Y.applyUpdate reads it directly.
	State []byte
}

// A Sink receives events. Emit must not block: it is called from the room
// goroutine, which is the goroutine every connected client's traffic waits on.
type Sink interface {
	Emit(Event)
}
