package room

import (
	"context"
	"errors"
	"fmt"

	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/store"
)

// Reading a document over HTTP has two sources, and which one is right depends
// on whether anybody is editing it:
//
//   - A resident room is asked, through its mailbox like everything else. Its
//     copy is what the connected clients are looking at, and it is ahead of the
//     database by up to one flush interval.
//   - A document with no room is read from the database directly, without
//     starting one. Waking a room for a read would hold the document in memory,
//     join it to the cluster and start it emitting hooks, all as a side effect
//     of somebody looking.
//
// What this cannot do is see an unflushed edit on another replica. A read is
// therefore as fresh as the other replicas' flush interval, which is 200 ms by
// default. Making it exact would mean a bus round trip per read, on the chance
// that some other node holds the document - a large hammer for a read API.

// ErrNoDocument means the name has never been written and no room holds it.
var ErrNoDocument = errors.New("room: no such document")

// A Snapshot is one reading of a document.
type Snapshot struct {
	// Update is the document as a Yjs update: everything when the request
	// carried no state vector, or the difference from it when it did.
	Update []byte
	// StateVector is what the document knows, whatever Update contains. It is
	// sync metadata; HTTP ETags validate the selected response representation.
	StateVector []byte
	// Clients is how many connections this node has on the document, and -1
	// when it was read from the database rather than from a room.
	Clients int
	// Resident says where this came from.
	Resident bool
	// Skipped counts stored updates that would not apply. It is almost always
	// zero; anything else means this reading of the document is missing rows,
	// and a backup taken from it is missing them too. The room says so in its
	// log when it loads a document, and this is how the read path says the same
	// thing rather than handing back quietly incomplete bytes.
	Skipped int
	// These identify the coherent stored read for guarded administrative
	// writes. They are internal metadata, not part of the HTTP representation.
	Owner       store.UUID
	Incarnation int64
}

// readCmd asks the room for a snapshot. Like every other command it goes
// through the mailbox, so the reader never touches the document concurrently
// with the goroutine that owns it.
type readCmd struct {
	sv    []byte
	reply chan readResult
}

type readResult struct {
	snapshot Snapshot
	err      error
}

func (readCmd) isCommand() {}

// Read returns the document as the room currently holds it. sv may be nil for
// everything, or an encoded state vector for the difference from it.
func (r *Room) Read(sv []byte) (Snapshot, error) {
	reply := make(chan readResult, 1)
	if err := r.send(readCmd{sv: sv, reply: reply}); err != nil {
		return Snapshot{}, err
	}
	select {
	case res := <-reply:
		return res.snapshot, res.err
	case <-r.done:
		// The room stopped after accepting the command. The caller retries
		// against the database, which by then has the room's final snapshot.
		return Snapshot{}, ErrClosed
	}
}

// read runs on the room goroutine.
func (r *Room) read(c readCmd) {
	snapshot, err := encodeSnapshot(r.doc, c.sv)
	snapshot.Clients = len(r.conns)
	snapshot.Resident = true
	c.reply <- readResult{snapshot: snapshot, err: err}
}

// encodeSnapshot is the shared half of both paths, so a document read from the
// database and one read from a room are encoded by the same code.
func encodeSnapshot(doc *crdt.Doc, sv []byte) (Snapshot, error) {
	var (
		update []byte
		err    error
	)
	if len(sv) == 0 {
		update, err = doc.EncodeStateAsUpdate(nil)
	} else {
		update, err = doc.EncodeDiff(sv)
	}
	if err != nil {
		return Snapshot{}, err
	}
	ours, err := doc.EncodeStateVector()
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Update: update, StateVector: ours, Clients: -1}, nil
}

// Fetch reads a document straight from the store, without starting a room.
//
// A name that was never written is ErrNoDocument rather than an empty document:
// a caller asking for a document that does not exist wants to hear so, and a
// connecting client would create it empty anyway.
func Fetch(ctx context.Context, p Persistence, name string, sv []byte) (Snapshot, error) {
	if p == nil {
		return Snapshot{}, ErrNoDocument
	}
	// LoadAny, not Load: this is the administrative read, which is above the
	// tenancy boundary and must not create a row for a name that has none.
	stored, err := p.LoadAny(ctx, store.DocumentID(name))
	if err != nil {
		return Snapshot{}, err
	}
	if stored.Snapshot == nil && len(stored.Updates) == 0 {
		return Snapshot{}, ErrNoDocument
	}
	doc := crdt.NewDoc(0)
	if stored.Snapshot != nil {
		if err := doc.ApplyUpdate(stored.Snapshot); err != nil {
			return Snapshot{}, fmt.Errorf("snapshot would not apply: %w", err)
		}
	}
	skipped := 0
	for _, u := range stored.Updates {
		// Same argument as load: one unusable row must not make the document
		// unreadable, and the room would skip it too. Unlike load, there is no
		// logger here - so the count travels back with the snapshot and the
		// caller says something about it.
		if err := doc.ApplyUpdate(u); err != nil {
			skipped++
		}
	}
	snapshot, err := encodeSnapshot(doc, sv)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.Skipped = skipped
	snapshot.Owner = stored.Owner
	snapshot.Incarnation = stored.Incarnation
	return snapshot, nil
}

// Resident returns the room holding a document, or nil if none does. It does
// not create one.
// OwnedBy reports whether this room's document belongs to the named tenant.
//
// Read without synchronisation on purpose: the owner is settled before the room
// is constructed and never changes afterwards - the only way to move a document
// is to evict it first - so there is nothing to race with.
func (r *Room) OwnedBy(owner string) bool { return r.owner == store.OwnerID(owner) }

func (m *Manager) Resident(name string) *Room {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rooms[name]
}
