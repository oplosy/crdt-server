package room

import (
	"context"
	"fmt"
	"time"

	"github.com/mesutokul/ycollab/internal/cluster"
	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/protocol"
	"github.com/mesutokul/ycollab/internal/store"
)

// Merging an update into a document from outside the WebSocket is how a
// document is restored from a backup, or seeded from a template.
//
// It is a merge and not a replace, and the name says so. These are CRDT
// updates: applying one adds what it contains to what is already there. There
// is no operation that makes a document forget something, because the format
// has none - deleting is itself an update that says what was deleted. Restoring
// over a document that has moved on therefore gives the union, not the backup.
// The runbook says to delete first when that is not what you want.

// mergeCmd applies an update from outside, on the room goroutine, like
// everything else that touches the document.
type mergeCmd struct {
	update []byte
	reply  chan error
}

func (mergeCmd) isCommand() {}

// Merge applies an update to a resident room, relays it to the connected
// clients and publishes it to the other replicas.
func (r *Room) Merge(update []byte) error {
	reply := make(chan error, 1)
	if err := r.send(mergeCmd{update: update, reply: reply}); err != nil {
		return err
	}
	return waitCommandReply(reply, r.done)
}

// waitCommandReply prefers a result already buffered when room shutdown wins
// the select. If shutdown happens before the actor handles the command, there
// is no result and the caller gets ErrClosed.
func waitCommandReply(reply <-chan error, done <-chan struct{}) error {
	select {
	case err := <-reply:
		return err
	case <-done:
		select {
		case err := <-reply:
			return err
		default:
			return ErrClosed
		}
	}
}

// merge runs on the room goroutine.
func (r *Room) merge(c mergeCmd) {
	r.touchDocument()
	if err := r.doc.ApplyUpdate(c.update); err != nil {
		r.metrics.ApplyFailed.Inc()
		c.reply <- err
		return
	}
	if err := r.recordAndWait(c.update); err != nil {
		// The actor has already integrated the CRDT update. Do not publish it
		// after an unconfirmed append; the room's next load remains authoritative.
		c.reply <- fmt.Errorf("%w: %w", ErrPersistence, err)
		return
	}
	r.changed()
	r.versionDirty = true
	// The clients holding this document have to be told, or the next thing they
	// send will be built on a version of the document that no longer matches
	// the server's. Every connection gets it, because none of them sent it.
	r.broadcast(protocol.WriteUpdate(c.update), nil)
	r.publish(cluster.KindUpdate, c.update, &r.stats.PublishedUpdate)
	r.log.Info("merged an update from the admin API", "bytes", len(c.update))
	c.reply <- nil
}

// MergeConfig is what Import needs when there is no room to hand the update to.
type MergeConfig struct {
	Store Persistence
	// Bus and NodeID publish the update to the replicas that do hold the
	// document. Without them a restore is invisible to any other node until it
	// reloads the document.
	Bus    cluster.Bus
	NodeID uint64
	// Owner is the tenant a newly created document is stamped with. Empty
	// leaves it owned by nobody, which is right for a server without tenancy
	// and for a restore where the operator did not say.
	//
	// It is also checked: restoring into a document that already belongs to
	// somebody else with an Owner set is refused. A restore is a blunt
	// instrument on an operator surface, and writing one tenant's bytes into
	// another's document should take more than a typo in a URL.
	Owner string
}

// Import merges an update into a document that no room on this node holds.
//
// It writes to the log rather than replacing the snapshot, which is what makes
// it the same operation the room would have done: the next load replays it like
// any other update.
func Import(ctx context.Context, cfg MergeConfig, name string, update []byte) error {
	if cfg.Store == nil {
		return ErrNoDocument
	}
	// Decoding it first means a body that is not an update is refused rather
	// than written and then found to be unreadable at the next load.
	if err := crdt.NewDoc(0).ApplyUpdate(update); err != nil {
		return err
	}
	// The document may not exist at all: restoring into a database that has
	// never heard of this name is the disaster-recovery case, and it is the one
	// that must not need a read first.
	id := store.DocumentID(name)
	owner, incarnation, err := cfg.Store.Ensure(ctx, id, name, store.OwnerID(cfg.Owner))
	if err != nil {
		return err
	}
	if cfg.Owner != "" && owner != store.OwnerID(cfg.Owner) {
		return fmt.Errorf("%w: %s", store.ErrWrongOwner, name)
	}
	if _, err := cfg.Store.Append(ctx, id, owner, incarnation, [][]byte{update}); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	if cfg.Bus == nil {
		return nil
	}
	// A replica that has the document resident would otherwise not see this
	// until it evicted and reloaded. Publishing under this node's id is what
	// every room already does, and the replicas that hold it apply it as an
	// ordinary remote update.
	env := cluster.Envelope{Origin: cfg.NodeID, Kind: cluster.KindUpdate, HasIncarnation: true, Incarnation: incarnation, Payload: update}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cfg.Bus.Publish(ctx, name, env); err != nil {
		return fmt.Errorf("published nothing to the cluster: %w", err)
	}
	return nil
}

// IsEmptyUpdate reports whether an update carries nothing, which is what a
// well-formed update built from an unchanged document looks like.
func IsEmptyUpdate(update []byte) bool { return isEmptyUpdate(update) }

// Bus reports the bus this manager's rooms publish on, so a caller doing the
// same thing outside a room can use it.
func (m *Manager) Bus() cluster.Bus { return m.cfg.Room.Bus }
