package store_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mesutokul/ycollab/internal/store"
)

// Every replica holding a document runs its own version timer, and they fire at
// the same interval boundary - so the interesting case is not "two calls" but
// "two calls at the same instant from different connections".
//
// `INSERT ... WHERE NOT EXISTS` does not survive that on its own: under READ
// COMMITTED both transactions evaluate the subquery against their own snapshot,
// neither sees the other's uncommitted row, and both insert. The guard has to
// be a lock, which is what Compact already does for the same reason.
func TestConcurrentReplicasWriteOneVersion(t *testing.T) {
	s, ctx := openStore(t)
	id := store.DocumentID(fmt.Sprintf("versions-race-%d", time.Now().UnixNano()))
	if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	// Eight callers, each with a different state vector so the state-vector
	// half of the check cannot be what saves us, all released at once.
	const replicas = 8
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	written := make([]bool, replicas)
	errs := make([]error, replicas)
	for i := range replicas {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			v := store.Version{
				StateVector: []byte{byte(i + 1)},
				Payload:     []byte{byte(i + 1)},
				Label:       fmt.Sprint(i),
			}
			written[i], errs[i] = saveVersionCurrent(t, s, context.Background(), id, v, time.Hour)
		}()
	}
	start.Done()
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("replica %d: %v", i, err)
		}
	}
	count, err := s.VersionCount(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	wrote := 0
	for _, w := range written {
		if w {
			wrote++
		}
	}
	if count != 1 || wrote != 1 {
		t.Errorf("%d replicas writing at once produced %d versions (%d reported success), want 1",
			replicas, count, wrote)
	}
}

// The test above is a smoke test: without the lock it passes most of the time,
// because the window is a statement's execution and eight goroutines rarely
// land inside it. This one is deterministic - it holds the document's advisory
// lock from outside and watches SaveVersion wait for it.
//
// It fails if the lock is ever removed, which is the point: the guard is
// invisible in normal operation and would otherwise be deleted by somebody
// tidying up a transaction that "does nothing".
func TestSaveVersionTakesTheDocumentLock(t *testing.T) {
	s, ctx := openStore(t)
	id := store.DocumentID(fmt.Sprintf("versions-lock-%d", time.Now().UnixNano()))
	if _, err := s.Load(ctx, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}

	// Hold the same lock SaveVersion takes, on a connection of the test's own
	// so nothing about the store's pool is involved.
	holder, err := pgx.Connect(ctx, os.Getenv(dbEnv))
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close(context.Background())
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`, id); err != nil {
		t.Fatalf("taking the lock: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := saveVersionCurrent(t, s, context.Background(), id,
			store.Version{StateVector: []byte{1}, Payload: []byte{1}}, 0)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("SaveVersion finished while the document was locked (err %v): it is not taking the lock", err)
	case <-time.After(750 * time.Millisecond):
		// Blocked, which is what the lock is for.
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SaveVersion after the lock was released: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SaveVersion never finished after the lock was released")
	}
}
