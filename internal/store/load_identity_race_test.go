package store_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mesutokul/ycollab/internal/store"
)

func TestLoadCannotCrossOwnerChangeDuringIdentityCheck(t *testing.T) {
	dsn := os.Getenv(dbEnv)
	if dsn == "" {
		t.Skipf("%s is not set; start the PostgreSQL test database to run this", dbEnv)
	}
	databaseURL, err := url.Parse(dsn)
	if err != nil || databaseURL.Scheme == "" {
		t.Fatalf("parse PostgreSQL URL: %v", err)
	}
	appName := fmt.Sprintf("ycollab-load-race-%d", time.Now().UnixNano())
	query := databaseURL.Query()
	query.Set("application_name", appName)
	databaseURL.RawQuery = query.Encode()
	t.Setenv(dbEnv, databaseURL.String())

	s, c := openStore(t)
	observer, err := pgx.Connect(c, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close(context.Background()) })

	name := fmt.Sprintf("load-owner-race-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	ownerA := store.OwnerID("owner-a-" + name)
	ownerB := store.OwnerID("owner-b-" + name)
	if _, err := s.Load(c, id, name, ownerA); err != nil {
		t.Fatalf("create document: %v", err)
	}

	blocker, err := pgx.Connect(c, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Close(context.Background()) })
	tx, err := blocker.Begin(c)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			_ = tx.Rollback(context.Background())
		}
	})
	var lockedID store.UUID
	if err := tx.QueryRow(c, `SELECT id FROM documents WHERE id = $1 FOR NO KEY UPDATE`, id).Scan(&lockedID); err != nil {
		t.Fatalf("lock document identity: %v", err)
	}
	var blockerPID int32
	if err := tx.QueryRow(c, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}

	loadDone := make(chan error, 1)
	go func() {
		_, err := s.Load(c, id, name, ownerA)
		loadDone <- err
	}()
	if err := waitForDocumentLockWait(c, observer, appName, blockerPID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(c, `UPDATE documents SET owner_id = $2, incarnation = nextval('document_incarnation_seq') WHERE id = $1`, id, ownerB); err != nil {
		t.Fatalf("transfer document owner: %v", err)
	}
	if err := tx.Commit(c); err != nil {
		t.Fatalf("commit owner transfer: %v", err)
	}
	released = true

	select {
	case err := <-loadDone:
		if !errors.Is(err, store.ErrWrongOwner) {
			t.Fatalf("Load after owner transfer returned %v, want ErrWrongOwner", err)
		}
	case <-c.Done():
		t.Fatalf("Load did not finish after owner transfer: %v", c.Err())
	}
}

func waitForDocumentLockWait(ctx context.Context, observer *pgx.Conn, appName string, blockerPID int32) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		err := observer.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity a
			 WHERE a.application_name = $1 AND a.wait_event_type = 'Lock'
			   AND a.query ILIKE '%documents%'
			   AND $2 = ANY(pg_blocking_pids(a.pid)))`, appName, blockerPID).Scan(&waiting)
		if err != nil {
			return fmt.Errorf("observe Load waiting on document lock: %w", err)
		}
		if waiting {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("Load did not wait on the held document identity lock")
}
