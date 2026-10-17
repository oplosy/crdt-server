package store_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mesutokul/ycollab/internal/store"
)

func TestIdentityLockCoversChildWrites(t *testing.T) {
	dsn := os.Getenv(dbEnv)
	if dsn == "" {
		t.Skipf("%s is not set; start the PostgreSQL test database to run this", dbEnv)
	}
	databaseURL, err := url.Parse(dsn)
	if err != nil || databaseURL.Scheme == "" {
		t.Fatalf("parse PostgreSQL URL: %v", err)
	}
	appName := fmt.Sprintf("ycollab-lock-%d", time.Now().UnixNano())
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

	type childWrite struct {
		name, table, query string
		seed               func(context.Context, store.UUID, store.UUID, int64) error
		write              func(context.Context, store.UUID, store.UUID, int64) error
		verify             func(context.Context, store.UUID) error
	}
	operations := []childWrite{
		{
			name: "append", table: "doc_updates", query: "INSERT INTO doc_updates%",
			write: func(c context.Context, id store.UUID, owner store.UUID, incarnation int64) error {
				_, err := s.Append(c, id, owner, incarnation, [][]byte{[]byte("append-lock-test")})
				return err
			},
			verify: func(c context.Context, id store.UUID) error {
				doc, err := s.LoadAny(c, id)
				if err != nil {
					return err
				}
				if len(doc.Updates) != 1 || !bytes.Equal(doc.Updates[0], []byte("append-lock-test")) {
					return fmt.Errorf("append result: updates=%q", doc.Updates)
				}
				return nil
			},
		},
		{
			name: "save-version", table: "doc_versions", query: "INSERT INTO doc_versions%",
			write: func(c context.Context, id store.UUID, owner store.UUID, incarnation int64) error {
				written, err := s.SaveVersion(c, id, owner, incarnation, store.Version{
					StateVector: []byte{1}, Payload: []byte("save-version-lock-test"),
				}, 0)
				if err == nil && !written {
					return fmt.Errorf("save version did not write a row")
				}
				return err
			},
			verify: func(c context.Context, id store.UUID) error {
				count, err := s.VersionCount(c, id)
				if err == nil && count != 1 {
					return fmt.Errorf("version count=%d, want 1", count)
				}
				return err
			},
		},
		{
			name: "prune-versions", table: "doc_versions", query: "DELETE FROM doc_versions%",
			seed: func(c context.Context, id store.UUID, owner store.UUID, incarnation int64) error {
				for i := byte(1); i <= 2; i++ {
					written, err := s.SaveVersion(c, id, owner, incarnation, store.Version{
						StateVector: []byte{i}, Payload: []byte{i},
					}, 0)
					if err != nil {
						return err
					}
					if !written {
						return fmt.Errorf("seed version %d was not written", i)
					}
				}
				return nil
			},
			write: func(c context.Context, id store.UUID, owner store.UUID, incarnation int64) error {
				removed, err := s.PruneVersions(c, id, owner, incarnation, 1)
				if err == nil && removed != 1 {
					return fmt.Errorf("pruned %d versions, want 1", removed)
				}
				return err
			},
			verify: func(c context.Context, id store.UUID) error {
				count, err := s.VersionCount(c, id)
				if err == nil && count != 1 {
					return fmt.Errorf("version count=%d, want 1", count)
				}
				return err
			},
		},
	}
	transitions := []struct {
		name, query string
		deleteDoc   bool
	}{
		{name: "set-owner", query: "UPDATE documents%"},
		{name: "delete", query: "DELETE FROM documents%", deleteDoc: true},
	}

	for _, operation := range operations {
		for _, transition := range transitions {
			if transition.deleteDoc && operation.table != "doc_updates" {
				continue
			}
			t.Run(operation.name+"/"+transition.name, func(t *testing.T) {
				c := ctx(t)
				name := fmt.Sprintf("identity-lock-%s-%d", t.Name(), time.Now().UnixNano())
				id := store.DocumentID(name)
				oldOwner := store.OwnerID("before-" + name)
				newOwner := store.OwnerID("after-" + name)
				if _, err := s.Load(c, id, name, oldOwner); err != nil {
					t.Fatal(err)
				}
				owner, incarnation := currentIdentity(t, s, c, id)
				if operation.seed != nil {
					if err := operation.seed(c, id, owner, incarnation); err != nil {
						t.Fatalf("seed child rows: %v", err)
					}
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
				if _, err := tx.Exec(c, "LOCK TABLE "+operation.table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
					t.Fatalf("lock child table: %v", err)
				}

				writeDone := make(chan error, 1)
				go func() { writeDone <- operation.write(c, id, owner, incarnation) }()
				writePID, err := waitForChildTableWait(c, observer, appName, operation.table, operation.query)
				if err != nil {
					t.Fatal(err)
				}

				transitionDone := make(chan error, 1)
				go func() {
					var changed bool
					var err error
					if transition.deleteDoc {
						changed, err = s.Delete(c, id)
					} else {
						changed, err = s.SetOwner(c, id, newOwner)
					}
					if err == nil && !changed {
						err = fmt.Errorf("identity transition changed no row")
					}
					transitionDone <- err
				}()
				if err := waitForParentRowWait(c, observer, appName, transition.query, writePID); err != nil {
					t.Fatal(err)
				}

				if err := tx.Rollback(c); err != nil {
					t.Fatalf("release child table: %v", err)
				}
				released = true
				select {
				case err := <-writeDone:
					if err != nil {
						t.Fatalf("child write after blocker release: %v", err)
					}
				case <-c.Done():
					t.Fatalf("child write did not finish: %v", c.Err())
				}
				select {
				case err := <-transitionDone:
					if err != nil {
						t.Fatalf("identity transition: %v", err)
					}
				case <-c.Done():
					t.Fatalf("identity transition did not finish: %v", c.Err())
				}
				if transition.deleteDoc {
					doc, err := s.LoadAny(c, id)
					if err != nil || doc.Exists {
						t.Fatalf("deleted document: doc=%+v err=%v", doc, err)
					}
				} else {
					currentOwner, newIncarnation := currentIdentity(t, s, c, id)
					if currentOwner != newOwner || newIncarnation == incarnation {
						t.Fatalf("identity after owner change: owner=%s incarnation=%d", currentOwner, newIncarnation)
					}
					if err := operation.verify(c, id); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
func waitForChildTableWait(ctx context.Context, observer *pgx.Conn, appName, table, query string) (int32, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var pid int32
		err := observer.QueryRow(ctx, `SELECT a.pid
			FROM pg_stat_activity a
			JOIN pg_locks l ON l.pid = a.pid AND NOT l.granted
			JOIN pg_class c ON c.oid = l.relation
			WHERE a.application_name = $1 AND a.wait_event_type = 'Lock'
			  AND c.relname = $2 AND a.query LIKE $3
			LIMIT 1`, appName, table, query).Scan(&pid)
		if err == nil {
			return pid, nil
		}
		if err != pgx.ErrNoRows {
			return 0, fmt.Errorf("observe child-table wait: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("store operation never waited on %s for query %q", table, query)
}

func waitForParentRowWait(ctx context.Context, observer *pgx.Conn, appName, query string, childPID int32) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		err := observer.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity a
			 WHERE a.application_name = $1 AND a.query LIKE $2
			   AND a.wait_event_type = 'Lock'
			   AND $3 = ANY(pg_blocking_pids(a.pid)))`, appName, query, childPID).Scan(&waiting)
		if err != nil {
			return fmt.Errorf("observe parent-row wait: %w", err)
		}
		if waiting {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("identity transition did not wait behind child-writing transaction")
}
