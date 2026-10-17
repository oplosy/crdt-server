package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/store"
)

func currentIdentity(t testing.TB, s *store.Store, ctx context.Context, id store.UUID) (store.UUID, int64) {
	t.Helper()
	doc, err := s.LoadAny(ctx, id)
	if err != nil || !doc.Exists {
		t.Fatalf("load identity: doc=%+v err=%v", doc, err)
	}
	return doc.Owner, doc.Incarnation
}

func appendCurrent(t testing.TB, s *store.Store, ctx context.Context, id store.UUID, payloads [][]byte) ([]int64, error) {
	t.Helper()
	owner, incarnation := currentIdentity(t, s, ctx, id)
	seqs, err := s.Append(ctx, id, owner, incarnation, payloads)
	return seqs, err
}

func saveVersionCurrent(t testing.TB, s *store.Store, ctx context.Context, id store.UUID, v store.Version, minAge time.Duration) (bool, error) {
	t.Helper()
	owner, incarnation := currentIdentity(t, s, ctx, id)
	return s.SaveVersion(ctx, id, owner, incarnation, v, minAge)
}

func pruneVersionsCurrent(t testing.TB, s *store.Store, ctx context.Context, id store.UUID, keep int) (int64, error) {
	t.Helper()
	owner, incarnation := currentIdentity(t, s, ctx, id)
	return s.PruneVersions(ctx, id, owner, incarnation, keep)
}
