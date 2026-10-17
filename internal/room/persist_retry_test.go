package room

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mesutokul/ycollab/internal/hook"
	"github.com/mesutokul/ycollab/internal/metrics"
	"github.com/mesutokul/ycollab/internal/protocol"
	"github.com/mesutokul/ycollab/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRetryableAppendError(t *testing.T) {
	for _, code := range []string{"40001", "40P01", "08000", "08001", "08003", "08006", "08007", "57P01", "57P02", "57P03", "53300"} {
		t.Run(code, func(t *testing.T) {
			if !retryableAppendError(context.Background(), &pgconn.PgError{Code: code}) {
				t.Fatalf("SQLSTATE %s was not classified as transient", code)
			}
		})
	}
	for _, code := range []string{"23505", "08004", "08P01"} {
		t.Run(code+"_terminal", func(t *testing.T) {
			if retryableAppendError(context.Background(), &pgconn.PgError{Code: code}) {
				t.Fatalf("SQLSTATE %s was classified as transient", code)
			}
		})
	}

	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF, pgconn.ErrConnClosed, temporaryNetError{}} {
		if !retryableAppendError(context.Background(), err) {
			t.Errorf("%T was not classified as transient", err)
		}
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, store.ErrStaleDocument, store.ErrWrongOwner} {
		if retryableAppendError(context.Background(), err) {
			t.Errorf("%v was classified as transient", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if retryableAppendError(ctx, &pgconn.PgError{Code: "40001"}) {
		t.Error("a canceled batch context was classified as transient")
	}
}

func TestAppendWithRetryResendsSameBatchAndReturnsConfirmedSequences(t *testing.T) {
	batch := [][]byte{{1, 2}, {3, 4}}
	want := [][]byte{{1, 2}, {3, 4}}
	calls := 0
	seqs, attempts, err := appendWithRetry(context.Background(), batch, func(_ context.Context, got [][]byte) ([]int64, error) {
		calls++
		if len(got) != len(batch) {
			t.Fatalf("attempt %d got %d payloads, want %d", calls, len(got), len(batch))
		}
		for i := range batch {
			if string(got[i]) != string(want[i]) {
				t.Fatalf("attempt %d changed payload %d", calls, i)
			}
		}
		if calls == 1 {
			return nil, &pgconn.PgError{Code: "40001"}
		}
		return []int64{7, 8}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || attempts != 2 {
		t.Fatalf("calls=%d attempts=%d, want 2 each", calls, attempts)
	}
	if len(seqs) != 2 || seqs[0] != 7 || seqs[1] != 8 {
		t.Fatalf("returned seqs %v, want [7 8]", seqs)
	}
}

func TestAppendWithRetryDoesNotRetryTerminalIdentityError(t *testing.T) {
	calls := 0
	_, attempts, err := appendWithRetry(context.Background(), [][]byte{{1}}, func(context.Context, [][]byte) ([]int64, error) {
		calls++
		return nil, store.ErrStaleDocument
	})
	if !errors.Is(err, store.ErrStaleDocument) {
		t.Fatalf("error=%v, want stale-document error", err)
	}
	if calls != 1 || attempts != 1 {
		t.Fatalf("calls=%d attempts=%d, want one terminal attempt", calls, attempts)
	}
}

func TestAppendWithRetryUsesOneSharedDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	calls := 0
	_, attempts, err := appendWithRetry(ctx, [][]byte{{1}}, func(context.Context, [][]byte) ([]int64, error) {
		calls++
		return nil, &pgconn.PgError{Code: "40001"}
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want shared deadline", err)
	}
	if calls != 1 || attempts != 1 {
		t.Fatalf("calls=%d attempts=%d, want one attempt before the shared deadline", calls, attempts)
	}
}

func TestDurableWriteRetriesTransientAppendBeforeRelay(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	sink := &recorder{}
	fake := &retryAppendStore{fakeStore: &fakeStore{}, failAttempts: 1, err: &pgconn.PgError{Code: "40001"}}
	r := runRoom(t, Config{
		Store: fake, FlushInterval: -1, Hooks: sink, Metrics: m, Tick: 5 * time.Millisecond,
	})
	author, peer := &fakeConn{id: 1}, &fakeConn{id: 2}
	if err := r.Join(author); err != nil {
		t.Fatal(err)
	}
	if err := r.Join(peer); err != nil {
		t.Fatal(err)
	}
	author.take()
	peer.take()
	update := readFixture(t, "text-insert-single", "update-000.bin")
	if err := r.Deliver(author, protocol.WriteUpdate(update)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(nil); err != nil { // mailbox barrier after relay
		t.Fatal(err)
	}
	eventually(t, "successful store hook", func() bool { return len(sink.of(hook.KindStore)) > 0 })
	assertPeerReceivedUpdate(t, peer, update)

	if got := fake.calls(); got != 2 {
		t.Fatalf("Append calls=%d, want initial attempt and one retry", got)
	}
	if got, _ := fake.fakeStore.counts(); got != 1 {
		t.Fatalf("confirmed logical updates=%d, want 1", got)
	}
	if got := testutil.ToFloat64(m.StoreFailed.WithLabelValues("append")); got != 0 {
		t.Fatalf("failed logical appends=%v, want 0 after recovery", got)
	}
	if got := sumStoreHookUpdates(sink.of(hook.KindStore)); got != 1 {
		t.Fatalf("store hook covers %d logical updates, want 1", got)
	}
	if got := appendHistogramCount(t, reg); got != 1 {
		t.Fatalf("append histogram observations=%d, want one logical batch", got)
	}
}

func TestDurableWriteRelaysAfterRetryExhaustionAndReleasesWaiter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	sink := &recorder{}
	fake := &retryAppendStore{fakeStore: &fakeStore{}, failAttempts: appendAttempts, err: &pgconn.PgError{Code: "40001"}}
	r := runRoom(t, Config{
		Store: fake, FlushInterval: -1, Hooks: sink, Metrics: m, Tick: 5 * time.Millisecond,
	})
	author, peer := &fakeConn{id: 1}, &fakeConn{id: 2}
	if err := r.Join(author); err != nil {
		t.Fatal(err)
	}
	if err := r.Join(peer); err != nil {
		t.Fatal(err)
	}
	author.take()
	peer.take()
	if err := r.Deliver(author, protocol.WriteUpdate(readFixture(t, "text-insert-single", "update-000.bin"))); err != nil {
		t.Fatalf("ordinary edit should still be accepted after storage failure: %v", err)
	}
	if _, err := r.Read(nil); err != nil { // relay must finish before the next room command
		t.Fatal(err)
	}
	assertPeerReceivedUpdate(t, peer, readFixture(t, "text-insert-single", "update-000.bin"))
	if got := fake.calls(); got != appendAttempts {
		t.Fatalf("Append calls=%d, want bounded attempts=%d", got, appendAttempts)
	}
	if got := testutil.ToFloat64(m.StoreFailed.WithLabelValues("append")); got != 1 {
		t.Fatalf("failed logical appends=%v, want 1", got)
	}
	if got := sumStoreHookUpdates(sink.of(hook.KindStore)); got != 0 {
		t.Fatalf("store hook reported %d updates after unconfirmed writes", got)
	}
	if got := appendHistogramCount(t, reg); got != 1 {
		t.Fatalf("append histogram observations=%d, want one exhausted logical batch", got)
	}

	// An administrative caller waits on the same writer path and receives the
	// final append error; it must not remain blocked after retry exhaustion.
	waiterStore := &retryAppendStore{fakeStore: &fakeStore{}, failAttempts: appendAttempts, err: &pgconn.PgError{Code: "40001"}}
	waiterRoom := New(Config{Store: waiterStore, FlushInterval: -1, Logger: quietLogger()})
	go waiterRoom.persist(waiterRoom.jobs)
	done := make(chan error, 1)
	waiterRoom.enqueue([]byte{9}, done)
	close(waiterRoom.jobs)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("durable waiter received success after all attempts failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("durable waiter was not released after bounded retries")
	}
	select {
	case <-waiterRoom.persistDone:
	case <-time.After(2 * time.Second):
		t.Fatal("persist writer did not stop after draining the failed batch")
	}
}

type retryAppendStore struct {
	*fakeStore
	mu           sync.Mutex
	appendCalls  int
	failAttempts int
	err          error
}

func (s *retryAppendStore) Append(ctx context.Context, id, owner store.UUID, incarnation int64, payloads [][]byte) ([]int64, error) {
	s.mu.Lock()
	s.appendCalls++
	call := s.appendCalls
	s.mu.Unlock()
	if call <= s.failAttempts {
		return nil, s.err
	}
	return s.fakeStore.Append(ctx, id, owner, incarnation, payloads)
}

func (s *retryAppendStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendCalls
}

func sumStoreHookUpdates(events []hook.Event) uint64 {
	var total uint64
	for _, event := range events {
		total += event.Updates
	}
	return total
}

func assertPeerReceivedUpdate(t *testing.T, peer *fakeConn, want []byte) {
	t.Helper()
	for _, frame := range peer.frames() {
		message, err := protocol.Decode(frame)
		if err != nil {
			t.Fatalf("decode peer frame: %v", err)
		}
		if update, ok := message.(protocol.UpdateMessage); ok && string(update.Update) == string(want) {
			return
		}
	}
	t.Fatal("peer did not receive the edited update")
}

func appendHistogramCount(t *testing.T, reg *prometheus.Registry) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "ycollab_store_duration_seconds" {
			continue
		}
		for _, metric := range family.Metric {
			if len(metric.Label) == 1 && metric.Label[0].GetName() == "op" && metric.Label[0].GetValue() == "append" {
				return metric.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

type temporaryNetError struct{}

func (temporaryNetError) Error() string   { return "temporary network failure" }
func (temporaryNetError) Timeout() bool   { return true }
func (temporaryNetError) Temporary() bool { return true }

var _ net.Error = temporaryNetError{}
