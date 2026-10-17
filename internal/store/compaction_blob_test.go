package store_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mesutokul/ycollab/internal/store"
)

// memoryBlobs makes the object-store half of compaction observable while the
// tests exercise the database half against PostgreSQL.
type memoryBlobs struct {
	mu sync.Mutex

	objects map[string][]byte
	puts    []string
	deletes []string

	putFinished chan string

	blockedGetKey string
	getStarted    chan struct{}
	releaseGet    chan struct{}
	getBlocked    bool
}

func newMemoryBlobs() *memoryBlobs {
	return &memoryBlobs{objects: make(map[string][]byte)}
}

func (m *memoryBlobs) Put(ctx context.Context, key string, body []byte) error {
	m.mu.Lock()
	m.puts = append(m.puts, key)
	m.mu.Unlock()

	m.mu.Lock()
	m.objects[key] = append([]byte(nil), body...)
	m.mu.Unlock()
	if m.putFinished != nil {
		select {
		case m.putFinished <- key:
		default:
		}
	}
	return nil
}

func (m *memoryBlobs) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	block := key == m.blockedGetKey && !m.getBlocked
	if block {
		m.getBlocked = true
	}
	m.mu.Unlock()
	if block {
		close(m.getStarted)
		select {
		case <-m.releaseGet:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	body, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("object %q not found", key)
	}
	return append([]byte(nil), body...), nil
}

func (m *memoryBlobs) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletes = append(m.deletes, key)
	delete(m.objects, key)
	return nil
}

func (m *memoryBlobs) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for key := range m.objects {
		keys = append(keys, key)
	}
	return keys
}

func (m *memoryBlobs) has(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[key]
	return ok
}

func useMemoryBlobs(t *testing.T) (*store.Store, *memoryBlobs, context.Context, store.UUID, string) {
	t.Helper()
	s := testStore(t)
	ctx := ctx(t)
	id, name := newDoc(t, s, ctx, "mem-compact")
	blobs := newMemoryBlobs()
	s.UseBlobs(blobs, nil)
	return s, blobs, ctx, id, name
}

func makeSnapshot(t *testing.T, s *store.Store, blobs *memoryBlobs, ctx context.Context, id store.UUID, snapshot []byte) (*store.Document, string) {
	t.Helper()
	seqs, err := appendCurrent(t, s, ctx, id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(ctx, id, doc.Owner, doc.Incarnation, doc.SnapshotGeneration, snapshot, seqs); err != nil {
		t.Fatalf("initial compact: %v", err)
	}
	keys := blobs.keys()
	if len(keys) != 1 {
		t.Fatalf("initial snapshot keys = %v, want one", keys)
	}
	doc, err = s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return doc, keys[0]
}

func TestSameWatermarkCompactionsKeepTheWinningSnapshotObject(t *testing.T) {
	s, blobs, c, id, name := useMemoryBlobs(t)
	seqs, err := appendCurrent(t, s, c, id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := s.LoadAny(c, id)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Compact(c, id, base.Owner, base.Incarnation, base.SnapshotGeneration, []byte("winning snapshot"), seqs); err != nil {
		t.Fatalf("winning compact: %v", err)
	}
	keys := blobs.keys()
	if len(keys) != 1 {
		t.Fatalf("winning snapshot keys = %v, want one", keys)
	}
	winnerKey := keys[0]
	if _, err := s.Compact(c, id, base.Owner, base.Incarnation, base.SnapshotGeneration, []byte("stale snapshot"), seqs); !errors.Is(err, store.ErrStaleSnapshot) {
		t.Fatalf("stale compact = %v, want ErrStaleSnapshot", err)
	}
	blobs.mu.Lock()
	puts := append([]string(nil), blobs.puts...)
	blobs.mu.Unlock()
	if len(puts) != 2 || puts[0] == puts[1] {
		t.Fatalf("same-watermark snapshot keys = %v, want two immutable keys", puts)
	}
	loserKey := puts[1]
	if blobs.has(loserKey) || !blobs.has(winnerKey) {
		t.Fatalf("objects after stale loser: loser=%v winner=%v", blobs.has(loserKey), blobs.has(winnerKey))
	}
	doc, err := s.Load(c, id, name, store.NilUUID)
	if err != nil {
		t.Fatalf("load winning snapshot: %v", err)
	}
	if !bytes.Equal(doc.Snapshot, []byte("winning snapshot")) {
		t.Fatalf("snapshot = %q, want winning snapshot", doc.Snapshot)
	}
}

func TestBlockedSnapshotReaderKeepsObjectsUntilItsTransactionEnds(t *testing.T) {
	t.Run("compaction", func(t *testing.T) {
		s, blobs, c, id, name := useMemoryBlobs(t)
		base, oldKey := makeSnapshot(t, s, blobs, c, id, []byte("old snapshot"))
		seqs, err := appendCurrent(t, s, c, id, [][]byte{{1, 0}})
		if err != nil {
			t.Fatal(err)
		}
		blobs.blockedGetKey = oldKey
		blobs.getStarted = make(chan struct{})
		blobs.releaseGet = make(chan struct{})
		blobs.putFinished = make(chan string, 1)

		readDone := make(chan error, 1)
		go func() {
			doc, err := s.Load(c, id, name, store.NilUUID)
			if err == nil && !bytes.Equal(doc.Snapshot, []byte("old snapshot")) {
				err = fmt.Errorf("read snapshot %q, want old snapshot", doc.Snapshot)
			}
			readDone <- err
		}()
		<-blobs.getStarted

		compactDone := make(chan error, 1)
		go func() {
			_, err := s.Compact(c, id, base.Owner, base.Incarnation, base.SnapshotGeneration, []byte("new snapshot"), seqs)
			compactDone <- err
		}()
		select {
		case <-blobs.putFinished:
		case <-time.After(5 * time.Second):
			close(blobs.releaseGet)
			t.Fatal("compactor did not write its candidate snapshot")
		}
		select {
		case err := <-compactDone:
			close(blobs.releaseGet)
			t.Fatalf("compaction completed while reader held the row lock: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		close(blobs.releaseGet)
		if err := <-readDone; err != nil {
			t.Fatalf("blocked old-snapshot read: %v", err)
		}
		if err := <-compactDone; err != nil {
			t.Fatalf("compact after reader: %v", err)
		}
		if blobs.has(oldKey) {
			t.Fatal("old snapshot object survived successful compaction")
		}
		current, err := s.Load(c, id, name, store.NilUUID)
		if err != nil {
			t.Fatalf("load compacted snapshot: %v", err)
		}
		if !bytes.Equal(current.Snapshot, []byte("new snapshot")) {
			t.Fatalf("load compacted snapshot = %q", current.Snapshot)
		}
	})

	t.Run("delete", func(t *testing.T) {
		s, blobs, c, id, name := useMemoryBlobs(t)
		_, oldKey := makeSnapshot(t, s, blobs, c, id, []byte("snapshot before delete"))
		blobs.blockedGetKey = oldKey
		blobs.getStarted = make(chan struct{})
		blobs.releaseGet = make(chan struct{})

		readDone := make(chan error, 1)
		go func() {
			doc, err := s.Load(c, id, name, store.NilUUID)
			if err == nil && !bytes.Equal(doc.Snapshot, []byte("snapshot before delete")) {
				err = fmt.Errorf("read snapshot %q, want pre-delete snapshot", doc.Snapshot)
			}
			readDone <- err
		}()
		<-blobs.getStarted

		deleteDone := make(chan error, 1)
		go func() {
			_, err := s.Delete(c, id)
			deleteDone <- err
		}()
		if err := waitForDocumentDeleteLock(c); err != nil {
			close(blobs.releaseGet)
			t.Fatal(err)
		}
		close(blobs.releaseGet)
		if err := <-readDone; err != nil {
			t.Fatalf("blocked old-snapshot read: %v", err)
		}
		if err := <-deleteDone; err != nil {
			t.Fatalf("delete after reader: %v", err)
		}
		if blobs.has(oldKey) {
			t.Fatal("deleted document's snapshot object survived")
		}
		doc, err := s.LoadAny(c, id)
		if err != nil {
			t.Fatalf("load deleted document: %v", err)
		}
		if doc.Exists {
			t.Fatal("deleted document still exists")
		}
	})
}

func waitForDocumentDeleteLock(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, os.Getenv(dbEnv))
	if err != nil {
		return fmt.Errorf("connect to observe delete lock: %w", err)
	}
	defer conn.Close(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		err := conn.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			 WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock'
			   AND query LIKE 'DELETE FROM documents WHERE%')`).Scan(&waiting)
		if err != nil {
			return fmt.Errorf("observe delete lock: %w", err)
		}
		if waiting {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("document delete did not wait on the reader's row lock")
}

func TestCompactionKeepsBlobWhenCommitReplyIsLost(t *testing.T) {
	dsn := os.Getenv(dbEnv)
	if dsn == "" {
		t.Skipf("%s is not set; start the PostgreSQL test database to run this", dbEnv)
	}
	proxy, err := newCommitReplyProxy(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	t.Setenv(dbEnv, proxy.dsn())

	s := testStore(t)
	blobs := newMemoryBlobs()
	s.UseBlobs(blobs, nil)
	c := ctx(t)
	id, name := newDoc(t, s, c, "commit-reply-loss")
	seqs, err := appendCurrent(t, s, c, id, [][]byte{{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := s.LoadAny(c, id)
	if err != nil {
		t.Fatal(err)
	}

	proxy.Arm()
	_, compactErr := s.Compact(c, id, base.Owner, base.Incarnation, base.SnapshotGeneration, []byte("committed snapshot"), seqs)
	if compactErr == nil {
		t.Fatal("compact succeeded despite dropping PostgreSQL's COMMIT response")
	}
	if !proxy.DroppedCommitReply() {
		t.Fatalf("compact returned %v but proxy did not observe and drop a successful COMMIT response", compactErr)
	}

	doc, err := s.Load(c, id, name, store.NilUUID)
	if err != nil {
		t.Fatalf("load after ambiguous commit: %v", err)
	}
	if !bytes.Equal(doc.Snapshot, []byte("committed snapshot")) {
		t.Fatalf("snapshot after ambiguous commit = %q", doc.Snapshot)
	}
	keys := blobs.keys()
	if len(keys) != 1 || !blobs.has(keys[0]) {
		t.Fatalf("objects after committed compaction = %v, want the referenced object retained", keys)
	}
}

// commitReplyProxy forwards PostgreSQL's wire protocol unchanged except that
// it closes one client connection after PostgreSQL sends CommandComplete(COMMIT).
// This makes the commit succeed on the server while its response is lost to the
// caller, the ambiguous outcome that must not cause the snapshot blob to be
// deleted.
type commitReplyProxy struct {
	listener net.Listener
	target   string
	url      string
	armed    atomic.Bool
	dropped  atomic.Bool
	closed   chan struct{}
}

func newCommitReplyProxy(dsn string) (*commitReplyProxy, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL test URL: %w", err)
	}
	if u.Hostname() == "" {
		return nil, errors.New("parse PostgreSQL test URL: missing host")
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	proxy := &commitReplyProxy{
		listener: listener,
		target:   net.JoinHostPort(u.Hostname(), port),
		closed:   make(chan struct{}),
	}
	u.Host = listener.Addr().String()
	query := u.Query()
	query.Set("sslmode", "disable")
	u.RawQuery = query.Encode()
	proxy.url = u.String()
	go proxy.accept()
	return proxy, nil
}

func (p *commitReplyProxy) dsn() string { return p.url }
func (p *commitReplyProxy) Arm()        { p.armed.Store(true) }
func (p *commitReplyProxy) DroppedCommitReply() bool {
	return p.dropped.Load()
}

func (p *commitReplyProxy) Close() {
	_ = p.listener.Close()
	<-p.closed
}

func (p *commitReplyProxy) accept() {
	defer close(p.closed)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.serve(client)
	}
}

func (p *commitReplyProxy) serve(client net.Conn) {
	server, err := net.DialTimeout("tcp", p.target, 3*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	defer client.Close()
	defer server.Close()

	var commitSent atomic.Bool
	go func() {
		buf := make([]byte, 32*1024)
		var tail []byte
		for {
			n, readErr := client.Read(buf)
			if n > 0 {
				if p.armed.Load() {
					scan := append(append([]byte(nil), tail...), buf[:n]...)
					if bytes.Contains(bytes.ToLower(scan), []byte("commit")) {
						commitSent.Store(true)
					}
					if len(scan) > 8 {
						tail = append(tail[:0], scan[len(scan)-8:]...)
					} else {
						tail = append(tail[:0], scan...)
					}
				}
				if err := writeAll(server, buf[:n]); err != nil {
					return
				}
			}
			if readErr != nil {
				return
			}
		}
	}()

	buf := make([]byte, 32*1024)
	var pending []byte
	for {
		n, readErr := server.Read(buf)
		if n > 0 {
			pending = append(pending, buf[:n]...)
			offset := 0
			for len(pending)-offset >= 5 {
				messageLen := int(binary.BigEndian.Uint32(pending[offset+1 : offset+5]))
				if messageLen < 4 {
					return
				}
				frameLen := messageLen + 1
				if len(pending)-offset < frameLen {
					break
				}
				frame := pending[offset : offset+frameLen]
				if commitSent.Load() && frame[0] == 'C' && bytes.Equal(frame[5:], []byte("COMMIT\x00")) && p.armed.CompareAndSwap(true, false) {
					if offset > 0 {
						_ = writeAll(client, pending[:offset])
					}
					p.dropped.Store(true)
					return
				}
				offset += frameLen
			}
			if offset > 0 {
				if err := writeAll(client, pending[:offset]); err != nil {
					return
				}
				pending = append(pending[:0], pending[offset:]...)
			}
		}
		if readErr != nil {
			return
		}
	}
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}
