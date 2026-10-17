// Package testutil contains helpers used only by integration tests.
package testutil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync/atomic"
	"time"
)

// CommitReplyProxy forwards PostgreSQL's wire protocol unchanged except that
// it closes one client connection after PostgreSQL sends CommandComplete(COMMIT).
type CommitReplyProxy struct {
	listener net.Listener
	target   string
	url      string
	armed    atomic.Bool
	dropped  atomic.Bool
	closed   chan struct{}
}

func NewCommitReplyProxy(dsn string) (*CommitReplyProxy, error) {
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
	proxy := &CommitReplyProxy{
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

func (p *CommitReplyProxy) DSN() string { return p.url }
func (p *CommitReplyProxy) Arm()        { p.armed.Store(true) }
func (p *CommitReplyProxy) DroppedCommitReply() bool {
	return p.dropped.Load()
}

func (p *CommitReplyProxy) Close() {
	_ = p.listener.Close()
	<-p.closed
}

func (p *CommitReplyProxy) accept() {
	defer close(p.closed)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.serve(client)
	}
}

func (p *CommitReplyProxy) serve(client net.Conn) {
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
