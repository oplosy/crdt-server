package main_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/mesutokul/ycollab/internal/crdt"
	"github.com/mesutokul/ycollab/internal/protocol"
)

// The read API exists so a backend does not have to open a WebSocket and speak
// the sync protocol to find out what a document says. This edits a document
// over a real connection and then reads it back over HTTP, which is the whole
// promise.
func TestReadingADocumentOverHTTP(t *testing.T) {
	srv := startServer(t, buildServer(t), freePort(t), "")
	doc := fmt.Sprintf("read-%d", time.Now().UnixNano())

	c := dial(t, srv.addr, doc)
	c.sync()
	update := readFixture(t, "text-insert-single", "update-000.bin")
	c.send(protocol.WriteUpdate(update))
	// The room applies frames in order, so a round trip proves the update was
	// integrated before the read.
	c.send(protocol.WriteSyncStep1(emptyStateVector(t)))
	c.recv()

	resp, body := get(t, srv, "/documents/"+doc, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET returned %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type is %q", got)
	}
	if resp.Header.Get(headerResidentTest) != "true" {
		t.Errorf("a document being edited was not reported as resident")
	}

	// The body has to be a Yjs update a client could apply, because that is
	// what makes this useful at all.
	fromHTTP := crdt.NewDoc(1)
	if err := fromHTTP.ApplyUpdate(body); err != nil {
		t.Fatalf("the body would not apply: %v", err)
	}
	expected := crdt.NewDoc(1)
	if err := expected.ApplyUpdate(update); err != nil {
		t.Fatal(err)
	}
	if got, want := textOf(t, fromHTTP), textOf(t, expected); got != want {
		t.Fatalf("read %q, want %q", got, want)
	}

	// The ETag validates the selected response bytes; the state vector remains
	// a header on every response so a caller can request a Yjs diff.
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("no ETag")
	}
	svHeader := resp.Header.Get(headerStateVectorTest)
	if svHeader == "" {
		t.Fatal("no state vector header")
	}
	sv, err := base64.StdEncoding.DecodeString(svHeader)
	if err != nil {
		t.Fatalf("the state vector header is not base64: %v", err)
	}

	// Asking again with that ETag is the caller saying "I have this version".
	notModified, _ := get(t, srv, "/documents/"+doc, http.Header{"If-None-Match": {etag}})
	if notModified.StatusCode != http.StatusNotModified {
		t.Errorf("If-None-Match returned %d, want 304", notModified.StatusCode)
	}

	// And ?sv= asks for the difference from it, which for an unchanged
	// document is an update that carries nothing.
	diffResp, diff := get(t, srv, "/documents/"+doc+"?sv="+base64.URLEncoding.EncodeToString(sv), nil)
	if diffResp.StatusCode != http.StatusOK {
		t.Fatalf("the diff request returned %d: %s", diffResp.StatusCode, diff)
	}
	if len(diff) >= len(body) {
		t.Errorf("the diff is %d bytes and the whole document is %d", len(diff), len(body))
	}
	if err := fromHTTP.ApplyUpdate(diff); err != nil {
		t.Errorf("the diff would not apply: %v", err)
	}
}

// The JSON view is a convenience over the binary one. It cannot say what kind
// a root type is, because the v1 wire format never states it - Yjs decides when
// the client calls getText or getMap - so it offers both readings and the test
// holds it to exactly that.
func TestTheJSONViewOffersBothReadingsOfARoot(t *testing.T) {
	srv := startServer(t, buildServer(t), freePort(t), "")

	t.Run("text", func(t *testing.T) {
		doc := fmt.Sprintf("json-text-%d", time.Now().UnixNano())
		c := dial(t, srv.addr, doc)
		c.sync()
		update := readFixture(t, "text-insert-single", "update-000.bin")
		c.send(protocol.WriteUpdate(update))
		c.send(protocol.WriteSyncStep1(emptyStateVector(t)))
		c.recv()

		view := getJSON(t, srv, "/documents/"+doc+"?format=json")
		if view.Document != doc {
			t.Errorf("the view is of %q", view.Document)
		}
		if !view.Resident || view.Clients == nil || *view.Clients != 1 {
			t.Errorf("resident=%v clients=%v", view.Resident, view.Clients)
		}
		if len(view.Roots) == 0 {
			t.Fatal("no roots")
		}
		expected := crdt.NewDoc(1)
		if err := expected.ApplyUpdate(update); err != nil {
			t.Fatal(err)
		}
		var text string
		for _, root := range view.Roots {
			text += root.Text
		}
		if want := textOf(t, expected); text != want {
			t.Errorf("the roots read %q, want %q (%+v)", text, want, view.Roots)
		}
	})

	t.Run("a map", func(t *testing.T) {
		doc := fmt.Sprintf("json-map-%d", time.Now().UnixNano())
		c := dial(t, srv.addr, doc)
		c.sync()
		updates := scenarioUpdatesFor(t, "map-set-overwrite")
		for _, u := range updates {
			c.send(protocol.WriteUpdate(u))
		}
		c.send(protocol.WriteSyncStep1(emptyStateVector(t)))
		c.recv()

		view := getJSON(t, srv, "/documents/"+doc+"?format=json")
		var keys int
		for _, root := range view.Roots {
			keys += len(root.Keys)
		}
		if keys == 0 {
			t.Errorf("a document of map entries reported no keys: %+v", view.Roots)
		}
	})

	t.Run("an XML root", func(t *testing.T) {
		doc := fmt.Sprintf("json-xml-%d", time.Now().UnixNano())
		c := dial(t, srv.addr, doc)
		c.sync()
		for _, u := range scenarioUpdatesFor(t, "xml-prosemirror") {
			c.send(protocol.WriteUpdate(u))
		}
		c.send(protocol.WriteSyncStep1(emptyStateVector(t)))
		c.recv()

		// The point is that the root is named and the binary form is complete,
		// not that this view can render a ProseMirror tree.
		view := getJSON(t, srv, "/documents/"+doc+"?format=json")
		if len(view.Roots) == 0 {
			t.Fatal("no roots")
		}
		if view.Bytes == 0 {
			t.Error("the view reports a document of no bytes")
		}
	})
}

// A diff is the difference from a version this server has never seen, so it
// cannot be rendered. Saying so beats ignoring half the request.
func TestAskingForJSONAndADiffIsRefused(t *testing.T) {
	srv := startServer(t, buildServer(t), freePort(t), "")
	doc := fmt.Sprintf("json-sv-%d", time.Now().UnixNano())
	c := dial(t, srv.addr, doc)
	c.sync()

	resp, body := get(t, srv, "/documents/"+doc+"?format=json&sv=AA==", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("returned %d, want 400: %s", resp.StatusCode, body)
	}
	badSV, _ := get(t, srv, "/documents/"+doc+"?sv=not-base64!", nil)
	if badSV.StatusCode != http.StatusBadRequest {
		t.Errorf("an unparseable sv returned %d, want 400", badSV.StatusCode)
	}
}

// A name nobody has ever written is a 404, not an empty document: a caller
// asking for something that does not exist wants to hear so.
func TestReadingAMissingDocumentIsNotFound(t *testing.T) {
	srv := startServer(t, buildServer(t), freePort(t), "")
	resp, _ := get(t, srv, fmt.Sprintf("/documents/never-written-%d", time.Now().UnixNano()), nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("returned %d, want 404", resp.StatusCode)
	}
}

// A document nobody is editing is read from the database, without waking a
// room: starting one for a read would hold it in memory and join it to the
// cluster as a side effect of somebody looking.
func TestReadingADocumentNobodyIsEditing(t *testing.T) {
	dbURL := os.Getenv(dbEnv)
	if dbURL == "" {
		t.Skipf("%s is not set; start deploy/docker-compose.yml to run this", dbEnv)
	}
	srv := startServer(t, buildServer(t), freePort(t), dbURL,
		// Short enough that the room evicts itself while the test watches.
		"-idle-timeout", "300ms", "-tick", "100ms")
	doc := fmt.Sprintf("idle-%d", time.Now().UnixNano())

	c := dial(t, srv.addr, doc)
	c.sync()
	update := readFixture(t, "text-insert-single", "update-000.bin")
	c.send(protocol.WriteUpdate(update))
	c.send(protocol.WriteSyncStep1(emptyStateVector(t)))
	c.recv()
	c.disconnect()

	var body []byte
	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, got := get(t, srv, "/documents/"+doc, nil)
		if resp.StatusCode == http.StatusOK && resp.Header.Get(headerResidentTest) == "false" {
			body = got
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the room never evicted; last read was %d, resident=%q\n%s",
				resp.StatusCode, resp.Header.Get(headerResidentTest), srv.logs)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Read from the database, and still the document.
	fromHTTP := crdt.NewDoc(1)
	if err := fromHTTP.ApplyUpdate(body); err != nil {
		t.Fatalf("the body would not apply: %v", err)
	}
	expected := crdt.NewDoc(1)
	if err := expected.ApplyUpdate(update); err != nil {
		t.Fatal(err)
	}
	if got, want := textOf(t, fromHTTP), textOf(t, expected); got != want {
		t.Errorf("read %q, want %q", got, want)
	}

	// And the JSON view of a document with no room reports no client count
	// rather than zero, which would be a claim it cannot make.
	view := getJSON(t, srv, "/documents/"+doc+"?format=json")
	if view.Resident {
		t.Error("the view claims the document is resident")
	}
	if view.Clients != nil {
		t.Errorf("the view reports %d clients for a document with no room", *view.Clients)
	}
}

func emptyStateVector(t *testing.T) []byte {
	t.Helper()
	sv, err := crdt.NewDoc(1).EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	return sv
}

// scenarioUpdatesFor returns a fixture scenario's incremental updates in the
// order Yjs produced them.
func scenarioUpdatesFor(t *testing.T, name string) [][]byte {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "testdata", "fixtures", name, "update-*.bin"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("no updates for %s: %v", name, err)
	}
	sort.Strings(matches)
	updates := make([][]byte, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatal(err)
		}
		updates = append(updates, b)
	}
	return updates
}

// Header names, repeated here rather than imported, so the test fails if the
// server renames one.
const (
	headerStateVectorTest = "X-Ycollab-State-Vector"
	headerResidentTest    = "X-Ycollab-Resident"
)

// documentView mirrors the JSON body. It is declared in the test rather than
// shared with the server for the same reason: this is the contract a caller
// writes against.
type documentView struct {
	Document    string `json:"document"`
	StateVector string `json:"state_vector"`
	Resident    bool   `json:"resident"`
	Clients     *int   `json:"clients"`
	Bytes       int    `json:"bytes"`
	Roots       []struct {
		Name string   `json:"name"`
		Text string   `json:"text"`
		Keys []string `json:"keys"`
	} `json:"roots"`
}

func get(t *testing.T, s *server, path string, header http.Header) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+s.admin+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func getJSON(t *testing.T, s *server, path string) documentView {
	t.Helper()
	resp, body := get(t, s, path, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d: %s", path, resp.StatusCode, body)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type is %q", got)
	}
	var view documentView
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, body)
	}
	return view
}

// disconnect closes the WebSocket, which is what makes the room idle.
func (c *client) disconnect() {
	_ = c.ws.Close(websocket.StatusNormalClosure, "done")
}
