package main_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/crdt"
)

func TestDocumentETagTracksSelectedRepresentation(t *testing.T) {
	srv := startServer(t, buildServer(t), freePort(t), "")
	doc := fmt.Sprintf("etag-%d", time.Now().UnixNano())
	client := dial(t, srv.addr, doc)
	client.sync()

	insert := readFixture(t, "text-delete", "update-000.bin")
	delete := readFixture(t, "text-delete", "update-001.bin")
	if got := post(t, srv, "/documents/"+doc, insert); got != http.StatusNoContent {
		t.Fatalf("insert returned %d, want 204", got)
	}

	// This fixture removes a character without advancing the Yjs state vector.
	reference := crdt.NewDoc(1)
	if err := reference.ApplyUpdate(insert); err != nil {
		t.Fatal(err)
	}
	beforeSV, err := reference.EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	if err := reference.ApplyUpdate(delete); err != nil {
		t.Fatal(err)
	}
	afterSV, err := reference.EncodeStateVector()
	if err != nil {
		t.Fatal(err)
	}
	if string(beforeSV) != string(afterSV) {
		t.Fatalf("delete fixture advanced state vector: %x -> %x", beforeSV, afterSV)
	}
	if got := textOf(t, reference); got != "Hello" {
		t.Fatalf("delete fixture produced %q, want Hello", got)
	}

	beforeBinary, beforeBinaryBody := get(t, srv, "/documents/"+doc, nil)
	beforeJSON, beforeJSONBody := get(t, srv, "/documents/"+doc, http.Header{"Accept": {"application/json"}})
	svQuery := base64.URLEncoding.EncodeToString(beforeSV)
	beforeDiff, beforeDiffBody := get(t, srv, "/documents/"+doc+"?sv="+svQuery, nil)
	for name, resp := range map[string]*http.Response{"binary": beforeBinary, "JSON": beforeJSON, "diff": beforeDiff} {
		if resp.StatusCode != http.StatusOK || resp.Header.Get("ETag") == "" {
			t.Fatalf("initial %s read returned %d with ETag %q", name, resp.StatusCode, resp.Header.Get("ETag"))
		}
	}
	if got := textOfUpdate(t, beforeBinaryBody); got != "Hello World" {
		t.Fatalf("initial binary body is %q, want Hello World", got)
	}
	var beforeView documentView
	if err := json.Unmarshal(beforeJSONBody, &beforeView); err != nil {
		t.Fatalf("initial JSON body: %v", err)
	}
	if got := rootsText(beforeView); got != "Hello World" {
		t.Fatalf("initial JSON text is %q, want Hello World", got)
	}
	if string(beforeDiffBody) == string(beforeBinaryBody) {
		t.Fatal("diff request unexpectedly returned the full binary body")
	}

	if got := post(t, srv, "/documents/"+doc, delete); got != http.StatusNoContent {
		t.Fatalf("delete returned %d, want 204", got)
	}

	afterBinary, afterBinaryBody := get(t, srv, "/documents/"+doc, nil)
	afterJSON, afterJSONBody := get(t, srv, "/documents/"+doc, http.Header{"Accept": {"application/json"}})
	afterDiff, afterDiffBody := get(t, srv, "/documents/"+doc+"?sv="+svQuery, nil)
	if beforeBinary.Header.Get(headerStateVectorTest) != afterBinary.Header.Get(headerStateVectorTest) ||
		beforeJSON.Header.Get(headerStateVectorTest) != afterJSON.Header.Get(headerStateVectorTest) ||
		beforeDiff.Header.Get(headerStateVectorTest) != afterDiff.Header.Get(headerStateVectorTest) {
		t.Fatal("delete-only change altered the response state vector")
	}
	if got := textOfUpdate(t, afterBinaryBody); got != "Hello" {
		t.Fatalf("updated binary body is %q, want Hello", got)
	}
	if string(beforeBinaryBody) == string(afterBinaryBody) {
		t.Fatal("binary body did not reflect the deletion")
	}
	var afterView documentView
	if err := json.Unmarshal(afterJSONBody, &afterView); err != nil {
		t.Fatalf("updated JSON body: %v", err)
	}
	if got := rootsText(afterView); got != "Hello" {
		t.Fatalf("updated JSON text is %q, want Hello", got)
	}
	if string(beforeJSONBody) == string(afterJSONBody) {
		t.Fatal("JSON body did not reflect the deletion")
	}
	if string(beforeDiffBody) == string(afterDiffBody) {
		t.Fatal("diff body did not carry the delete set")
	}

	representations := []struct {
		name    string
		path    string
		header  http.Header
		oldETag string
		newETag string
		before  []byte
		after   []byte
	}{
		{"binary", "/documents/" + doc, nil, beforeBinary.Header.Get("ETag"), afterBinary.Header.Get("ETag"), beforeBinaryBody, afterBinaryBody},
		{"JSON Accept", "/documents/" + doc, http.Header{"Accept": {"application/json"}}, beforeJSON.Header.Get("ETag"), afterJSON.Header.Get("ETag"), beforeJSONBody, afterJSONBody},
		{"diff", "/documents/" + doc + "?sv=" + svQuery, nil, beforeDiff.Header.Get("ETag"), afterDiff.Header.Get("ETag"), beforeDiffBody, afterDiffBody},
	}
	for _, rep := range representations {
		if rep.oldETag == rep.newETag || bytes.Equal(rep.before, rep.after) {
			t.Errorf("%s ETag/body did not change with the delete set", rep.name)
		}
		changedHeaders := rep.header.Clone()
		if changedHeaders == nil {
			changedHeaders = make(http.Header)
		}
		changedHeaders.Set("If-None-Match", rep.oldETag)
		changed, body := get(t, srv, rep.path, changedHeaders)
		if changed.StatusCode != http.StatusOK || !bytes.Equal(body, rep.after) {
			t.Errorf("stale %s ETag returned %d or stale body", rep.name, changed.StatusCode)
		}
		unchangedHeaders := rep.header.Clone()
		if unchangedHeaders == nil {
			unchangedHeaders = make(http.Header)
		}
		unchangedHeaders.Set("If-None-Match", rep.newETag)
		unchanged, _ := get(t, srv, rep.path, unchangedHeaders)
		if unchanged.StatusCode != http.StatusNotModified {
			t.Errorf("current %s ETag returned %d, want 304", rep.name, unchanged.StatusCode)
		}
		if rep.header.Get("Accept") != "" {
			for _, resp := range []*http.Response{changed, unchanged} {
				if !strings.Contains(strings.ToLower(resp.Header.Get("Vary")), "accept") {
					t.Errorf("%s response has Vary %q, want Accept", rep.name, resp.Header.Get("Vary"))
				}
			}
		}
	}
	for _, cross := range []struct {
		name, path string
		tag        string
		body       []byte
	}{
		{"JSON tag on binary", "/documents/" + doc, afterJSON.Header.Get("ETag"), afterBinaryBody},
		{"full tag on diff", "/documents/" + doc + "?sv=" + svQuery, afterBinary.Header.Get("ETag"), afterDiffBody},
	} {
		resp, body := get(t, srv, cross.path, http.Header{"If-None-Match": {cross.tag}})
		if resp.StatusCode != http.StatusOK || !bytes.Equal(body, cross.body) {
			t.Errorf("%s returned %d or the wrong body", cross.name, resp.StatusCode)
		}
	}

	// format explicitly selects the representation, even if Accept disagrees.
	binaryByFormat, binaryByFormatBody := get(t, srv, "/documents/"+doc+"?format=binary", http.Header{"Accept": {"application/json"}})
	jsonByFormat, jsonByFormatBody := get(t, srv, "/documents/"+doc+"?format=json", http.Header{"Accept": {"application/octet-stream"}})
	if binaryByFormat.Header.Get("Content-Type") != "application/octet-stream" || string(binaryByFormatBody) != string(afterBinaryBody) {
		t.Error("format=binary did not override Accept: application/json")
	}
	if binaryByFormat.Header.Get("ETag") != afterBinary.Header.Get("ETag") {
		t.Error("format=binary returned a different ETag for the same binary body")
	}
	if jsonByFormat.Header.Get("Content-Type") != "application/json" || string(jsonByFormatBody) != string(afterJSONBody) {
		t.Error("format=json did not override Accept: application/octet-stream")
	}
	if jsonByFormat.Header.Get("ETag") != afterJSON.Header.Get("ETag") {
		t.Error("format=json returned a different ETag for the same JSON body")
	}

	currentETag := afterBinary.Header.Get("ETag")
	for _, match := range []string{"W/" + currentETag, `"other", ` + currentETag, "*"} {
		resp, _ := get(t, srv, "/documents/"+doc, http.Header{"If-None-Match": {match}})
		if resp.StatusCode != http.StatusNotModified {
			t.Errorf("If-None-Match %q returned %d, want 304", match, resp.StatusCode)
		}
	}
}

func TestJSONDocumentETagTracksClientCount(t *testing.T) {
	srv := startServer(t, buildServer(t), freePort(t), "")
	doc := fmt.Sprintf("etag-clients-%d", time.Now().UnixNano())
	first := dial(t, srv.addr, doc)
	first.sync()
	before, beforeBody := get(t, srv, "/documents/"+doc, http.Header{"Accept": {"application/json"}})
	var beforeView documentView
	if err := json.Unmarshal(beforeBody, &beforeView); err != nil {
		t.Fatal(err)
	}
	if beforeView.Clients == nil || *beforeView.Clients != 1 {
		t.Fatalf("initial JSON clients=%v, want 1", beforeView.Clients)
	}

	second := dial(t, srv.addr, doc)
	second.sync()
	after, afterBody := get(t, srv, "/documents/"+doc, http.Header{"Accept": {"application/json"}})
	var afterView documentView
	if err := json.Unmarshal(afterBody, &afterView); err != nil {
		t.Fatal(err)
	}
	if before.Header.Get(headerStateVectorTest) != after.Header.Get(headerStateVectorTest) {
		t.Fatal("connecting another client changed the state vector")
	}
	if afterView.Clients == nil || *afterView.Clients != 2 || string(beforeBody) == string(afterBody) {
		t.Fatalf("JSON body did not reflect client count change: before=%+v after=%+v", beforeView.Clients, afterView.Clients)
	}
	if before.Header.Get("ETag") == after.Header.Get("ETag") {
		t.Fatal("JSON ETag did not change when its body changed")
	}
	changed, body := get(t, srv, "/documents/"+doc, http.Header{
		"Accept":        {"application/json"},
		"If-None-Match": {before.Header.Get("ETag")},
	})
	if changed.StatusCode != http.StatusOK || string(body) != string(afterBody) {
		t.Fatalf("stale JSON ETag returned %d or stale body", changed.StatusCode)
	}
	unchanged, _ := get(t, srv, "/documents/"+doc, http.Header{
		"Accept":        {"application/json"},
		"If-None-Match": {after.Header.Get("ETag")},
	})
	if unchanged.StatusCode != http.StatusNotModified {
		t.Errorf("current JSON ETag returned %d, want 304", unchanged.StatusCode)
	}
	for _, resp := range []*http.Response{changed, unchanged} {
		if !strings.Contains(strings.ToLower(resp.Header.Get("Vary")), "accept") {
			t.Errorf("JSON metadata response has Vary %q, want Accept", resp.Header.Get("Vary"))
		}
	}
}

func rootsText(view documentView) string {
	var text string
	for _, root := range view.Roots {
		text += root.Text
	}
	return text
}
