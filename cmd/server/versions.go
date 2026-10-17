package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/mesutokul/ycollab/internal/room"
	"github.com/mesutokul/ycollab/internal/store"
)

// Version history is the answer to the question the rest of the API cannot
// answer: not "what does this document say" but "what did it say before
// somebody pasted over it". Three endpoints, all on the admin listener beside
// the others:
//
//	GET  /documents/{name}/versions        the list, newest first, no payloads
//	GET  /documents/{name}/versions/{id}   one version, as a Yjs update
//	POST /documents/{name}/versions        take one now
//
// Restoring is deliberately not a fourth endpoint. It is DELETE followed by
// POST /documents/{name} with the version's bytes, because those two steps are
// what restoring actually is here: a merge cannot remove what the document has
// since gained ([D89]), so "restore" without the delete would hand back the
// union of the old version and the damage. Two visible steps beat one endpoint
// whose name promises more than it does.

// maxVersionList caps a listing. A history is browsed, not exported.
const maxVersionList = 200

// listVersions serves the history of one document.
func listVersions(documents *store.Store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			http.Error(w, "no document name", http.StatusBadRequest)
			return
		}
		limit := maxVersionList
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				http.Error(w, "limit must be a positive number", http.StatusBadRequest)
				return
			}
			limit = min(n, maxVersionList)
		}

		ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
		defer cancel()
		versions, err := documents.ListVersions(ctx, store.DocumentID(name), limit)
		if err != nil {
			log.Error("could not list versions", "document", name, "err", err)
			http.Error(w, "could not read the history", http.StatusInternalServerError)
			return
		}

		body := versionList{Document: name, Versions: []versionView{}}
		for _, v := range versions {
			body.Versions = append(body.Versions, versionView{
				ID:          v.ID,
				CreatedAt:   v.CreatedAt.UTC().Format(time.RFC3339Nano),
				StateVector: base64.StdEncoding.EncodeToString(v.StateVector),
				Label:       v.Label,
				Bytes:       v.Bytes,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(body); err != nil {
			log.Debug("could not write the history", "document", name, "err", err)
		}
	}
}

// getVersion serves one version's bytes, in the same form as the document read
// API so the same code opens both.
func getVersion(documents *store.Store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if name == "" || err != nil {
			http.Error(w, "the version id must be a number", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
		defer cancel()
		v, err := documents.LoadVersion(ctx, store.DocumentID(name), id)
		switch {
		case errors.Is(err, store.ErrNoVersion):
			// The document name is part of the lookup, so this is also the
			// answer for a version that exists under a different document.
			http.Error(w, "no such version", http.StatusNotFound)
			return
		case err != nil:
			log.Error("could not read a version", "document", name, "version", id, "err", err)
			http.Error(w, "could not read the version", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(headerStateVector, base64.StdEncoding.EncodeToString(v.StateVector))
		w.Header().Set("Cache-Control", "no-store")
		// A version never changes once written, so its ETag is stable and a
		// caller that already has it can skip the download.
		etag := `"v` + strconv.FormatInt(v.ID, 10) + `"`
		w.Header().Set("ETag", etag)
		if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if _, err := w.Write(v.Payload); err != nil {
			log.Debug("could not write a version", "document", name, "err", err)
		}
	}
}

// takeVersion records a version now.
//
// It goes through the room when one is resident, so the version is the document
// the connected clients are looking at rather than what the database last heard
// about. With no room, the stored document is versioned directly - which is the
// same thing, because with nobody editing there is nothing newer.
func takeVersion(manager *room.Manager, documents *store.Store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			http.Error(w, "no document name", http.StatusBadRequest)
			return
		}
		label := r.URL.Query().Get("label")
		if len(label) > 200 {
			http.Error(w, "the label is too long", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), readTimeout)
		defer cancel()

		var (
			written bool
			err     error
		)
		if resident := manager.Resident(name); resident != nil {
			written, err = resident.TakeVersion(label)
			if errors.Is(err, room.ErrClosed) {
				// Evicted while we were asking; it wrote its final snapshot on
				// the way out, so the stored document is what it had.
				written, err = versionStored(ctx, documents, name, label)
			}
		} else {
			written, err = versionStored(ctx, documents, name, label)
		}
		switch {
		case errors.Is(err, room.ErrNoDocument):
			// Not the same thing as "no history is kept", and saying so sent an
			// operator looking at the wrong thing: a server with versioning
			// working perfectly answered 503 "this server keeps no version
			// history" for a name that simply does not exist.
			http.Error(w, "no such document", http.StatusNotFound)
			return
		case errors.Is(err, room.ErrNoVersioning):
			http.Error(w, "this server keeps no version history", http.StatusServiceUnavailable)
			return
		case err != nil:
			log.Error("could not take a version", "document", name, "err", err)
			http.Error(w, "could not take a version", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		// 201 when a row was written, 200 when the document is unchanged since
		// the last version and the store declined to store it again. Both are
		// success; the difference is whether anything new exists.
		if written {
			w.WriteHeader(http.StatusCreated)
		}
		log.Info("version requested", "document", name, "label", label, "written", written)
		_ = json.NewEncoder(w).Encode(map[string]any{"document": name, "written": written})
	}
}

// versionStored versions the document as the database has it.
func versionStored(ctx context.Context, documents *store.Store, name, label string) (bool, error) {
	if documents == nil {
		return false, room.ErrNoVersioning
	}
	id := store.DocumentID(name)
	snapshot, err := room.Fetch(ctx, documents, name, nil)
	if err != nil {
		return false, err
	}
	// minAge zero: somebody asked by hand. The state-vector check still
	// applies, so asking twice for an unchanged document stores one version.
	return documents.SaveVersion(ctx, id, snapshot.Owner, snapshot.Incarnation, store.Version{
		StateVector: snapshot.StateVector,
		Payload:     snapshot.Update,
		Label:       label,
	}, 0)
}

type versionList struct {
	Document string        `json:"document"`
	Versions []versionView `json:"versions"`
}

type versionView struct {
	ID          int64  `json:"id"`
	CreatedAt   string `json:"created_at"`
	StateVector string `json:"state_vector"`
	Label       string `json:"label,omitempty"`
	Bytes       int    `json:"bytes"`
}
