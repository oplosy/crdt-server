package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mesutokul/ycollab/internal/pack"
)

// Version history answers the question the rest of this store cannot: not "what
// does the document say" but "what did it say yesterday, before somebody
// pasted over it". Nothing else in the server can answer it - a CRDT update log
// is a record of what was added, and compaction folds it away by design.
//
// A version is a complete document rather than a diff from the version before
// it. A chain would be smaller, and would make reading one version a walk
// through every version before it, with no way to drop an old one - which is
// the only operation retention consists of. What keeps the size honest is that
// a version is written only when it differs from the newest one, so a document
// nobody edited produces one row however long the timer runs.

// ErrNoVersion means the version asked for is not there, or belongs to another
// document.
var ErrNoVersion = errors.New("store: no such version")

// A Version is one stored state of a document.
type Version struct {
	// ID orders versions within a document; it is what a caller asks for.
	ID int64
	// CreatedAt is when it was taken.
	CreatedAt time.Time
	// StateVector is the document's sync version at that moment. It does not
	// include deletion-only changes and is not an HTTP representation ETag.
	StateVector []byte
	// Label is empty for the versions the timer took, and whatever the caller
	// said for the ones taken by hand.
	Label string
	// Bytes is the size of Payload. On a listing, where Payload is not read,
	// it is the *stored* size, which is what the version costs and may be
	// smaller than the document it holds; on a load it is the document.
	Bytes int
	// Payload is the whole document as a Yjs update. Nil on listings: a list of
	// twenty versions of a megabyte document is not a listing.
	Payload []byte
}

// SaveVersion stores a version, unless one would say nothing new.
//
// Two conditions, both there because every replica holding a document runs its
// own timer:
//
//   - nothing is written when a version newer than minAge already exists, so
//     three replicas on the same document produce one version per interval
//     rather than three;
//   - nothing is written when the newest version has the same state vector, so
//     a document nobody is editing does not accumulate identical copies.
//
// The advisory lock is what makes those hold between replicas, and it is not
// decoration. `INSERT ... WHERE NOT EXISTS` looks atomic and is not: under READ
// COMMITTED two concurrent statements each evaluate the subquery against their
// own snapshot, neither sees the other's uncommitted row, and both insert.
// Demonstrated against this schema with two sessions, which produced two rows.
//
// The damage was not corruption but retention: duplicates eat the budget, so
// "keep the last 24" on a three-replica cluster silently becomes "keep the last
// eight moments". Compact takes the same lock, for the same reason (C2, C6).
//
// The captured owner and incarnation fence the write from a deleted/recreated
// document. It reports whether a row was written.
func (s *Store) SaveVersion(ctx context.Context, id, owner UUID, incarnation int64, v Version, minAge time.Duration) (bool, error) {
	if len(v.Payload) == 0 {
		return false, errors.New("store: a version needs a payload")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("store: save version: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	// Per document, released at commit. Two replicas versioning two different
	// documents never wait for each other.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`, id); err != nil {
		return false, fmt.Errorf("store: save version lock: %w", err)
	}
	if err := requireIdentity(ctx, tx, id, owner, incarnation); err != nil {
		return false, err
	}

	// History is the largest thing in this database - a whole document per
	// version, twenty-four per document by default - so it is the blob most
	// worth compressing.
	packed, codec := pack.Pack(v.Payload)
	s.observePack(len(v.Payload), len(packed))

	// The object first, the row second, for the reason in blobs.go. The insert
	// below may decide this version says nothing new and write no row at all, in
	// which case the object is an orphan and is deleted straight away - nothing
	// ever named it.
	key, err := s.putVersionBlob(ctx, id, packed)
	if err != nil {
		return false, err
	}
	// The stored size, taken before the column is emptied: after that nothing
	// here knows it, and length(payload) would report zero for exactly the rows
	// that cost the most.
	stored := int64(len(packed))
	if key != "" {
		// Empty rather than nil: payload is NOT NULL, and the constraint is
		// worth keeping - a version row with no payload and no key would be a
		// version that says nothing, which is not a thing this table should be
		// able to hold.
		packed = []byte{}
	}
	tag, err := tx.Exec(ctx,
		`INSERT INTO doc_versions (doc_id, state_vector, payload, label, codec, blob_key, blob_bytes)
		 SELECT $1, $2, $3, $4, $6, $7, $8
		  WHERE EXISTS (SELECT 1 FROM documents WHERE id = $1)
		    AND NOT EXISTS (
		          SELECT 1 FROM doc_versions v
		           WHERE v.doc_id = $1
		             AND (v.created_at > now() - $5::interval OR v.state_vector = $2)
		             AND v.id = (SELECT max(id) FROM doc_versions WHERE doc_id = $1))`,
		id, v.StateVector, packed, v.Label, minAge, codec, key, stored)
	if err != nil {
		s.dropBlob(ctx, key)
		return false, fmt.Errorf("store: save version: %w", err)
	}
	written := tag.RowsAffected() > 0
	if !written {
		// This version said nothing new, so no row names the object.
		s.dropBlob(ctx, key)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("store: save version: %w", err)
	}
	return written, nil
}

// ListVersions returns a document's versions, newest first, without their
// payloads. limit caps how many; zero or less means fifty.
func (s *Store) ListVersions(ctx context.Context, id UUID, limit int) ([]Version, error) {
	if limit <= 0 {
		limit = 50
	}
	// The size comes from whichever column actually holds the bytes: a row in
	// object storage has an empty payload, so length(payload) alone would report
	// every S3-backed version as costing nothing.
	rows, err := s.pool.Query(ctx,
		`SELECT id, created_at, state_vector, label,
		        CASE WHEN blob_key = '' THEN octet_length(payload) ELSE blob_bytes END
		   FROM doc_versions WHERE doc_id = $1 ORDER BY id DESC LIMIT $2`,
		id, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list versions: %w", err)
	}
	defer rows.Close()
	var out []Version
	for rows.Next() {
		var v Version
		if err := rows.Scan(&v.ID, &v.CreatedAt, &v.StateVector, &v.Label, &v.Bytes); err != nil {
			return nil, fmt.Errorf("store: scan version: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list versions: %w", err)
	}
	return out, nil
}

// LoadVersion reads one version, payload included.
//
// The document id is part of the lookup, not just the version id: a caller that
// guesses a number must not be handed somebody else's document.
func (s *Store) LoadVersion(ctx context.Context, id UUID, version int64) (*Version, error) {
	v := &Version{ID: version}
	var codec pack.Codec
	var key string
	err := s.pool.QueryRow(ctx,
		`SELECT created_at, state_vector, label, payload, codec, blob_key
		   FROM doc_versions WHERE doc_id = $1 AND id = $2`,
		id, version,
	).Scan(&v.CreatedAt, &v.StateVector, &v.Label, &v.Payload, &codec, &key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoVersion
		}
		return nil, fmt.Errorf("store: load version: %w", err)
	}
	if v.Payload, err = s.readBlob(ctx, key, v.Payload, codec); err != nil {
		return nil, fmt.Errorf("store: load version: %w", err)
	}
	v.Bytes = len(v.Payload)
	return v, nil
}

// PruneVersions keeps the newest keep versions of a document and deletes the
// rest, reporting how many went. keep of zero or less deletes nothing, because
// "keep none" is what DELETE on the document is for.
//
// Unbounded history is unbounded storage - each row is a whole document - so
// something has to say when to stop, and a count is the form an operator can
// reason about: "the last twenty-four" is a promise, "a gigabyte" is not.
func (s *Store) PruneVersions(ctx context.Context, id, owner UUID, incarnation int64, keep int) (int64, error) {
	if keep <= 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: prune versions: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`, id); err != nil {
		return 0, fmt.Errorf("store: prune versions lock: %w", err)
	}
	if err := requireIdentity(ctx, tx, id, owner, incarnation); err != nil {
		return 0, err
	}
	// RETURNING names the objects the deleted rows owned, in the same statement
	// that removes them - so the set is exactly what went, with no second query
	// that could see a different answer.
	rows, err := tx.Query(ctx,
		`DELETE FROM doc_versions
		  WHERE doc_id = $1
		    AND id NOT IN (
		          SELECT id FROM doc_versions WHERE doc_id = $1 ORDER BY id DESC LIMIT $2)
		RETURNING blob_key`,
		id, keep)
	if err != nil {
		return 0, fmt.Errorf("store: prune versions: %w", err)
	}
	var n int64
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: prune versions: %w", err)
		}
		n++
		if key != "" {
			keys = append(keys, key)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: prune versions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		// The deletion may have committed. Keep the objects if the outcome is
		// ambiguous; a committed row deletion makes them orphans, but deleting
		// them before confirmation could break a row that survived.
		return 0, fmt.Errorf("store: prune versions: %w", err)
	}
	// Rows first, objects second: see blobs.go.
	s.dropBlobs(ctx, keys)
	return n, nil
}

// VersionCount reports how many versions a document has.
func (s *Store) VersionCount(ctx context.Context, id UUID) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM doc_versions WHERE doc_id = $1`, id).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count versions: %w", err)
	}
	return n, nil
}
