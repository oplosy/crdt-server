package store

// Listing the documents an owner has, and moving a document between owners.
//
// Until this file the database had no way to answer "what is here". The id is a
// one-way hash of the name, so a listing built from ids alone would return
// identifiers nobody could open - which is why the name column exists and why
// rows written before it are visible here with an empty name rather than
// silently omitted.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// DefaultListLimit is how many documents one page returns when the caller does
// not say. Bounded because the alternative - every document a tenant has - is a
// response whose size is somebody else's decision.
const DefaultListLimit = 100

// MaxListLimit caps what a caller may ask for.
const MaxListLimit = 1000

// A Listing is one document as it appears in a list. It carries sizes, never
// content: the point of a listing is deciding what to fetch.
type Listing struct {
	ID   UUID
	Name string
	// Owner is included even though a list is usually filtered to one, because
	// the unfiltered form an operator uses is the one that needs it.
	Owner     UUID
	UpdatedAt time.Time
	// SnapshotBytes is the size of the stored snapshot, and Updates the number
	// of log rows on top of it. Together they are what a document costs.
	SnapshotBytes int64
	Updates       int64
}

// ErrBadCursor means an `after` value did not come from a previous page.
var ErrBadCursor = errors.New("store: malformed cursor")

// Cursor is the position to resume a listing from.
//
// It is (name, id) rather than name alone because name is not unique: every row
// written before the name column existed has an empty one, and a cursor on name
// alone would either skip all but one of them or loop forever on the first.
type Cursor struct {
	Name string
	ID   UUID
}

// String encodes a cursor for a URL. Document names cannot contain a slash -
// the gateway takes the whole path as the name and refuses one that does - so
// the separator is unambiguous.
func (c Cursor) String() string { return c.Name + "/" + c.ID.String() }

// ParseCursor reads what String wrote.
func ParseCursor(s string) (Cursor, error) {
	name, rest, ok := strings.Cut(s, "/")
	if !ok {
		return Cursor{}, fmt.Errorf("%w: %q", ErrBadCursor, s)
	}
	id, err := ParseUUID(rest)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %q", ErrBadCursor, s)
	}
	return Cursor{Name: name, ID: id}, nil
}

// ListRequest is what List is asked for.
type ListRequest struct {
	// Owner selects one owner's documents. NilUUID is a real owner - the one a
	// server without tenancy writes - so it selects those, and AllOwners is how
	// a caller asks for everything.
	Owner UUID
	// AllOwners ignores Owner and lists the whole database. It is for the
	// administrative surface, which serves an operator rather than a tenant.
	AllOwners bool
	// After resumes from a previous page. The zero value starts at the
	// beginning.
	After Cursor
	// Limit bounds the page.
	Limit int
}

// A ListPage is one page of documents plus where to continue.
type ListPage struct {
	Documents []Listing
	// Next is the cursor for the following page, empty when this was the last
	// one. It is derived from having read a full page and finding more, not
	// from the count alone, so a caller is never sent back for an empty page.
	Next string
}

// List returns one page of documents, ordered by name.
//
// Keyset pagination rather than OFFSET: a tenant's documents change while
// somebody pages through them, and OFFSET would skip or repeat rows as they do.
// It also costs the same on page one thousand as on page one, which OFFSET does
// not.
func (s *Store) List(ctx context.Context, req ListRequest) (ListPage, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	// One more than asked for, so "is there another page" is answered by
	// reading rather than by a second count query.
	const query = `
		SELECT d.id, d.name, d.owner_id, d.updated_at,
		       CASE WHEN d.snapshot_key = ''
		            THEN coalesce(octet_length(d.snapshot), 0)
		            ELSE d.snapshot_bytes END,
		       (SELECT count(*) FROM doc_updates u WHERE u.doc_id = d.id)
		FROM documents d
		WHERE ($1 OR d.owner_id = $2) AND (d.name, d.id) > ($3, $4)
		ORDER BY d.name, d.id
		LIMIT $5`

	rows, err := s.pool.Query(ctx, query,
		req.AllOwners, req.Owner, req.After.Name, req.After.ID, limit+1)
	if err != nil {
		return ListPage{}, fmt.Errorf("store: list documents: %w", err)
	}
	defer rows.Close()

	var page ListPage
	for rows.Next() {
		var l Listing
		if err := rows.Scan(&l.ID, &l.Name, &l.Owner, &l.UpdatedAt, &l.SnapshotBytes, &l.Updates); err != nil {
			return ListPage{}, fmt.Errorf("store: list documents: %w", err)
		}
		page.Documents = append(page.Documents, l)
	}
	if err := rows.Err(); err != nil {
		return ListPage{}, fmt.Errorf("store: list documents: %w", err)
	}
	if len(page.Documents) > limit {
		last := page.Documents[limit-1]
		page.Documents = page.Documents[:limit]
		page.Next = Cursor{Name: last.Name, ID: last.ID}.String()
	}
	return page, nil
}

// SetOwner moves a document to an owner, reporting whether there was one to
// move.
//
// This is the migration path and the correction path, and there is no other
// way to change an owner: opening a document never reassigns it. A deployment
// turning tenancy on has documents owned by nobody, and they stay that way -
// visible to a connection that claims no owner, invisible to every tenant -
// until somebody says out loud whose they are. A real owner change rotates the
// incarnation so rooms on other replicas cannot write with their old identity;
// assigning the existing owner is a no-op for that token.
func (s *Store) SetOwner(ctx context.Context, id UUID, owner UUID) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE documents
		SET owner_id = $2,
		    incarnation = CASE WHEN owner_id IS DISTINCT FROM $2
		                       THEN nextval('document_incarnation_seq') ELSE incarnation END
		WHERE id = $1`, id, owner)
	if err != nil {
		return false, fmt.Errorf("store: set owner: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// Name records the name of a document that has none, so that a row written
// before the name column existed can be listed.
//
// It never overwrites a name that is already there. The id is a hash of the
// name, so a row whose name is set is already findable, and rewriting it would
// mean trusting the caller's word over the row's about which document this is.
func (s *Store) Name(ctx context.Context, id UUID, name string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE documents SET name = $2 WHERE id = $1 AND name = ''`, id, name)
	if err != nil {
		return false, fmt.Errorf("store: name document: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
