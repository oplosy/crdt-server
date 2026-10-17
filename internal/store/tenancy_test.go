package store_test

// Ownership and listing, against a real PostgreSQL. The concurrency test in
// particular is only worth anything here: the claim it checks is about what two
// transactions do under READ COMMITTED, which no fake can tell you.

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mesutokul/ycollab/internal/store"
)

func TestTheFirstOpenerOwnsTheDocument(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("owned-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	acme, globex := store.OwnerID("acme"), store.OwnerID("globex")

	owner, _, err := s.Ensure(ctx, id, name, acme)
	if err != nil {
		t.Fatal(err)
	}
	if owner != acme {
		t.Fatalf("the creator does not own the document")
	}
	// Somebody else asking gets the truth, not their own wish.
	owner, _, err = s.Ensure(ctx, id, name, globex)
	if err != nil {
		t.Fatal(err)
	}
	if owner != acme {
		t.Fatalf("Ensure reported %v; the second caller was told it owns somebody else's document", owner)
	}

	// And Load enforces it rather than leaving that to the caller.
	if _, err := s.Load(ctx, id, name, globex); !errors.Is(err, store.ErrWrongOwner) {
		t.Fatalf("Load returned %v, want ErrWrongOwner", err)
	}
	if _, err := s.Load(ctx, id, name, acme); err != nil {
		t.Fatalf("the owner was refused their own document: %v", err)
	}
}

// The claim in ensureIn's comment, checked rather than asserted: two
// transactions racing to create the same document agree on one owner. Reading
// first and inserting after would let both see nothing and both insert - the
// shape of D102, with a tenancy boundary instead of a retention count.
func TestConcurrentCreatorsAgreeOnOneOwner(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("contested-%d", time.Now().UnixNano())
	id := store.DocumentID(name)

	const racers = 12
	owners := make(chan store.UUID, racers)
	errs := make(chan error, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			owner, _, err := s.Ensure(ctx, id, name, store.OwnerID(fmt.Sprintf("tenant-%d", i%2)))
			if err != nil {
				errs <- err
				return
			}
			owners <- owner
		}()
	}
	close(start)
	wg.Wait()
	close(owners)
	close(errs)

	for err := range errs {
		t.Fatalf("ensure: %v", err)
	}
	seen := map[store.UUID]bool{}
	for owner := range owners {
		seen[owner] = true
	}
	if len(seen) != 1 {
		t.Fatalf("%d different owners were reported for one document: %v", len(seen), seen)
	}
}

// A read on the administrative surface must not create the row. It used to,
// because Load creates one - and a row created by an operator's curl would be
// owned by nobody, so the tenant who later opened that name would be refused
// their own document forever.
func TestReadingAMissingDocumentDoesNotCreateIt(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("never-written-%d", time.Now().UnixNano())
	id := store.DocumentID(name)

	doc, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatalf("LoadAny on a missing document: %v", err)
	}
	if doc.Snapshot != nil || len(doc.Updates) != 0 {
		t.Fatal("a document that was never written came back with content")
	}

	// The name is still free, and a tenant may take it.
	acme := store.OwnerID("acme")
	owner, _, err := s.Ensure(ctx, id, name, acme)
	if err != nil {
		t.Fatal(err)
	}
	if owner != acme {
		t.Fatal("the read left a row behind, and the tenant cannot have their own name")
	}
}

func TestListingIsFilteredByOwnerAndPaged(t *testing.T) {
	s, ctx := openStore(t)
	run := time.Now().UnixNano()
	acme, globex := store.OwnerID("acme"), store.OwnerID("globex")

	// Names are prefixed per run so the assertions survive a database that has
	// other tests' documents in it.
	var acmeNames []string
	for i := range 5 {
		name := fmt.Sprintf("list-%d-acme-%02d", run, i)
		acmeNames = append(acmeNames, name)
		if _, _, err := s.Ensure(ctx, store.DocumentID(name), name, acme); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		name := fmt.Sprintf("list-%d-globex-%02d", run, i)
		if _, _, err := s.Ensure(ctx, store.DocumentID(name), name, globex); err != nil {
			t.Fatal(err)
		}
	}

	// One owner's documents, and only theirs.
	page, err := s.List(ctx, store.ListRequest{Owner: acme, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, d := range page.Documents {
		if d.Owner != acme {
			t.Fatalf("%q belongs to %v and was listed for acme", d.Name, d.Owner)
		}
		got[d.Name] = true
	}
	for _, name := range acmeNames {
		if !got[name] {
			t.Errorf("%q is missing from its owner's listing", name)
		}
	}

	// Paging covers everything exactly once. Keyset pagination is the thing
	// being checked: a page boundary must not skip or repeat.
	seen := map[string]int{}
	req := store.ListRequest{Owner: acme, Limit: 2}
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("paging did not terminate")
		}
		p, err := s.List(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range p.Documents {
			seen[d.Name]++
		}
		if p.Next == "" {
			break
		}
		cursor, err := store.ParseCursor(p.Next)
		if err != nil {
			t.Fatalf("the cursor this store wrote does not parse: %v", err)
		}
		req.After = cursor
	}
	for _, name := range acmeNames {
		if seen[name] != 1 {
			t.Errorf("%q appeared %d times across the pages, want once", name, seen[name])
		}
	}
}

// A listing carries sizes so a caller can decide what to fetch, and the name so
// it can fetch it at all.
func TestAListingCarriesWhatIsNeededToActOnIt(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("sized-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	owner := store.OwnerID("acme")

	if _, _, err := s.Ensure(ctx, id, name, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := appendCurrent(t, s, ctx, id, [][]byte{{1, 2, 3}, {4, 5}}); err != nil {
		t.Fatal(err)
	}

	page, err := s.List(ctx, store.ListRequest{Owner: owner, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var found *store.Listing
	for i := range page.Documents {
		if page.Documents[i].Name == name {
			found = &page.Documents[i]
		}
	}
	if found == nil {
		t.Fatal("the document is not in its owner's listing")
	}
	if found.ID != id {
		t.Errorf("id = %v, want %v", found.ID, id)
	}
	if found.Updates != 2 {
		t.Errorf("updates = %d, want 2", found.Updates)
	}
	if found.UpdatedAt.IsZero() {
		t.Error("no updated_at")
	}
}

// SetOwner is the only way an owner changes, so it is the migration path for a
// database that predates tenancy and the correction path for a mistake.
func TestSetOwnerMovesADocument(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("moving-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	acme, globex := store.OwnerID("acme"), store.OwnerID("globex")

	if _, _, err := s.Ensure(ctx, id, name, acme); err != nil {
		t.Fatal(err)
	}
	moved, err := s.SetOwner(ctx, id, globex)
	if err != nil {
		t.Fatal(err)
	}
	if !moved {
		t.Fatal("SetOwner reported nothing to move")
	}
	if _, err := s.Load(ctx, id, name, acme); !errors.Is(err, store.ErrWrongOwner) {
		t.Fatalf("the old owner can still open it: %v", err)
	}
	if _, err := s.Load(ctx, id, name, globex); err != nil {
		t.Fatalf("the new owner cannot open it: %v", err)
	}

	// And a document that is not there is reported rather than silently
	// succeeding, so a typo in a name is not a no-op that looks like a move.
	missing, err := s.SetOwner(ctx, store.DocumentID("no-such-document-"+name), acme)
	if err != nil {
		t.Fatal(err)
	}
	if missing {
		t.Error("SetOwner claimed to move a document that does not exist")
	}
}

func TestSetOwnerRotatesOnlyWhenOwnerChanges(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("owner-incarnation-%d", time.Now().UnixNano())
	id := store.DocumentID(name)
	a, b := store.OwnerID("owner-a"), store.OwnerID("owner-b")
	if _, err := s.Load(ctx, id, name, a); err != nil {
		t.Fatal(err)
	}
	first, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetOwner(ctx, id, a); err != nil {
		t.Fatal(err)
	}
	same, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if same.Incarnation != first.Incarnation {
		t.Fatalf("no-op owner assignment changed incarnation %d to %d", first.Incarnation, same.Incarnation)
	}
	if _, err := s.SetOwner(ctx, id, b); err != nil {
		t.Fatal(err)
	}
	second, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if second.Incarnation == first.Incarnation {
		t.Fatal("owner transfer did not rotate incarnation")
	}
	if _, err := s.SetOwner(ctx, id, a); err != nil {
		t.Fatal(err)
	}
	third, err := s.LoadAny(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if third.Incarnation == second.Incarnation || third.Incarnation == first.Incarnation {
		t.Fatalf("A->B->A reused an incarnation: %d, %d, %d", first.Incarnation, second.Incarnation, third.Incarnation)
	}
}

// A row written before the name column existed can be given its name, once,
// and then it appears in listings.
func TestNamingARowThatHasNoName(t *testing.T) {
	s, ctx := openStore(t)
	name := fmt.Sprintf("legacy-%d", time.Now().UnixNano())
	id := store.DocumentID(name)

	// A row with no name is what Import used to write, and what every row in a
	// database from before this column looks like.
	if _, _, err := s.Ensure(ctx, id, "", store.NilUUID); err != nil {
		t.Fatal(err)
	}
	named, err := s.Name(ctx, id, name)
	if err != nil {
		t.Fatal(err)
	}
	if !named {
		t.Fatal("the row would not take a name")
	}
	// Once. A second call must not rewrite it: the id is a hash of the name, so
	// a row that has one is already findable, and overwriting would mean taking
	// the caller's word over the row's about which document this is.
	again, err := s.Name(ctx, id, "something-else")
	if err != nil {
		t.Fatal(err)
	}
	if again {
		t.Error("a document that already had a name was renamed")
	}

	page, err := s.List(ctx, store.ListRequest{Owner: store.NilUUID, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range page.Documents {
		if d.ID == id {
			if d.Name != name {
				t.Errorf("listed as %q, want %q", d.Name, name)
			}
			return
		}
	}
	t.Error("the named document is not in the listing")
}
