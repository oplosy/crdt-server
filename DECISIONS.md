# DECISIONS

One entry per non-obvious design decision, with the alternative rejected and why.
Part 2 is the wire-format write-up: every claim cites the source file and function it
was derived from. All paths are relative to `tools/fixturegen/node_modules/`, pinned at
`yjs@13.6.31`, `y-protocols@1.0.7`, `lib0@0.2.117`, `y-websocket@3.0.0`.

---

## Part 1 — Decisions

### D1. Go module path is `github.com/mesutokul/ycollab`
The directory is named `CrdtServer`, the project is named `ycollab`. Rejected
`github.com/mesutokul/crdtserver` — the import path should read like the product, and the
directory name is an accident of where the repo was cloned.

The repository itself is hosted at `github.com/Contictus/crdt-server`, and the released
image follows the repo (`ghcr.io/contictus/crdt-server`, from `github.repository` in the
Release workflow). The module path is left pointing at the product name deliberately and is
not required to resolve as a `go get` target.

### D2. Fixture tooling depends on real `yjs`, pinned to exact versions
`tools/fixturegen` installs `yjs`, `y-protocols`, `lib0` with `--save-exact`. Rejected
caret ranges: the fixtures are checked in as binary, so a silent minor-version bump that
changes encoding would show up as a mysterious Go test failure rather than as a dependency
change in review.

`y-websocket@3.0.0` is a devDependency of the same project. The outer websocket framing
(`messageSync = 0`, `messageAwareness = 1`, `messageAuth = 2`, `messageQueryAwareness = 3`) is
defined by `y-websocket`, not by `y-protocols`, and the brief says derive the protocol from
source rather than memory. Since Phase 2 it also does real work: `tools/soak` drives its
`WebsocketProvider` against the Go server, and `web/` pins the same version. Node tooling, not
a Go dependency.

### D3. `node_modules` is gitignored, `package-lock.json` is committed
The brief calls `node_modules` the authoritative spec source, which argues for committing it.
Rejected: ~10 MB of vendored JS in a Go repo, and the lockfile plus exact versions already
make the tree reproducible with `npm ci`. The fixture `manifest.json` also records the
versions the committed binaries were generated with.

### D4. Fixture client IDs are hardcoded
`Y.Doc` assigns a random 32-bit `clientID`, so every regeneration would produce different
bytes and a noisy diff. `createHarness().doc(label, clientID)` overwrites `doc.clientID`
before the first operation. Verified: two consecutive `node generate.mjs` runs produce 221
byte-identical files.

### D5. One `node_modules`; the verifier imports through the generator
`tools/verify/apply.mjs` gets `yjs` via `import { Y } from '../fixturegen/dump.mjs'` instead of
having its own `npm install`. Rejected a second install: the verifier's whole job is to prove
Go bytes are accepted by *the same* Yjs that produced the fixtures. Two installs could drift
apart and turn a real incompatibility into a passing test.

### D6. Fixtures carry both bytes and a JSON description of those bytes
Each scenario directory holds the binary (`state.bin`, `update-NNN.bin`, `diff.bin`, framed
`msg-*.bin`) *and* `expected.json` / `updates.json`, which describe every struct in wire order
(`id`, `origin`, `rightOrigin`, `parent`, `parentSub`, content ref and value) as decoded by
`Y.decodeUpdate` (`yjs/src/utils/updates.js:139`). Rejected bytes-only fixtures: when the Go
decoder disagrees, a struct-level JSON diff points at the field, whereas a byte diff points at
an offset.

### D7. The generator fails if fixture coverage regresses
`assertCoverage()` decodes every emitted `.bin` and requires content refs 1, 3, 4, 5, 6, 7, 8, 9
and struct kinds `Item`, `GC`, `Skip` to be present. Rejected trusting the scenario list: a
scenario edit that quietly stops producing `GC` structs would leave a green test suite covering
less than it claims.

Content ref 2 (`ContentJSON`) has no fixture: current Yjs never emits it (`ContentAny`
superseded it), so it cannot be produced through the public API. The Go decoder must still
support it — that will be covered by a hand-built byte sequence in the Go tests.

### D8. Decode every content ref; expose only `YText` and `YMap` in Go
Section 8 puts `YXmlFragment` out of scope, but the Phase 2 acceptance criterion is TipTap,
and TipTap (`y-prosemirror`) stores its document as `Y.XmlFragment` of `Y.XmlElement` /
`Y.XmlText`. So `ContentType` with type refs 3/4/6, plus `ContentFormat` and `ContentEmbed`,
*will* arrive on the wire whatever we decide.

Resolution (confirmed with you): `internal/crdt` decodes and re-encodes every content ref
byte-exactly, but the typed Go API stays `YText` + `YMap` as the brief specifies. The server
never needs type semantics — it integrates structs and computes diffs. The `xml-prosemirror`
fixture is verified by round-trip and state-vector equality, not by a Go XmlFragment API.
Rejected: implementing a Go XmlFragment API now (more Phase 1 surface for no server benefit);
also rejected swapping the demo to Monaco (loses the richer demo and does not remove the
decoding requirement anyway).

### D9. The import-purity test walks non-test files only
Section 4 requires zero third-party imports in `internal/crdt`, and also allows `gopter` for
the Phase 1 property tests that live in that directory. Resolution: the purity test inspects
non-`_test.go` files, and property tests live in external test packages (`package crdt_test`).
This keeps the shipped package standard-library-only, which is the actual goal.

### D10. Update format v1 only
Yjs has two encodings: v1 (`UpdateEncoderV1`) and v2 (`UpdateEncoderV2`, run-length /
delta-compressed columns, `yjs/src/utils/UpdateEncoder.js:162`). `y-websocket` and
`y-protocols/sync` use v1 exclusively (`sync.js:61` calls `Y.encodeStateAsUpdate`, which is v1
per `yjs/src/utils/encoding.js:555`). Implementing v2 would add several column encoders
(`IntDiffOptRleEncoder`, `UintOptRleEncoder`, `StringEncoder`) for zero wire benefit. Out of
scope per section 8, and confirmed by the source: v1 is what browsers send.

### D11. Each fixture records its `gc` setting
Applying a fixture's `state.bin` into a document with a different `gc` flag collapses
tombstones differently (`Y.Doc({gc})` controls whether deleted items become `GC` structs), so
`expected.json` carries `gc` and the verifier constructs its document accordingly. Without
this, `text-delete` (gc off, `ContentDeleted` preserved) and `gc-and-skip` (gc on, structs
collapsed) cannot both be checked by the same harness.

### D12. `lib0.Encoder` collects a sticky error instead of returning one per write
Every write would otherwise return an `error` that no call site can do anything useful with,
and the CRDT encoder makes thousands of them. Instead the first failure is recorded, later
writes become no-ops, and callers check `Err()` once — the `bufio.Writer` pattern. The only
failure a writer can produce is a value above `MaxSafeInteger`, which is a programming error,
not a runtime condition.

### D13. Go is deliberately stricter than lib0 on truncated input
`lib0/decoding.js` `readVarInt` reads past the end of its buffer, gets `undefined`, and
returns `0` instead of throwing (`readVarUint` does throw). Go returns `ErrUnexpectedEOF`.
Rejected bug-compatibility: silently accepting truncated frames from the network is how a
server ends up integrating garbage. The divergence is recorded as a `goStricter` vector in
`testdata/fixtures/lib0/vectors.json`, and the generator asserts lib0 still behaves that way,
so if a future lib0 fixes it we find out.

### D14. `-race` runs locally via MinGW-w64
`go test -race` on windows/amd64 needs `CGO_ENABLED=1` and a C compiler, and this machine had
neither. Resolved by installing WinLibs MinGW-w64 (`winget install
BrechtSanders.WinLibs.POSIX.UCRT`, gcc 16.1.0) and `go env -w CGO_ENABLED=1`. The installer
puts its `mingw64\bin` on the persistent user PATH, so new shells pick it up; shells started
before the install need to be restarted. Rejected running the race build only in Docker/WSL:
the feedback loop matters most in Phase 2 where the room actor and gateway are written, and a
container round trip per test run would discourage running it.

### D15. Decoded `any` objects keep their key order
`writeAny` writes object keys in `Object.keys` order (`lib0/encoding.js:590`) and Yjs stores
the resulting bytes verbatim inside `ContentAny`, so a value that is decoded and re-encoded
must reproduce that order. A Go `map[string]any` cannot, so `ReadAny` returns an ordered
`*lib0.Object` (a `[]Field` slice with `Get`/`Set`). `WriteAny` still accepts a plain map for
values we construct ourselves, writing its keys sorted so our own output stays deterministic.
Rejected returning `map[string]any` for ergonomics: it would make re-encoding a document
byte-unstable, which is exactly the property Phase 1 is verified on.

### D16. NaN is canonicalised on write
Go's `math.NaN()` is `0x7FF8000000000001`; V8 produces `0x7FF8000000000000`. Both are NaN, but
only the second matches the bytes a browser writes, so `WriteAny` writes the canonical pattern
for every NaN and drops the payload bits. JavaScript cannot express a NaN payload through
`writeAny`, so nothing is lost. Found by the golden vector, not by reading the source —
`isFloat32(NaN)` is `false` because it compares `NaN === NaN`, which is what puts NaN on the
float64 branch in the first place while `Infinity` goes to float32.

### D17. `any` decoding is depth- and length-guarded
`readAny` is recursive and takes lengths from the input; a hostile 5-byte frame can otherwise
ask for a 2^31-element array. Go caps nesting at 128 (`ErrDepthExceeded`) and refuses any
length prefix larger than the remaining input before allocating. Yjs has no such limits;
this only rejects input no real client produces.

### D18. Encoding canonicalises client order; decoding accepts any order
Yjs writes client blocks, delete-set clients and state-vector clients descending, but its
decoder accepts any order. Go decodes any order and always *writes* the canonical one, so
`Encode` is a fixed point: encoding an already-encoded update never changes it again. The
fixture test asserts byte identity for the 132 updates Yjs actually produced; the fuzz target
asserts idempotency for everything else. Rejected preserving the input order: it would mean
carrying an ordering field through the whole data model to be bug-compatible with input no
client sends.

### D19. Values that decode must be re-encodable
`lib0`'s `readVarUint` can return values above 2^53−1 (its range check runs only when a
continuation byte follows) while `writeVarUint` refuses them. That gap let a decoded client id
become an update that could not be written back — found by the fuzzer as a silently truncated
re-encode. Client ids, clocks and IDs are now range-checked on the way in, and `Update.Encode`
/ `EncodeStateVector` return an error instead of a short buffer, since `lib0.Encoder` drops
every write after its first failure.

### D20. Structs whose info byte carries meaningless flags are rejected
The flag bits for origin, rightOrigin and parentSub have no meaning on a `GC` or `Skip`
struct. Yjs ignores them and writes zeros back, i.e. it silently rewrites the struct. Go
rejects the update instead (`ErrCorruptUpdate`), so that what this server relays is always
what it received. Also found by the fuzzer.

### D21. Embed, format and legacy-JSON values are kept as raw JSON text
Yjs stores these as `JSON.stringify` output and re-serialises them on write. Go cannot
reproduce JavaScript's key order from a parsed value, so a decode-then-re-encode would change
bytes that clients hash and merge. The raw string is stored instead and parsed only when a
typed accessor needs the value. Same reasoning as [D15] one level up.

### D22. Undeliverable updates are held, not rejected
An update whose dependencies are missing goes into a pending list and is retried after every
later integration, mirroring Yjs's `restStructs`/`pendingStructs`. Rejecting it would lose the
edit permanently, and a Yjs client legitimately sends an update before the one it builds on
when a websocket reconnects mid-stream. The same applies to delete-set ranges naming clocks we
have not received (`pendingDeletes`). `Doc.PendingCount` exposes the backlog so Phase 2 can
alarm on it.

### D23. Type lengths are computed by walking, not cached
Yjs maintains `AbstractType._length` incrementally. Go recomputes it, because the server does
not edit documents in a loop - it integrates updates and serialises them - and an incremental
counter is one more thing that can silently drift out of sync with the item list. Revisit if
Phase 6 benchmarks show it.

### D24. Go re-encodes documents byte-identically to Yjs
Not a decision so much as a checked property: applying each fixture's `state.bin` into a Go
`Doc` and calling `EncodeStateAsUpdate(nil)` reproduces the original file byte for byte, for
all 13 scenarios, and `tools/verify/apply.mjs` accepts every one of those outputs in a real
`Y.Doc`. `cmd/ycollab-dump` regenerates the outputs for that check.

### D25. `internal/protocol` is pure bytes
The framing layer takes and returns byte slices and touches no socket, no clock beyond the
timestamp callers pass in, and no room state. That is what lets every message type be checked
against the committed `msg-*.bin` fixtures that real `y-protocols` produced, in both
directions. Rejected: folding the framing into the gateway, where it would only be reachable
through a live connection and the fixtures would go unused.

### D26. Awareness state is relayed verbatim, never parsed
A client's awareness state is kept as the raw JSON string it sent. The server has no business
knowing what a cursor is, and re-serialising the value would let key order or number formatting
drift on the way through, for no benefit. Same reasoning as D21 for embed and format content.
Rejected: decoding to a Go map, which would also mean re-encoding on every fanout.

### D27. Updates are relayed as the author's bytes
When a client sends an update the room applies it and then broadcasts **the bytes it received**,
rather than re-encoding a delta from its own document. Two reasons: peers get exactly what the
author produced, and an update whose dependencies we are still missing is relayed rather than
stuck behind our own integration state (which is what would happen if we broadcast a diff from
our store — see [C5]). Yjs's own server broadcasts the update its transaction emitted, which is
a merged form; ours is closer to the wire the client wrote. Duplicate structs on the wire are
idempotent, so the cost is bandwidth, not correctness.

### D28. A sender never receives its own update back
The reference `y-websocket` server broadcasts to every connection including the author. We skip
the author: it already has the update by construction, and echoing doubles the fanout of the
one message pattern that dominates traffic. Consequence to be aware of: a client **alone** in a
room hears nothing at all, so after 30 s it hits `messageReconnectTimeout`
(`y-websocket.js:99,388-397`) and reconnects. That happens with the reference server too - its
ping is a WebSocket control frame, which does not reset the client's timer - and a reconnect
costs one `SyncStep1` and a diff. Rejected: sending traffic we do not need in order to keep a
client's timer happy.

### D29. Backpressure closes the connection at 1008
Per the brief. The outbound queue is 256 frames; when it is full the room closes that
connection with 1008 and drops it, without growing the buffer and without ever blocking the
room goroutine. Recovery is a reconnect and a diff against the client's state vector, which is
precisely what the CRDT makes cheap. Rejected: an unbounded queue (OOM under fanout), and
blocking the room (one slow client stalls everybody editing that document).

### D30. Removals do not bump the client's clock
Found by the 5-minute soak, not by reading the source. When a connection leaves, the server
retracts the awareness states it published. Bumping the clock on that retraction looks natural -
it guarantees peers accept it - but the departing client does not know we bumped it, so when it
reconnects and announces itself one clock past what it last sent, the announcement lands on an
equal clock and is rejected. The client stays a ghost until its own 15 s renewal pushes it past
us. Removals therefore go out at the client's own clock, which peers accept anyway under the
equal-clock null rule (`awareness.js:250`). `y-protocols` does the same: `removeAwarenessStates`
bumps only the *local* client's clock, never that of the peers it drops (`awareness.js:175-181`).

### D31. Room eviction calls a hook, and in Phase 2 the hook is nil
An idle room stops after five minutes and the document is dropped. `OnEvict` is the seam Phase 3
fills with a snapshot write. Rejected: introducing a `Store` interface with a no-op
implementation now, which would be a guess at Phase 3's shape dressed up as a design.

### D34. Room names map to document UUIDs by hashing
The schema keys documents by UUID; the wire protocol keys rooms by the URL path,
which people want to be readable. A name that already is a UUID is used as one; anything else
becomes a version 5 UUID under a fixed namespace. Deterministic, no lookup table, and the
schema stays exactly as the brief wrote it. Rejected: requiring UUIDs in the URL (unusable
demo, and it pushes name management onto every client), and adding a `name` column (a schema
change plus a second round trip on every open, to buy a rename feature nobody asked for).

### D35. Loading applies every remaining log row, not just those above snapshot_seq
The brief describes loading as the snapshot plus `doc_updates WHERE seq > snapshot_seq`. We
read the whole remaining log instead. `seq` comes from an identity column, and identity values
are handed out before commit, so a row with a lower seq can become visible *after* one with a
higher seq. With the brief's filter, such a row is skipped forever - silent data loss.
Compaction deletes exactly the rows it folded in, so anything still in the table has not been
folded in, and if a row is ever re-applied redundantly it costs nothing, because updates are
idempotent. This remains necessary for [C6]. The room writer also reloads the complete
remaining log before replacing a snapshot, so a late lower-seq row can be merged into the
replacement rather than skipped.

### D36. The room supplies an immutable candidate; the writer rebases before pruning
The room owns the live CRDT document, so it encodes an immutable candidate and hands it to the
writer. The writer flushes queued updates, reloads the current persisted snapshot and every
remaining log row, then merges those with the candidate in a separate temporary CRDT document.
It asks `Store.Compact` to replace the snapshot only against the generation and document
incarnation it loaded, and supplies the exact seqs included in the merge (including its
accumulated local rows; see [D41]). If another replica wins first, the writer reloads and
remerges within a bounded attempt count. It never mutates the room's live document. Unreadable
rows or unresolved dependencies prevent pruning and leave the candidate for a later retry.

### D37. Writes are asynchronous, and the durability window is the flush interval
The room hands updates to a per-room writer goroutine and carries on. A crash therefore loses
whatever was queued but not yet written - at most one flush interval, 200 ms by default. That
is acceptable here in a way it would not be in most systems, because the client that authored
the update still has it and pushes it again during the reconnect handshake; the acceptance test
covers exactly that path. Rejected: writing synchronously, which puts a database round trip in
the middle of every keystroke's fanout and makes the database decide how fast people can type.
When the write queue does fill, the room blocks rather than dropping: stalling is visible and
recoverable, and silently discarding updates that clients believe are saved is neither.

### D38. A failed read closes the room; a failed write does not
Asymmetric on purpose. If the document cannot be read, serving an empty one under that name
would let clients merge their state into a blank document and write the result back, which
destroys the document for everyone. So the room closes with 1011 and clients retry. If a write
fails, the document is still correct in memory and in every connected client, so the room keeps
serving and logs loudly. Losing durability is bad; losing the document is worse.

### D39. The resident-room cap evicts, it does not refuse
Now that a room can write itself out, hitting the cap should cost a snapshot, not a document:
the least recently used *idle* room is evicted to make space. A room with somebody connected is
never evicted - freeing memory by disconnecting people who are editing is not a trade the
registry may make - so the cap can still be reached, and then the join is refused.

### D42. ContentJSON is tested against hand-built bytes
Ref 2 is the one content type with no fixture: current `yjs` writes `ContentAny` for everything
the public API can produce, so the generator cannot emit one. Documents written by older
versions still contain it. The test therefore builds the update by hand from `ContentJSON.js`,
and the bytes were checked against real Yjs once: it reads the five values - including the
`undefined` that is not valid JSON - and re-encodes them byte for byte identically to what we
wrote. Rejected: leaving the branch untested because no fixture exists, which is how an
unreachable-looking decoder path turns into corruption the first time a real old document
arrives.

### D40. Pending deletions are advertised, unlike in Yjs
`EncodeStateAsUpdate` includes the deletions being held for structs we have not received, which
`Y.encodeStateAsUpdate` does not (it omits `pendingDs`). A peer that cannot resolve the range
holds it pending exactly as we do; a peer that can resolve it applies a deletion it was going to
receive anyway. The gain is that two replicas which have exchanged everything now agree, instead
of the deletion trailing its structs by one exchange, and that a pending deletion survives being
written into a snapshot. Rejected: matching Yjs exactly, which is the default position in this
project — but here the divergence is invisible on the wire (the delete set is a delete set
either way) and removes a real class of transient disagreement. Resolves [C5].

### D41. Compaction deletes rows by identity, not by range
`DELETE ... WHERE seq = ANY($folded)` rather than `seq <= watermark`. Identity values are handed
out before commit, so a range can take a row that the snapshot never saw. The room's writer
therefore accumulates the seqs it has written, hands them over with the snapshot, and clears
them once the transaction lands. Rejected: keeping the range and taking a heavier lock on the
append path, which would put contention on the hot path to protect an operation that runs once
per 500 updates. Resolves [C6].

### D43. The bus carries envelopes, not bare updates
Every message between replicas is `varUint(version) varUint(origin) varUint(kind)
varUint8Array(payload)`, encoded with our own lib0. The origin is what makes loop prevention
possible at all, the kind is what lets one channel carry updates, awareness and anti-entropy,
and the version is what makes a rolling restart legible instead of mysterious. Rejected:
publishing the raw update bytes to a channel per kind, which saves a few bytes and gives up the
origin - and without an origin, Pub/Sub delivering to the publisher means either a hop count or
a permanent echo. Rejected also: JSON, which would put a second serialisation format in a
project whose first one is tested byte for byte against real Yjs.

### D44. Loops are prevented by origin, at the subscriber
A replica drops any envelope carrying its own node id, and never publishes anything that came
off the bus. Together those two rules make a message cross the cluster exactly once, whatever
the cluster size, which is the property the Phase 4 acceptance criterion asks for a counter to
prove: with the clients quiet, update traffic goes to zero, and the counters say so on /statsz.
Rejected: a hop count or TTL, which bounds a loop instead of preventing it and needs tuning per
topology. Not available: not receiving your own publishes - Redis Pub/Sub has no such option.

### D45. The node id is random, not configured
64 bits from crypto/rand, masked to the 53 lib0's varUint can carry. Rejected: deriving it from
the hostname, pod name or replica index, all of which are one misconfiguration away from two
replicas sharing an id - at which point each ignores the other's traffic and the two diverge in
a way that looks exactly like a CRDT bug. Zero is reserved, so an unset field cannot be mistaken
for a valid id.

### D46. One multiplexed subscriber connection, not one per room
The Redis bus keeps a single subscriber connection and demultiplexes by channel name, adding
and removing channels as rooms come and go. Rejected: a subscription per room, which is simpler
to write and costs a Redis connection per resident document - a limit nobody wants to discover
during an incident, and one that would interact badly with the resident-room cap ([D39]).

### D47. Subscribing waits for Redis to confirm it
Found by the integration test, which failed immediately and consistently: `PubSub.Subscribe`
returns once the command has been written, so a publish issued straight afterwards can reach
Redis before the subscription exists and be delivered to nobody. For a room that is precisely
the case that matters - it subscribes, then serves a client whose first edit must not vanish -
so the bus now waits for the server's subscription confirmation, which arrives on the receive
loop as a `*redis.Subscription`. That is also why the loop calls `Receive` rather than
`ReceiveMessage`: the latter swallows exactly the message being waited for. Rejected: sleeping.
Rejected: accepting the loss on the grounds that anti-entropy would repair it, which trades a
correctness property for the absence of ten lines.

### D48. An update is written by the replica whose client authored it
Remote updates are applied and relayed to local clients but not appended to the log. Every
update is therefore in the log exactly once, from its origin. Rejected: having every replica
write everything, which multiplies the write rate and the log size by the replica count to
protect against a case - the origin dying inside its flush window - that the client's own
reconnect already covers ([D37]). This is also the point where [D41] stops being a nicety: with
two replicas appending to one document, a compaction that deleted a *range* would delete rows
the other replica had written and this one had never seen.

### D49. Anti-entropy is a periodic state vector, answered with a diff
A room with connections publishes its state vector every 15 s; a replica that sees one computes
`EncodeDiff` against it and publishes the difference, or nothing at all when there is none.
This is the `SyncStep1`/`SyncStep2` exchange the clients already perform, run between replicas
on a timer, and it is what makes an at-most-once bus safe to build on. Resolves [C1].
Rejected: reconciling once when a room is created, which fixes a restarting replica and not a
dropped message, and which [C7] rules out anyway - one exchange is not a fixed point. Rejected:
Redis Streams, which would give at-least-once delivery and take back consumer groups, trimming
and a second failure mode; the periodic exchange costs one short message per room per replica
and repairs everything, including whatever a Streams consumer would still have missed.

### D50. A timeout is local; a disconnect is published
When a connection leaves, its awareness retraction goes out on the bus, so the cursor
disappears everywhere at once. When the awareness sweep times a client out, it does not: a
timeout is this replica's conclusion drawn from its own silence, and the replica the client is
actually connected to is the one that knows better. Publishing sweeps would let a node with a
slow bus connection delete live cursors across the cluster.

### D51. Publishing is off the room goroutine, and drops rather than blocks
The room hands envelopes to a bounded queue drained by a publisher goroutine, and the bus hands
envelopes to a bounded per-room queue drained by the room. Neither side ever blocks the other:
a document must not wait for the network, and one busy document must not stall every other
document on the node. Both queues drop and count when full, which is honest - the next
anti-entropy round repairs the loss - and both counters are on /statsz, so a node under that
much pressure is not silent about it. Rejected: blocking, which is what the *persistence* queue
does ([D36]) and is right there, because losing a write is unrecoverable while losing a fanout
message is not.

### D52. A room that cannot join the bus refuses to serve
If the subscription fails, the room closes with 1011 instead of starting. Rejected: serving
anyway, which produces the worst failure this system has - a document that looks fine, accepts
edits, and silently ignores everyone on the other replicas. Failing makes the client reconnect,
probably to a replica that is healthy.

### D53. /statsz is JSON, and deliberately small
The cluster counters are exposed as a flat JSON object. Prometheus is Phase 6 and this is not a
substitute for it: it exists because the Phase 4 criterion is about numbers a test has to read,
and a test that has to parse an exposition format to learn whether updates looped is a test
nobody writes.

### D54. Caddy does not pin a client to a replica
No sticky sessions, no affinity. The point of this phase is that any replica can serve any
document, so a load balancer that pinned clients would hide the very thing the deployment
exists to demonstrate. A client that reconnects lands wherever it lands and resyncs from its
state vector, which costs one diff.

### D55. A token names its document, so it is a capability rather than a login
The `doc` claim is required and must match the document being opened. A token
that leaks therefore opens one document until it expires, instead of being a key
to the server. Rejected: authenticating the user and looking their permissions up
in a table, which is what most systems do and which would put a database read on
the connection path and a permission model in a server that has no idea what a
user is. The application that knows who its users are mints these tokens; this
server only checks them.

### D56. HS256 with a shared secret, and the algorithm is pinned
One symmetric key, given to the server and to whatever mints tokens. Rejected:
RS256 or a JWKS endpoint, both of which are better when the issuer is a separate
system that already exists, and both of which add key distribution or a network
call to a phase whose job is to make the seam real. The parser is pinned to
HS256 with `jwt.WithValidMethods`: without that, a token whose header says
`"alg":"none"` is accepted, which is the oldest bug in JWT and is why the test
suite mints one and checks that it is refused.

### D57. Several secrets may be configured at once
`-jwt-secret a,b` accepts tokens signed with either. That is the whole of key
rotation: add the new key, wait for the tokens signed with the old one to expire,
remove the old one. Rejected: a single key, which makes rotation an outage.

### D58. Expiry is required by default, and may be capped
A capability with no expiry is a permanent one, and the safety of putting a token
in a URL rests entirely on it being short-lived - a URL ends up in browser
history, in access logs and in whatever the user pastes into chat. So a token
with no `exp` is refused unless `-jwt-require-expiry=false`, and
`-jwt-max-lifetime` refuses tokens valid for longer than a deployment wants to
allow. Rejected: trusting the issuer to be sensible, which is a policy the server
can enforce for the cost of two comparisons.

### D59. The token travels in the query string
`?token=...`, which is what `y-websocket`'s `params` option writes. Rejected: an
`Authorization` header, which a browser cannot set on a WebSocket - the API
simply has no argument for it - so requiring one would mean writing a custom
client and giving up the project's central promise that unmodified Yjs clients
work. Rejected also: inventing an auth frame the client sends after connecting,
which would be a message `y-websocket` does not send, breaking the same promise.
A header *is* accepted as an alternative, for clients that are not browsers. The
cost is that the token appears in URLs, which is exactly what [D58] is about.

### D60. Authorisation happens before the upgrade, and the refusal is sent after it
The check runs before `websocket.Accept`, so a request that fails never reaches
a room and cannot even cause one to be created. The *rejection*, though, is
written over the upgraded connection as a `y-protocols/auth` permission-denied
message followed by a 1008 close, because that is where the reference client
reads it (`y-websocket.js:84-92`): a client refused with an HTTP status simply
retries forever, while one that receives this message stops. Rejected: replying
401 before upgrading, which is more correct as HTTP and worse as behaviour.

### D61. Read-only is enforced in the room, not at the edge
The gateway decides the permission once and the room applies it, because the room
is the thing that integrates updates - and after Phase 4 it is also the thing that
publishes them to other replicas. Enforcing it there means a refused update is
never applied, never persisted, never relayed to a local peer and never put on
the bus, all from one check.

An empty update is exempt. A well-behaved read-only client answers our SyncStep1
with a diff, and once it has the document that diff carries nothing; treating it
as an attempt to write would disconnect every read-only client on its second
message.

### D62. A read-only client that writes is told, and disconnected
It gets a permission-denied message and a 1008 close. Rejected: silently
discarding the update, which is tempting because it keeps the connection alive -
and which shows the user a document that looks edited, will not survive a reload,
and diverges from what everyone else sees. A visible failure is kinder than a
document that lies. This is the same policy as backpressure ([D29]): say why,
then close.

### D63. The write pump flushes its queue before the close frame
Refusing an update means queueing an explanation and then closing the connection,
and the write pump selected between "a frame is waiting" and "we are closing"
with no order between them - so the explanation was lost about half the time.
The pump now drains whatever is queued, then writes the close frame, then
unblocks the reader ([D33]). The queue is bounded at 256 and every write has its
own deadline, so a hostile peer cannot hold the shutdown open.

### D64. Without a secret, the server is open, and says so
No `-jwt-secret` means every connection may read and write, and the server logs a
warning at startup, alongside the ones for no database and no Redis. Rejected:
refusing to start without one, which would make the demo a configuration exercise
and push people towards checking in a secret. Rejected also: staying quiet, which
is how an open server reaches production.

### D65. The operator endpoints live on their own listener
`/metrics`, `/statsz` and `/debug/pprof` are served on `-admin-addr`, which
defaults to `127.0.0.1:6060`, and none of them is on the port clients connect to.
pprof will dump the heap, print the command line the process was started with,
and block the process for a thirty-second CPU profile on request: on a public
port that is both an information leak and a way for anyone to stall the server.
A separate listener lets the deployment decide who can reach them - a bind
address, a firewall rule, or in Kubernetes simply not naming the port in the
Service. `-pprof=false` turns the profiler off while leaving the metrics, for a
deployment that cannot isolate the port. Rejected: serving them on the main port
behind a path prefix, which is one misconfigured proxy away from being public.
`/healthz` stays on both, because a load balancer probes the port it sends
traffic to.

### D66. Metrics are a struct that is passed in, not package-level variables
`metrics.New(registry)` returns every collector, and the packages that report
into it take one in their config. That costs two config fields and buys two
things: a test builds a fresh set against its own registry and asserts on what
was recorded, and `metrics.Nop()` exists - registered nowhere - so no call site
ever checks for nil before counting something. Rejected: package-level
collectors on the default registry, which is what most Go code does and which
makes two tests in one binary collide.

### D67. Labels are a fixed small set, never a document name
Close codes are labelled, message types are labelled, refusal reasons are
labelled with a slug rather than the error text. No metric carries a room name
or a client id. A label whose values come from user input is how a metrics
endpoint becomes the thing that takes the server down: cardinality is memory
here and in whatever scrapes it.

### D68. The load bot speaks the wire protocol, not Yjs
`cmd/ycollab-load` builds updates by hand and holds a socket per client, so a
thousand clients cost a thousand goroutines and a few megabytes. Rejected:
driving real `y-websocket` clients, which is what `tools/soak` does and is the
right tool for *correctness* - but a real client costs a document, a provider and
a couple of megabytes, so a few hundred of them make the load generator the
bottleneck and measure nothing about the server. Wire correctness is already
covered byte for byte by the fixtures, so the bot is free to be cheap.

Its update builder is the one place outside `internal/crdt` that writes Yjs
structs, which is a real risk of drift, so it self-tests at startup: it builds a
chain, integrates it with the actual engine and checks the text. A malformed
builder fails there instead of as a stream of 1002 closes.

### D69. The bot connects in waves
Dialling a thousand sockets at once overran the listen backlog and the kernel
refused a fifth of them - which measures the accept queue, not the server, and is
not what any real population of clients does. `-connect-at-once` bounds the dials
in flight.

### D70. The latency report states the clock's resolution
The propagation percentiles are printed next to the smallest step this machine's
monotonic clock can take, measured at startup. On the development machine that
step is about half a millisecond, so a p50 below it is reported as `0s` - which
without the resolution line reads either as a bug or as a boast, and is neither.
Rejected: printing microseconds the clock cannot resolve.

### D71. A removed awareness entry is forgotten after ten minutes
Removing a cursor keeps its clock, so a replayed update cannot resurrect it. That
was right and incomplete: a Yjs client picks a new random id for every `Y.Doc`,
so every reconnect left an entry behind, and a room that stays resident for days
grew one per reconnect. It was a slow leak in exactly the rooms that matter most,
found by reading the code rather than by any test failing.

The sweep now drops entries whose state was removed more than ten minutes ago -
far past any in-flight duplicate, far short of a working day. The worst case if
a genuinely ancient update arrives afterwards is one ghost cursor, which the next
sweep removes. Rejected: keeping them forever, which is correct and unbounded;
rejected: dropping them immediately on removal, which would let a duplicate
removal or a slow peer resurrect a cursor.

### D72. A node caps its connections
`-max-conns` refuses connections past a limit with 503, before the upgrade. A
connection costs two goroutines, a 256-frame queue and a room, and nothing else
in the server bounded how many there could be: with `-jwt-secret` unset there was
no gate at all, and with it set there was still none on how many connections one
tokenholder could open. Default is zero, meaning no cap, which is right for a
laptop and wrong for anything reachable - so the Kubernetes manifest sets it, as
it sets `-max-rooms`.

### D73. Durable writes are an option, not the default
`-durable-writes` makes the room wait for the final result of a bounded append
retry sequence before it relays an update, reducing the normal unflushed window
[D37] leaves open. It is off by default
because it puts a database round trip on every keystroke. It may help when a
client retains and resends an edit after a server restart, but that is not
guaranteed when no client still holds it. It exists because "how much may we
lose" is a question only the deployment can answer, and the honest answer for
some of them is "nothing".

What it promises is narrower than the name suggests, and the difference matters
when reading an incident: the room waits for up to three attempts within one
30-second batch deadline, not for proof that the update is on disk. A final
failure is logged and counted on `ycollab_store_failed_total`, and the update
is relayed regardless; the exhausted batch is not queued for later replay. The
alternative - block until the database answers - turns a database outage into
every document on the server hanging, with the edits still only in memory
either way. The flag reduces the normal unflushed window; it does not turn the
server into one that refuses edits it cannot store. A deployment that
needs the second thing needs an alert on that counter and a decision about what
to do when it fires, because this server does not have one to make on its own.

### D74. Documents can be deleted, and retention is opt-in
`DELETE /documents/{name}` on the admin listener removes a document from memory
and from the database; the log goes with it through the foreign key's cascade. A
document somebody is editing is refused with 409 rather than deleted out from
under them.

The admin listener is the authorisation. A token would be the wrong mechanism:
the tokens this server understands are per-document capabilities minted for
editors, not operator credentials, and inventing a second kind would be inventing
an identity system.

`-retention` deletes documents nothing has touched for a given period, off unless
asked for. A collaborative document nobody has opened in months is still
somebody's document, and deleting it by default would be the server making a
policy call it has no standing to make. Activity is read from the log rather than
maintained as a column, which keeps the append path at one statement: appending
is the hot path, retention runs a few times a day.

### D75. The cluster counters are collected at scrape time
`/metrics` exposes the same numbers as `/statsz` through a collector that reads
the room manager's atomics when Prometheus asks. Rejected: incrementing a
Prometheus counter next to each atomic, which is the obvious implementation and
gives two sources of truth for one fact - the kind of duplication that ends with
a dashboard and an endpoint disagreeing during an incident.

### D76. Measured before optimised
`internal/crdt` has benchmarks for the three paths a busy server lives in. On the
development machine: integrating an update is about 630 ns, an update that is
already known costs 415 ns to reject, a state vector encodes in 480 ns, and a
20 KB document produces a full diff in 23 µs. The load runs put the server at
98,000 delivered frames a second with nothing dropped, and the server's own
histogram puts the mean integration at about a microsecond.

So no optimisation was applied: at those numbers the engine is not what limits
this server, and a speculative change to the one part of the codebase whose
output is verified byte for byte would be trading a real guarantee for an
imaginary gain. The benchmarks are committed as the baseline the next person
argues against.

### D77. An awareness state is bounded, and so is the number of them
A cursor is a name, a colour and a couple of offsets - a few hundred bytes. The
frame limit is 16 MiB, so until now one client could publish a 16 MiB "cursor",
which this server held in memory, relayed to every peer in the room and published
to every replica. That was the cheapest amplification the server offered, and it
needed no permission beyond being allowed to connect. Found while writing the
project evaluation, not by a test.

`-awareness-max-state` (4 KiB) and `-awareness-max-clients` (1024) bound both
dimensions: the size of a state and how many clients one document tracks. Client
ids are chosen by the client, so without the second one connection could invent
millions of them.

Three details that took a second pass:

  - A removal is always accepted, whatever the limits say. Refusing one would
    leave a cursor on screen that its owner had retracted.
  - The cap counts *cursors*, not entries. A removed entry is a remembered clock
    ([D71]), and holding a slot for it would make a full room refuse newcomers
    for ten minutes after every departure - which the first version did, and a
    test caught.
  - Remembered clocks are themselves capped at twice the limit, oldest dropped
    first, because a client cycling through ids would otherwise grow the map for
    the whole ten-minute window.

Rejected: refusing the offending entry and continuing. The room already closes a
connection that sends an unusable awareness update, and a client publishing a
megabyte of cursor is one this server should not be carrying for.

### D78. A refused cursor closes the local connection and only counts a remote one
A local connection gets a permission-denied message and 1008, the same treatment
backpressure and read-only writes get: say which rule was broken, then close. An
oversized state that arrived over the cluster bus is counted and dropped instead.
There is nobody to disconnect there, and taking a replica down because a client
on the far side of the cluster misbehaved would spread the problem rather than
contain it.

### D79. Rate limiting slows a connection, it does not close it
A token bucket per connection, on two dimensions: messages per second and bytes
per second. They are two different floods - many small updates cost CPU per
message, a few large ones cost memory and fanout bandwidth - and a limit on one
alone leaves the other open.

Over the limit the read pump waits. It does not close the connection, because a
client that bursts is usually a client being used, and disconnecting it costs a
reconnect and a resync to fix something that resolves itself in a millisecond.
Waiting also puts the backpressure where it belongs: we stop reading, the socket
buffer fills, and the sender's own TCP stack slows it down. Only that connection
is affected - the limiter is per connection and the room never waits on it.

Defaults are on and generous: 200 messages a second and 8 MiB a second, against
the ten to thirty messages a second a person actually produces. They exist to
stop a client that has gone wrong, not to shape normal traffic. Zero means the
default and negative means no limit, so "unset" and "unlimited" are not the same
word.

Verified against a real server: ten clients pushing 60 messages a second each
into a 20/s limit were slowed to the limit with zero errors and zero
disconnections, and the throttle counters recorded 2,919 waits totalling 144
seconds.

### D80. 1011 is for a document we could not serve, not for a peer that left
That load run showed ten connections closing with 1011, "internal error",
because a write that failed after the client went away was being recorded that
way. The close code never reaches a peer that is already gone; what it reaches is
the metric, and "ten internal errors" every time ten people close a tab is how an
operator learns to ignore the graph. A failed write is now 1001. 1011 is left to
mean what it says: this server could not serve this document.

### D81. A hook is an observer, never a participant
Hocuspocus lets a hook be awaited, and so refuse or delay what the client did.
This one cannot. A document that stops accepting keystrokes because somebody's
webhook receiver is slow is a worse outcome than a missed notification, every
time, and the failure is remote, intermittent and invisible from the editor.

So `Emit` never blocks, the queue is bounded, and a full queue drops events and
counts them. Authorisation already has a place to live - the token, checked
before the connection is accepted ([D60]) - so the thing a blocking hook would
be for is answered somewhere better.

Rejected: an optional blocking mode for the deployments that want it. The knob
would be off in every test and on in production, which is the arrangement that
guarantees the failure is first seen by a user.

### D82. Events are coalesced onto the room's tick, not sent per update
A person typing produces tens of updates a second. A webhook per update is an
outage at the receiving end and a queue that is permanently full at this one.

The room therefore marks that something changed and emits at most one event of
each kind per tick. That costs an increment on the hot path, needs no second
timer per document, and produces an event that says "twelve updates since the
last one" rather than twelve events. A room with no hooks configured does none
of it, including the state-vector encode.

The consequence to know about: the delay between an edit and its webhook is
bounded by `-tick` (5 s by default), not by a knob of its own. Rejected: a
per-document debounce timer, which is a second timer per resident document for a
feature most deployments do not turn on, to buy latency that `-tick` already
buys.

### D83. Only local edits raise a change event
Every replica holding a document applies every edit, so the naive version turns
one keystroke into one webhook per replica. The event is raised where the client
is: an update that arrived on the cluster bus is applied and relayed but not
reported, because the replica whose client made it already reported it.

`document.store` is different and deliberately not deduplicated - it is raised
by whichever replica actually wrote rows, which is a fact about that replica.

A receiver still has to be idempotent. Two people editing the same document on
two replicas produce an event from each, both true, and a retried delivery
repeats one - which is why the delivery id is generated once per event and
reused across its retries, so a receiver can recognise the repeat.

### D84. The signature covers the timestamp, and a store event means the write happened
Two details that are the difference between a webhook and a webhook somebody can
build on.

The signature is `HMAC-SHA256(secret, "<unix>.<body>")`, sent as
`t=<unix>,v1=<hex>`. The timestamp is inside the signed text rather than beside
it, so a captured request cannot be replayed later with a fresh timestamp:
changing it breaks the signature. `hook.Verify` is exported and is what the
tests use, so the format has exactly one reader and one writer.

`document.store` is raised by the persist goroutine after `Append` returns
without an error, not by the room when it queues the write. The room learns
about it through an atomic counter it drains on its next tick. Its count is
logical updates in confirmed batches, not a physical row count: if PostgreSQL
committed but its reply was lost, a retry may leave duplicate idempotent Yjs
rows. A final unconfirmed write raises no store event, so a receiver must not
treat that event as a client-visible persisted acknowledgement.

### D85. A read is served by the room when there is one, and never starts one
`GET /documents/{name}` asks the resident room through its mailbox, like every
other command, so what it returns is exactly what the connected clients are
looking at - up to a flush interval ahead of the database. A document with no
room is read from the store directly.

Rejected: starting a room for the read, which is the obvious implementation and
the wrong one. It would hold the document in memory, join it to the cluster,
begin its idle timer and start it emitting hooks, all as a side effect of
somebody looking at it. A read should not change what the server is doing.

The cost is stated rather than hidden: in a cluster, an edit still sitting in
another replica's flush window is not visible to a read here. Making it exact
would mean a bus round trip on every read, on the chance that some other node
holds the document.

### D86. The state vector is sync metadata; ETags validate representations
The state vector is the version used by Yjs clients and anti-entropy to request
missing structs. It does not advance for a delete-set-only update, so it cannot
validate a GET response by itself. `ETag` hashes the selected response bytes:
the full binary update, a binary diff for `?sv=`, or the rendered JSON body. The
state vector header remains available on every response, including 304, and
continues to serve the sync protocol. `Vary: Accept` identifies the negotiated
JSON representation.

No new document revision counter is needed: the representation bytes already
provide a validator that works across replicas serving the same response.

### D87. The JSON view has no "type" field, because the wire format has none
The first version reported a type per root and a `rendered` flag. A test found
every root coming back as `unknown`, and the reason is in `doc.go`: **the v1
format never states a root type's kind.** `doc.getText('x')` and
`doc.getMap('x')` read the same bytes two ways, and Yjs decides at the moment
the client asks. A server that only ever saw updates cannot know which was
meant.

So the field is gone rather than guessed. Each root offers both readings - `text`
for the sequence, `keys` for the map - and one of them is empty. A guess would
have been right most of the time, which is worse than useless: it would be
believed.

The binary form remains the complete one. The JSON view is a convenience, and
for an XML root it shows a name and no content, because the engine decodes XML
and exposes no reader for it ([C4], [D8]).

### D88. Reading lives on the admin listener, not on the client port
Two reasons, and the second is the one that decides it.

The document name is the URL path on the client listener, so `ws://host/notes`
is document "notes". Any HTTP route added there is a document name somebody can
no longer use.

And there is no token that would authorise it. The tokens this server
understands are per-document capabilities minted for an editor ([D60]); "read
any document" is an operator power, and the admin listener is where operator
powers already live, next to `DELETE /documents/{name}`, with the same argument:
the deployment decides who can reach the port.

### D89. Restoring is POST and it merges, because the format cannot replace
A backup nobody can restore is not a backup, so `GET /documents/{name}` needed a
counterpart. It is POST rather than PUT, and the difference is not pedantry:
these are CRDT updates. Applying one adds what it carries to what is already
there, and the v1 format has no operation that makes a document forget - a
deletion is itself an update saying what was deleted. A PUT would promise a
replacement this server cannot perform.

So the method, the handler's name and the documentation all say *merge*, and the
runbook says to `DELETE` first when you want the backup rather than the union of
the backup and whatever the document has become.

A merge into a resident room goes through the mailbox like every other command,
is broadcast to every connection and published to the cluster. Clients that were
already connected have to hear about it, or they keep building on a version the
server no longer holds.

Rejected: refusing to merge into a document that has connections, the way DELETE
does. DELETE is destructive and a merge is not; refusing would mean a document
can only be restored when nobody is looking at it, which is the opposite of when
somebody notices it needs restoring.

### D90. A restore with no local room still publishes to the bus
When no room on this node holds the document, the update is appended to the log
directly. That is enough for the next load - and invisible to a replica that has
the document resident right now, which would not reread it until it evicted, and
would then write its own snapshot over the restore.

So `Import` publishes the update on the cluster bus as well, under this node's
id. Any replica holding the document applies it as an ordinary remote update.
There is no room here to filter it back, and there is nothing to loop.

Rejected: telling the operator to stop the cluster first. It is correct advice
for a whole-database restore, where the tables move underneath every replica at
once, and the runbook gives it there. For one document it would be a footgun
disguised as a procedure.

### D91. The runbook's commands were run, not written
Every command in `docs/RUNBOOK.md` was executed against the real containers
before it went in, and the backup section is a transcript. The one that mattered
was `pg_restore`: `doc_updates.seq` is `GENERATED ALWAYS AS IDENTITY` and the
primary key is `(doc_id, seq)`, so a restore that reset the sequence would make
the next write to every restored document collide. It does not - `max(seq)` and
the sequence's `last_value` were both 76 after the round trip, and the next
insert got 77 - but that is a fact worth checking rather than assuming, and the
runbook tells the reader to check it too.

The per-document backup and restore is not documented on trust either: it is
what `TestADocumentCanBeBackedUpAndRestored` performs on every CI run.

### D92. The admin listener gets a token, because it stopped being read-only
The original argument for no authentication was that the deployment decides who
can reach the port ([D65]), and it was sound while the surface was metrics,
stats and pprof. Then it grew `DELETE`, and then `POST` ([D89]), and now a
request that reaches it can rewrite any document on the server. Network
isolation is still the first control, but it became the *only* control on a
destructive surface, and one misconfigured Service or one forwarded port is the
whole distance between "internal" and "anybody". Found while scoring the
project, not by an incident.

`-admin-token` is a shared secret checked as a bearer credential, deliberately
not the JWT machinery: those tokens are per-document capabilities minted for
editors ([D60]), and "may administer this server" is not a document capability.
A shared secret is the right shape for a surface whose users are an operator, a
scrape job and a backup script.

Three details:

  - **Everything except `/healthz`.** A load balancer probing liveness cannot
    hold an operator credential, and the answer is one word that says nothing
    about the documents. It is registered on an outer mux rather than excepted
    inside the check, so "what is open" is one line to read instead of a
    condition to reason about. `/metrics` is *not* excepted - Prometheus can
    send a header, and pprof next door prints the command line the process was
    started with.
  - **Header only, never a query parameter.** A token in a URL ends up in
    access logs, browser history and the `Referer` of anything the response
    links to. The WebSocket endpoint accepts `?token=` because a browser cannot
    set a header on a WebSocket handshake; nothing on this listener has that
    excuse.
  - **Compared as SHA-256 digests.** `subtle.ConstantTimeCompare` returns early
    on a length mismatch, so comparing the raw strings would leak the secret's
    length. Digesting first makes every comparison fixed-width.

Rejected: refusing to start without a token. A laptop running the demo has
nothing to protect and the listener defaults to localhost, so it is a warning at
startup, the same treatment running without a signing secret gets ([D64]).

### D93. X-Forwarded-For is ignored unless a proxy is named
Nothing in this server read a peer address at all, so an incident had no answer
to "where is it coming from" and `-max-conns` was a limit one client could reach
alone.

The address has to be established carefully, because the header that usually
carries it is written by the client: `X-Forwarded-For` is a request header like
any other, and anyone can send one saying anything. Trusting it unconditionally
would let an attacker pick their own identity - and a per-address limit an
attacker can opt out of is *worse* than none, because it costs the work and
gives the false impression of a control.

So the header is read only when the machine on the other end of the socket is a
proxy the operator named in `-trusted-proxies`, and the address taken is the
**rightmost** entry that is not itself trusted: the last address a machine we
trust actually saw. Taking the leftmost is the classic mistake - that end of the
list is the part the client wrote. A hop that will not parse stops the walk,
because the chain cannot be reasoned about past it.

`loopback` and `private` are named shorthands for the two real deployments,
because the alternative is every operator writing out the same RFC 1918 blocks
and one of them getting it wrong. A malformed entry is an error at startup, not
a silently narrower set.

The address goes in the logs and never in a metric label: a label taken from a
peer address makes the endpoint's cardinality the number of addresses on the
internet ([D67]).

### D94. The per-address cap is off by default, unlike the rate limits
The rate limits ship on ([D79]) on the argument that a limit which only catches
a client that has gone wrong should not need configuring. The same argument does
not carry here, and the difference is NAT.

A rate limit is per connection, so its false-positive case is one misbehaving
client. A per-address cap is per *address*, and an office, a school or a mobile
carrier is one address to this server - a live default would be a live default
on how many colleagues may edit at once, breaking exactly the collaborative case
this server exists for. The deployment knows its clients; the Kubernetes
manifests set it to 100.

There is a matching footgun the server warns about: behind a load balancer with
no `-trusted-proxies`, every client looks like the load balancer, so the cap
would apply to the whole deployment at once.

### D95. A version is a whole document, not a diff
The obvious design is a chain of diffs, and it would be smaller. It was rejected
for two reasons that both bite at the moment somebody needs it: reading one
version becomes a walk through every version before it, and there is no way to
drop an old one - which is the only operation retention consists of. A history
you cannot prune is not a history, it is a leak.

So each row is a complete Yjs update, and restoring is one read. What keeps the
size honest is that a version is written only when its state vector differs from
the newest one, so a document nobody edited produces one row however long the
timer runs. `-version-keep` (24) bounds the rest.

Rejected also: Yjs's own `Y.snapshot`, which is a state vector plus a delete set
and reconstructs a past state with `createDocFromSnapshot`. It only works on a
document created with garbage collection off, and it would need that function
implemented in this engine - a large piece of work whose output would be the
same bytes this design stores directly.

### D96. The duplicate check is a condition on the insert, not a read then a write
Every replica holding a document runs its own version timer, so the naive
implementation produces one version per replica per interval.

`SaveVersion` is a single statement whose `WHERE NOT EXISTS` covers both the age
gate and the state-vector check. There is no window between deciding and
writing, so three replicas offering a version in the same second produce one row
- whichever gets there first - with no lock, no leader and no coordination.

The room keeps a `versionDirty` flag purely as an optimisation: an unchanged
document does not encode itself every interval only to have the store refuse it.
Correctness lives in the statement; the flag saves the 26 µs.

### D97. Restoring is DELETE then POST, and there is no restore endpoint
`POST /documents/{name}` merges ([D89]), and CRDT updates cannot remove what a
document has since gained. A `POST /versions/{id}/restore` would therefore hand
back the union of the old version and the damage somebody was trying to undo -
an endpoint whose name promises more than the format can deliver.

Two visible steps say what is actually happening. The runbook and the README
both spell them out, and `TestAVersionSurvivesSomebodyWreckingTheDocument` runs
them end to end.

### D98. Restoring into a document that never existed had to be made to work
Writing that test found a real defect. `doc_updates` has a foreign key to
`documents`, and the only thing that created a document row was `Load` - as a
side effect of reading. So `Import` appended to the log of a parent that did not
exist and returned 500.

The backup test written a session earlier passed anyway, because it happened to
`GET` the document between deleting and restoring it, and that read created the
row. The bug was invisible until a test skipped the read - and the case it broke
is restoring into an empty database, which is the disaster-recovery path.

`Store.Ensure` is now an explicit statement on the `Persistence` interface, and
`Import` calls it. The regression has its own test rather than staying covered
by accident.

### D99. Subdocuments needed checking, not building
Derived from source, as the brief requires, and the source is emphatic: the
string "subdoc" does not appear anywhere in `y-websocket`. `Y.Doc` emits a
`subdocs` event (`yjs/src/utils/Doc.js`) and the application opens a second
provider per subdocument, naming the room after the guid.

So a subdocument is an ordinary document to this server, and the sync path
already worked. The right deliverable was therefore evidence rather than code: a
fixture generated by real Yjs containing `ContentDoc` (ref 9, which the coverage
line now reports), and an end-to-end test that syncs a parent, reads its guids
back intact, and syncs the subdocument as its own room.

Rejected: inventing a server-side subdocument protocol - a parent connection
that multiplexes its children, or a room that opens its subdocuments' rooms.
Either would be this server speaking something `y-websocket` does not, which is
the one thing the brief rules out.

### D100. The parent names its subdocuments, so the server says so
`Doc.Subdocs()` reads the live `ContentDoc` guids, and the JSON view carries
them. It is a small thing that closes a real hole: a parent document is the
*only* place a subdocument is named, so without it nothing can know that
deleting "book" orphans "chapter-one", or that a backup of "book" is not a
backup of the book. Both are now stated in the README and the runbook rather
than left to be discovered.

Deleted references are excluded. A subdocument whose reference was removed is
not part of the document any more, and a list somebody deletes from must not
include it.

Rejected: making `DELETE` cascade to subdocuments. The server would have to read
the parent to find out what else to remove, and a delete that quietly destroys
documents the caller did not name is worse than one that leaves them. The guids
are one request away; the decision stays with whoever is deleting.

### D101. A token names one document, subdocuments included
With `-jwt-secret` set, a client needs a token per subdocument. That is the same
round trip the application already makes for the parent's token, repeated when
the `subdocs` event fires - and at that moment the app has the guid, because it
just received it.

Rejected: letting a parent's token cover its subdocuments. The server would have
to load the parent on every connection to check the guid is really referenced,
turning an authorisation check into a document read; and a token whose scope
grows as the document changes is not a scope anybody can reason about. A
capability should name what it grants ([D60]).

### D102. `INSERT ... WHERE NOT EXISTS` is not a lock
Found in a debugging pass, by doubting a claim I had made rather than by a
failure. D96 said the duplicate check on `SaveVersion` was safe between replicas
because it was "a condition on the insert rather than a read followed by a
write", so "there is no window between deciding and writing". That is wrong, and
the demonstration is two psql sessions against this schema:

    A: BEGIN; INSERT ... WHERE NOT EXISTS (...);   -- held open
    B:        INSERT ... WHERE NOT EXISTS (...);   -- while A is uncommitted
    -> two rows

Under READ COMMITTED each statement evaluates its subquery against its own
snapshot, neither sees the other's uncommitted row, and both insert.

The damage was not corruption but retention. Duplicates eat the budget, so
"keep the last 24" on a three-replica cluster silently becomes "keep the last
eight moments" - the history gets shorter exactly as the cluster gets bigger.

The fix is the advisory lock `Compact` already takes for the same reason (C2,
C6). The regression test does not try to win a race: it holds the document's
lock from outside and watches `SaveVersion` wait for it, so it fails
deterministically if the lock is ever removed by somebody tidying up a
transaction that appears to do nothing.

### D103. A forced shutdown has to abandon the queue, not drain it slowly
`Webhook.Close` took a deadline and did not honour it. On timeout it cancelled
the workers - but the request in flight was built from `context.Background`, so
cancelling did not abort it, and the workers then went on to drain the rest of
the queue, paying one request timeout per event.

Measured: a one-second deadline against a receiver that accepts and never
answers did not return in **two minutes**. The comment above the code said it
abandoned in-flight work; it did not.

This matters because of where it lands. In Kubernetes the process is SIGKILLed
at the end of `terminationGracePeriodSeconds`, so an overrunning Close turns
"the webhook endpoint is down" into "rolling deploys lose the flush window" -
a data-loss bug wearing an observability bug's clothes.

Requests are now built from the shutdown context, and workers stop taking new
events once it is cancelled. Same measurement afterwards: 1.01 s.

A note on method: the first evidence for this came from timing a real process
through `taskkill` and a `tasklist` polling loop, which measures the harness
more than the server. The number it produced was withdrawn and the defect
re-established with a unit test that fails without the fix and passes with it.

### D104. "No such document" and "no history is kept" are different answers
`POST /documents/{name}/versions` mapped both `ErrNoDocument` and
`ErrNoVersioning` to `503 this server keeps no version history`. So asking to
version a name that does not exist, on a server where versioning works
perfectly, told the operator their versioning was not configured. Confirmed
against a running server before it was changed; it is now `404`.

The general shape is worth keeping in mind: two errors joined in one `case` arm
because they were handled the same way, and the message could only be true for
one of them.

### D105. A read that skipped rows says so
`Fetch` applied stored updates and ignored the ones that would not apply, which
is right - one corrupt row must not make a document unopenable, and the room
does the same. But the room *logs* it and `Fetch` was silent, so a document read
over HTTP came back quietly short with no signal anywhere.

Whoever calls that endpoint is very possibly taking a backup, and a backup that
is missing a few updates and looks identical to a good one is the worst version
of this. The count now travels back on the snapshot, the handler logs it, and
the JSON view carries `skipped`.

### D106. The token path and the callback path are alternatives, not layers
`-jwt-secret` and `-auth-url` cannot both be set; the server refuses to start.

The tempting design is to layer them: verify the JWT here, and ask the endpoint
about the ones that pass. It reads like defence in depth and is not. The main
reason to want a callback is "this user lost access" — and a callback that is
only consulted about tokens this server already accepted cannot express that,
because the expired or revoked ones never reach it. Layering would also mean
answering "which one wins when they disagree", and every answer to that surprises
somebody at three in the morning.

Rejected alternative: make `-auth-url` a fallback consulted only when there is no
token. That is the same problem wearing a hat — it makes the *client* choose
which authority applies, by deciding whether to send a token.

The endpoint gets the token verbatim, so an application that wants JWTs verifies
them itself and keeps the revocation check. Nothing is lost.

### D107. Fail-open covers an outage, not a misconfiguration
`-auth-fail-open` lets everybody in while the authorisation endpoint is
unreachable. It deliberately does **not** apply when the endpoint answers a 404, a
redirect, a 204, or a 200 whose body has no `allow` field: those are refused
regardless.

The reason is what each failure means. A timeout or a 5xx is "the service is
down", which is an outage the operator chose to trade for availability. A 404 is
"you typed the URL wrong", and a 200 without `allow` is "your endpoint is not the
one this server was built to talk to". Under the naive reading — anything that is
not a clear yes goes through the fail-open switch — a typo in `-auth-url` produces
a server that lets the entire internet read and write every document while logging
a warning nobody is watching. The first deploy is exactly when that typo exists.

This is also why `allow` is decoded into a `*bool`. An absent field has to be
distinguishable from `false`; treating absent as either is a guess, and the guess
is wrong in a direction somebody finds out about later.

### D108. The authorisation cache is off by default, and the single flight is not
Two mechanisms, and only one of them is a default, because they trade different
things.

The single flight — identical questions in the air at once collapse into one
request — costs nothing. It changes no answer; it only stops fifty clients
reconnecting after a deploy from becoming fifty simultaneous requests to the
slowest thing in the connection path. Always on.

The cache does change answers: a remembered decision is a revocation that has not
taken effect yet. That is a real tradeoff and it is the operator's, so
`-auth-cache-ttl` defaults to `0`. An endpoint may return a *shorter* `ttl` to be
re-asked sooner; a longer one is capped, because how much staleness a deployment
tolerates is not the endpoint's call.

Rejected alternative: an LRU. Eviction here is a sweep of what expired and, only
if that frees nothing, a wholesale clear. The cache is a stampede guard with a
lifetime in seconds, not a hit-rate optimisation, and an LRU's bookkeeping would
sit on the connection path to protect a number nobody is tuning.

Three things are never remembered: a fail-open grant (the point is to leave that
state the moment the endpoint returns), a refusal caused by an outage (it is not a
decision about the token), and a client that walked away mid-question. The first
of those was a bug the test found — the comment said "not cached" and the code
cached it — which is why "may this be remembered" is now a field on the answer
rather than something the caller infers from a nil error.

### D109. One HMAC scheme, in one package
`internal/signature` was extracted from `internal/hook` when the authorisation
callback needed to sign its requests too. `hook.Sign` and `hook.Verify` remain as
one-line wrappers, because they are the names a webhook receiver is written
against and the README documents them.

The alternative was for `internal/auth` to import `internal/hook`, which puts a
webhook dependency in the authorisation path for the sake of eight lines of HMAC,
or to write those eight lines twice. Two implementations of a signing format are
two implementations that can drift, and the receiver only ever writes one verifier.

### D110. The audit trail is a stream, not a table
The obvious place to put an audit log, in a server that already has PostgreSQL,
is a table. It was rejected.

An audit trail's value is that it is harder to change than the thing it records.
A table in the same database that the audited surface can reach is not: the
credential that just deleted a document is a credential with access to the
database that holds the record of it. Making it genuinely append-only needs a
restricted role, a trigger, or a separate cluster — none of which this server can
arrange for the operator, and all of which it would be implying by shipping the
table.

A stream of JSON lines on **stdout** goes to whatever the deployment already
collects, which is somewhere else by construction. It also works with no
database at all, and needs no schema migration, no retention job and no read API.
The default is on, because an audit trail nobody enabled is an audit trail nobody
has on the day it is needed.

The one thing the table would have given is queryability, and that is the log
pipeline's job. The RUNBOOK's procedures are `jq` one-liners.

Rejected alternative: write it through `slog` alongside the process log. That
buries the interesting lines under debug output and forces every reader to filter
first. stdout versus stderr costs nothing and is already separate.

### D111. The audit record names the credential, and never the one that failed
A successful request records `credential`: the first eight hex digits of the
SHA-256 of the token used. A refused one records the literal `invalid`.

The asymmetry is the point. Fingerprinting the token that was *presented* on a
failure would turn the trail into an oracle: somebody who can read the log — a
wider set than those who hold the token, since the log gets shipped — could
confirm guesses against it offline. There is nothing to learn from a failed
attempt's fingerprint that the `denied` result and the source address do not
already say.

Four bytes on the success side is short deliberately. Its job is telling a
handful of tokens apart during a rotation, not resisting somebody holding both the
log and a guess. A longer prefix buys no more distinguishing power for that job
and offers more for the other one.

The limit worth stating: with one shared `-admin-token` this says *which
credential*, not which person. That is what the server actually knows, and the
README says so rather than implying otherwise.

### D112. `-admin-token` takes a list, because rotating one secret has no safe order
It used to be one string. With one, every rotation has a window: remove the old
token before its holders are updated and the backup script breaks; add the new one
after and there is a moment where nothing works.

`-admin-token a,b` follows what `-jwt-secret` already does. The step that was
missing is the last one — knowing when the old token has actually stopped being
used — and that is what D111's fingerprint answers. The two features are one
feature.

Comparison stays constant-time and does not stop at the first match. Stopping
early would leak only the position in a list that is not secret, but a check that
is constant-time except for the part somebody added later is the usual way this
goes wrong.

### D113. Auditing wraps each route, not the mux
`http.ServeMux` fills in a pattern's wildcards as part of matching, so
`r.PathValue("name")` is populated for a handler registered *as* a route and empty
for a wrapper sitting outside the mux. A single outer layer would therefore have
had to parse `/documents/{name}/versions/{id}` a second time, in a copy of the
routing table that could drift from the real one.

Wrapping per route costs one call per registration and gets the document name from
the router that already worked it out.

The gap that leaves — a request matching no route at all — was real, and a test
found it: an authorised caller poking at paths this server does not serve was
invisible. A catch-all `/` handler, audited as `unknown`, closes it. `ServeMux`
prefers the more specific pattern, so every real route still wins.

### D114. A document belongs to whoever opened it first, and never changes hands by itself
The alternative designs both have a hole. Assigning an owner only when a token
carries one, and treating an unowned document as open to anybody, leaves a
permanent gap: every row written before tenancy existed stays reachable by every
tenant forever. Letting the first tenant to *open* an existing unowned document
claim it turns a race into an ownership decision - whoever connects first wins,
including somebody who guessed the name.

So: creating a document stamps it, and nothing else ever changes the owner except
an operator saying so through `PUT /documents/{name}/owner`. A deployment turning
tenancy on finds its existing documents owned by nobody, reachable by exactly the
tokens that already worked, and moves them deliberately.

`NilUUID` is therefore a real owner rather than a placeholder, and this is the
half people get wrong: a connection claiming no owner reaches only documents that
have none, and a tenant does *not* inherit the documents that have none. Both
directions are tested, because only enforcing the first would make an old
tokenless connection a skeleton key.

### D115. The tenancy check is settled once per document, not once per connection
The obvious implementation asks the database "whose is this" on every connection.
That is a round trip per reconnect, on the path a client waits on, for an answer
that cannot change while the document is open.

Instead the room holds the owner, settled when the document was opened. A
resident document answers from a map lookup; only opening one costs a query. The
comparison is over the derived owner id rather than the tenant name it came from,
because the same tenant may be named by its slug in one token and by its UUID in
another.

That is also why `PUT .../owner` evicts the room first and refuses when somebody
is connected. A room that kept running with a stale owner would apply the change
on other replicas immediately and on this one whenever it happened to fall out of
memory, and would leave the connected clients attached to a room whose owner no
longer matches their grant.

### D116. Ownership is decided by INSERT ... ON CONFLICT and a read, in one transaction
Read-then-insert is wrong here for the reason D102 established: under READ
COMMITTED two transactions each evaluate the read against their own snapshot,
neither sees the other's uncommitted row, and both insert. There it cost a
retention count. Here it would be two tenants each believing they own the same
document.

`INSERT ... ON CONFLICT DO NOTHING` followed by `SELECT owner_id`, in one
transaction, has no such window: whoever loses the insert reads the winner's row.
A test runs twelve goroutines at one new document against a real PostgreSQL and
asserts they all report the same owner.

### D117. The documents table needed a name column before a listing was worth anything
`DocumentID` hashes a name into a UUID, one way. A listing built from the existing
schema could return identifiers and nothing anybody could open, which is not a
listing.

The column is additive and `Migrate` runs `ALTER TABLE ... ADD COLUMN IF NOT
EXISTS` on every boot, so an existing database gets it in place - verified against
one carrying rows from earlier phases. Those rows keep an empty name: the id is a
hash, so the server cannot recover what it was called. `PUT .../owner` records the
name as a side effect, because that is the moment somebody is looking at the
document and knows what it is called.

The name is written on creation and never rewritten. Overwriting it on every open
would mean taking a caller's word over the row's about which document this is.

### D118. Paging is keyset, and the cursor carries the id as well as the name
`OFFSET` skips and repeats rows when the underlying set changes, and a tenant's
documents change while somebody pages through them. That matters more than usual
here, because paging this listing is how "delete everything belonging to this
account" is carried out - a skipped row is data that should have been deleted and
was not.

The cursor is `(name, id)`, not name alone: rows written before the name column
have an empty name, and a cursor on name alone would either skip all but one of
them or loop forever on the first.

### D119. A read on the admin surface must not create the document row
`Load` creates the row it reads, because opening a document is how a document
comes to exist. Carrying that into the administrative read would have been a real
defect: an operator `curl` of a name nothing had written would leave behind a row
owned by nobody, and the tenant who later opened that name would be refused their
own document forever.

`LoadAny` therefore neither checks the owner nor creates the row. Two behaviours
in one method is usually a smell; here it is one idea - "look, do not touch" - and
it is a separate method rather than a flag on `Load` so that reading past the
tenancy boundary is a call somebody can grep for, and so no call site can do it by
leaving a parameter at its zero value.

### D120. The refusal is the same one a bad token gets
A client refused because a document belongs to another tenant is told exactly what
a client with no token is told, in the same words, with the same close code.

Distinguishing them would turn the boundary into an oracle: a caller could ask
"does a document called X exist somewhere on this server" one guess at a time, and
document names are often meaningful - a customer name, a project, a deal. The
information the operator needs is in the log line and the metric, where the client
cannot see it.

The hole this had, found by a test: the check lived on the non-resident restore
path only, so a cross-tenant `POST /documents/{name}` succeeded for exactly the
documents somebody was editing at the time. Enforcement now sits on both paths.

### D121. Compression is worth about 2x, which is less than I claimed before measuring
I estimated three to five times. The measurement says **1.97x to 2.70x**, and a
live server storing five real corpora reported 2.13x. The estimate was wrong and
the number in the README is the measured one.

The first corpus I generated reported 64x, which is what put me on to it: it typed
the same twenty-five words in a loop, so what deflate was compressing was my
generator rather than a document. `tools/fixturegen/corpus.mjs` now builds five
cases that bracket the truth - natural English prose from this repository's own
documentation at one end, words drawn from a four-thousand-entry vocabulary at the
other - and it is committed so the number can be rechecked rather than believed.

Two times is still worth having: history is twenty-four whole documents per
document, and halving the largest thing in the database costs nothing on the hot
path, because snapshots are written at compaction and versions on a timer.

The reason it is only two times is worth knowing: a Yjs update is mostly
varint-encoded client ids and clocks, which are high entropy by construction. The
text is a minority of the bytes. Anybody quoting a much larger figure for a CRDT
store is measuring their test data.

`flate.BestCompression` was measured against `DefaultCompression` on every corpus
and produced **identical** ratios for up to five times the CPU, so the default
stands - on measurement rather than on the usual assumption that the default is
fine.

Rejected: zstd, which would be a dependency for a ratio that decides nothing here.
Rejected: gzip, whose ten-byte header would restate per blob what the codec column
already says.

### D122. The codec is a column, not a prefix byte
The obvious encoding is a marker byte at the front of the blob. It does not work.
Every row written before compression existed is a bare Yjs update, and a Yjs
update begins with a varUint that can hold any value - including whichever byte we
picked as the marker. There is no prefix that could not also be a legitimate
document, so reading the old rows would be a guess.

A column cannot be ambiguous, its default of `0` already means "raw", and it makes
"how much of this database is still uncompressed" a question SQL can answer during
a migration. The cost is two `ALTER TABLE` statements, which the schema file
already needed for the name column.

An unknown codec is an error rather than a fallback. That is what a rollback to an
older binary looks like, and reading those bytes as something else would produce a
document that is wrong instead of one that is missing.

### D123. Storing must never make a payload larger
`Pack` compresses, compares, and keeps the raw bytes when compression did not
help. Without that guard the test shows 511 random bytes becoming 521 - deflate
genuinely grows incompressible input, and a CRDT update full of ids is not an
exotic case.

A storage layer that can grow a document by storing it is a storage layer with a
surprising bill, and the surprise would land on exactly the documents that are
already the most expensive.

### D124. SigV4 is written out, and what that costs is stated rather than discovered
Asked and answered: hand-rolled, no dependency. `go.mod` keeps six direct
requirements instead of gaining four and about fifteen indirect ones, which is in
character for a project whose CRDT engine, lib0 codec and UUID handling are all
written out.

The surface actually needed is four requests. The signing is about 200 lines and
the client about 250. It was accepted by a real MinIO on the first attempt,
including keys containing spaces, unicode, `%25`, `&` and `?` - and a test signs
with a deliberately wrong secret to prove MinIO is checking rather than waving
things through.

The failure mode is what makes this defensible. A signature that is wrong is
refused with 403 by every request, immediately. It cannot be subtly wrong in a way
that works today and leaks tomorrow.

What it costs, in the README rather than in a footnote: no IRSA, no EC2 instance
roles, no SSO, no shared config file, no multipart upload, no presigning.
Credentials come from flags or the three environment variables every S3 tool
honours. A deployment that needs an instance role needs an SDK, and the `Blobs`
interface is three methods wide so that swap is small.

Rejected: `aws-sdk-go-v2`, for the dependency weight. Rejected: `minio-go`, which
was the reasonable middle and would have been the choice if the surface were
larger than four verbs.

### D125. Object first then row on the way in, row first then object on the way out
This is the whole correctness argument for splitting a document across two
systems, and it is one sentence: **an orphan is a bill, a dangling pointer is data
loss.**

Writing the row first and the object second would mean a failure in between
leaves a row naming bytes that are not there - a document that cannot be read,
found later by whoever tries. Writing the object first means the same failure
leaves an object nothing points at, which costs storage and nothing else.

Deleting is the mirror. Row first: a failure leaves an orphan. Object first: a
failure leaves a version that has quietly become unreadable.

Both directions are tested by making a put fail on demand, and both were checked
by reversing them and watching the tests go red.

Orphans are therefore possible by design. Snapshot attempts use unique object
keys, so a failed or ambiguous commit can leave an unreferenced object without
overwriting the object named by a committed row. Orphan count is not bounded by
the sequence number and cleanup remains operational work.

### D126. Each snapshot attempt has its own immutable object key
The obvious key is `snapshots/<document>`, overwritten each time. It is wrong as
soon as there are two replicas.

Compaction serializes database replacement and uses an owner, immutable document
incarnation, and monotonic snapshot generation compare-and-swap ([C2], [C6]).
Generation is separate from `snapshot_seq`: a late lower-seq row can require a
replacement at the same coverage watermark. The replacement's `snapshot_seq`
is the maximum of its prior value and the exact rows folded by that snapshot.

Every attempt uses a unique immutable key. A stale compare-and-swap is known not
to have named its candidate object, so that object can be dropped. If commit
returns an ambiguous error, the candidate is retained because the database may
have committed a row that references it. Such an object can become an orphan;
cleanup is operational work.

The superseded object is deleted after the commit, never before: a crash in
between leaves an orphan, and the other order would leave the live row naming
bytes that had already gone.

### D127. Where the bytes are is a column, not a server-wide mode
`snapshot_key` and `blob_key` are empty when the bytes are in the column beside
them and set when they are an object. That is per row, so one database holds both.

A mode switch would have meant a migration before object storage could be turned
on, and a second one to turn it off - which is exactly the shape of change nobody
wants to attempt on a database holding customer documents. As it stands, turning
it on changes where the *next* write goes and nothing else, and the rows written
before are read by the same query that reads the new ones.

The one case that must not be silent: a server with no bucket configured meeting a
row that names one. That is an error saying so, because the alternative - an empty
document - reads exactly like a document somebody had emptied.

### D128. The memory bound is bytes, and the y-redis rewrite was not the answer
I had this listed as the architectural change - stateless workers, documents in
Redis streams, no resident document, the shape y-redis has. Having got here, I do
not think that is the right work, and the reason is worth writing down.

That design does not remove the memory. It relocates it into Redis and puts a
network round trip on the update path, and it makes Redis a hard dependency of
*reading a document* - where today it is fanout only, and losing it degrades to
"replicas cannot see each other" rather than "nothing loads". For a server whose
apply time is 630 ns because the document is in memory, that is a large price for
a problem that turns out to be mostly an accounting failure.

The accounting failure: the only bound was `-max-rooms`, a count. An operator
sizing a pod writes bytes; a count of documents tells them nothing, and it makes
eviction pick badly - the least recently used tiny document goes while a huge one
nobody has touched stays, because they weigh the same to a counter.

So `-max-memory`, measured and enforced. What it does *not* do is stated in the
README rather than implied: it is not a hard guarantee, because a room with
somebody in it is never evicted, and it does not lift the ceiling, because a
replica serving a document holds all of it. Raising the ceiling is consistent
routing at the ingress, which needs no code here at all.

### D129. The estimate is arithmetic, and it was checked against real memory
A memory budget is only worth having if the number it spends is close to what the
process actually allocates. So the estimate is built from `unsafe.Sizeof` on the
item and content headers plus what those headers point at, rather than a
structs-times-a-constant guess.

Measured against `runtime.MemStats` across the corpora: **0.91 to 1.00** of real
heap growth. A test asserts it stays between 0.5x and 2.5x, which is deliberately
loose - it guards against the estimate becoming decorative, not against the
allocator having overhead.

It is a floor and says so: it does not model size classes, map buckets, or spans
the collector is holding. The README tells operators to leave headroom rather
than pretending otherwise.

`Content` was not widened with a `Bytes()` method for this. That interface is the
wire contract - every type in it exists because the format has a ref for it - and
a question about heap layout does not belong there. A type switch in one file
does the job and keeps the two concerns apart.

### D130. The budget is enforced on a timer, not only when a document is opened
The first implementation checked the budget on the join path, which is where the
room-count cap is checked. A test caught that this bounds nothing: it only fires
when a *new* document is opened, and documents grow. A server with a stable set
of rooms and people typing into them sails past the budget and is never asked
about it again.

So there is a sweep on the same interval the rooms re-measure on. The join-path
check stays, because it is free and it keeps a burst of new documents from
overshooting between sweeps.

A second defect, found in the field rather than by a test: a room measures itself
when it loads, which for a new document is a measurement of nothing - and the
interval then blocked re-measuring for fifteen seconds. Twenty rooms under load
reported zero bytes. The interval now throttles re-measuring a weight that is
already known, and never blocks learning one.

### D134. The image is published on tags only, and it can say which build it is
A `latest` that moves whenever `main` moves is an image nobody can pin, and
"which build broke it" stops having an answer. So the release workflow triggers
on `v*` tags; the branch gets a `main` tag for anyone who wants the tip on
purpose, and the versioned tags are never rewritten.

It re-runs the tests rather than depending on the CI workflow having passed.
Workflows triggered by different events do not gate each other - a tag push runs
the release and nothing else - so without that step a release could ship a build
no check ever saw.

`-version` and a stamped binary came out of the same thought. An image that
cannot answer "which build is this" cannot be correlated with an incident, and
the answer has to survive being copied into a `docker run` months later. The
version is passed in as a build argument rather than derived, because the `.git`
directory is deliberately not in the build context; an unstamped `go build`
falls back to the VCS revision the toolchain records anyway, so a local build
still says something true. The last step of the workflow pulls the published
image back and asks it - a tag pointing at a build stamped with a different
version is exactly the failure an operator would otherwise find months later
while reading a log.

`--platform=$BUILDPLATFORM` on the build stage is not decoration either. Without
it, buildx emulates an arm64 toolchain under QEMU to produce the arm64 image;
with it, Go cross-compiles natively from `GOARCH`. Same output, minutes instead
of tens of minutes, and arm64 is what an Apple laptop and a Graviton instance
both are.

### D133. A missing bucket is its own error, because 404 means two things
Found by accident: the tests were run against a MinIO whose bucket had been
wiped by its tmpfs, and every failure read `blob: no such object`. The bucket did
not exist. S3 answers both cases with a 404 and only the XML body says which,
and the client was not reading it.

The message was the small half. `ErrNotFound` is *acted on*: `Delete` treats a
missing object as success, because the caller wanted it gone and it is gone. So
a mistyped `-s3-bucket` meant every write failed loudly and **every delete
reported success**, forever, while nothing was ever removed. That is the shape
of bug that is found a year later by a storage bill.

`ErrNoBucket` is now distinct, the message names the bucket rather than a key
nobody asked about, and `Delete` only swallows a 404 that is actually a missing
object. Both guards were broken to check the test goes red; the delete one fails
with `err = <nil>, want ErrNoBucket`, which is the silent success stated plainly.

`Exists` still cannot tell the two apart, and says so where it is defined:
it uses HEAD, S3 answers HEAD with no body, and the body is the only thing that
carries the code. Nothing that decides to delete something uses `Exists`, which
is what makes that acceptable rather than a second instance of the same bug.

### D132. The client package wraps three gaps, and refuses to wrap anything else
A client library for a server that is already plain `y-websocket` is a bad idea
by default: it adds a dependency, a version to keep in step, and a layer between
the application and a provider that works. The brief for it was "smooth the
integration story", which is how wrappers that exist to exist get written.

So the package is scoped to what reading `y-websocket`'s source says the caller
*cannot* do, not to what looks nicer:

- `permissionDeniedHandler` is a module-level `const` calling `console.warn`
  (`y-websocket.js:105`). It is not an option and cannot be replaced. An
  application has no way to learn that the server refused it, or why.
- The provider reconnects forever with the parameters it was constructed with,
  so an expiring token turns into a silent 1008 loop an hour after page load.
- Subdocuments are announced by guid through an event and connected by nobody.
  Each is a separate document to the server, so each needs its own token, which
  is why the token source takes a name.

Everything else is delegated, and `client.provider` is public so that being in
the way is never the answer.

Two things fell out of reading the server rather than the client. Authorisation
happens at the WebSocket upgrade and never again, so a token expiring under an
open connection costs nothing - refreshing therefore writes `provider.params`
(documented as safely updatable, re-read by `get url()` on every connect) and
leaves the socket alone. A test asserts the socket object is unchanged across a
refresh, because the obvious implementation - reconnect with a new token - would
have dropped every editor's connection once an hour for no reason.

And a 1008 does not say whether the token is stale or the caller is unwelcome.
One retry with a freshly minted token separates them: refused twice is about the
caller, and further retries are noise on somebody else's server. Rejected: a
retry budget with backoff, which is a lot of machinery for a question that one
extra attempt answers exactly.

### D131. MIT, to match the ecosystem this server has to live in
Until now there was no LICENSE file at all, which does not mean "take it" - it
means the opposite. Without a licence the default is exclusive copyright: anyone
who found this could read it and nothing else. Three days of work sitting on one
disk under a rule that forbids its own use is not a licensing position, it is an
omission.

MIT rather than Apache-2.0. Apache is the better licence on the merits - it
grants patent rights explicitly, which MIT only arguably does by implication -
but this server exists to be dropped in beside `yjs`, `y-protocols`,
`y-websocket` and `lib0`, all of which are MIT. Matching them means nobody
integrating has to reason about two licences, and the one real advantage Apache
has is a patent grant from an author who holds no patents. Chosen for the
ecosystem, not because the difference does not exist.

The copyright line carries the name git is configured with. Anyone who wants
their legal name on it should edit one line before this is published anywhere.

### D33. The close frame goes out before the reader is unblocked
Found by running the gateway tests under `-race`, which turned an occasional flake into a
consistent failure: a connection the room closed with 1008 or 1002 arrived at the client as an
abrupt disconnect with no code. Cause: closing a connection cancelled the read context, and
`coder/websocket` tears a connection down immediately when its read context is cancelled - so
the write pump lost the race to send the close frame. The write pump now owns the whole
sequence: write the close frame, then cancel the reader, then signal the handler, which waits
before returning so the hijacked socket is not torn down underneath it. Without this the close
code is decorative, and the backpressure policy (D29) depends on the client being told why it
was disconnected.

### D32. The Go directive moved to 1.23
`github.com/coder/websocket` requires it. The brief asks for Go 1.22+, so this is inside the
constraint; noting it because it was a side effect of `go get`, not a decision made on purpose.

### D135. Documentation now references measured numbers, not estimates
README, RUNBOOK and QUICKSTART previously quoted a single throughput figure
from an early laptop run. Follow-up commits replace those with the measured
ranges across corpora and clarify which numbers are shape versus promise. The
intent is that an operator sizing from the docs gets a range rather than a
point estimate that was never re-measured.

---

## Part 2 — The wire format, derived from source

### 2.1 lib0 primitives

| Primitive | Encoder | Decoder |
|---|---|---|
| `uint8` | `lib0/encoding.js:174` `writeUint8` | `lib0/decoding.js:146` `readUint8` |
| `varUint` | `lib0/encoding.js:260` | `lib0/decoding.js:245` |
| `varInt` | `lib0/encoding.js:277` | `lib0/decoding.js:277` |
| `varString` | `lib0/encoding.js:344` | `lib0/decoding.js:390` |
| `varUint8Array` | `lib0/encoding.js:434` | `lib0/decoding.js:122` |
| `any` | `lib0/encoding.js:544` `writeAny` | `lib0/decoding.js:501` `readAny` |

Bit constants (`lib0/binary.js:20,21,58,59`): `BIT7 = 0x40`, `BIT8 = 0x80`, `BITS6 = 0x3F`,
`BITS7 = 0x7F`.

**varUint** — plain LEB128-style base-128, little-endian groups, high bit = continuation:

```
while num > 127: emit(0x80 | (num & 0x7F)); num = floor(num / 128)
emit(num & 0x7F)
```

The decoder accumulates with multiplication (`num + (r & 0x7F) * mult`, `mult *= 128`), not
shifts, so values above 32 bits are supported up to `Number.MAX_SAFE_INTEGER` (2^53 − 1);
beyond that it throws (`decoding.js:258`). **Go: use `uint64` and return an error above
2^53 − 1**, so we fail the same way Yjs does instead of producing a value no browser can read.

**varInt** — this is the trap named in section 9. The first byte is laid out differently from
every following byte:

```
first byte:  C S V V V V V V     C = 0x80 continuation, S = 0x40 sign, V = 6 value bits
later bytes: C V V V V V V V     C = 0x80 continuation,               V = 7 value bits
```

From `encoding.js:277-291`:

```js
write((num > 63 ? 0x80 : 0) | (isNegative ? 0x40 : 0) | (63 & num)); num = floor(num / 64)
while (num > 0) { write((num > 127 ? 0x80 : 0) | (127 & num)); num = floor(num / 128) }
```

So the multiplier sequence on decode is 1, 64, 64·128, 64·128², … (`decoding.js:280`
`mult = 64`, then `mult *= 128`). The sign is **not** two's complement and **not** zigzag: the
magnitude is written unsigned and the sign lives in bit 6 of byte 0 only. Negative zero is
representable (`isNegative` comes from `lib0/math.js:60` `isNegativeZero`, true for `-0`), and
decodes to `-0`; Go should decode it as `0`.

**varString** = `varUint(byteLength of UTF-8)` followed by the UTF-8 bytes
(`decoding.js:377` decodes `readVarUint8Array` as UTF-8). Note the length is in **bytes**, not
code points.

**varUint8Array** = `varUint(len)` + raw bytes.

**any** (`writeAny`) — a one-byte type tag then the payload. Tags descend from 127 (the
comment table is at `encoding.js:516-531`); `readAny` indexes a lookup table with `127 - tag`
(`decoding.js:501`).

| Tag | Type | Payload |
|---|---|---|
| 127 | undefined | — |
| 126 | null | — |
| 125 | integer | `varInt` (only if integral and \|n\| ≤ 2^31−1) |
| 124 | float32 | 4 bytes, big-endian (`setFloat32(0, num, false)`) |
| 123 | float64 | 8 bytes, big-endian |
| 122 | bigint | 8 bytes, big-endian, signed |
| 121 | false | — |
| 120 | true | — |
| 119 | string | `varString` |
| 118 | object | `varUint(numKeys)` then `varString(key) any(value)` pairs, in `Object.keys` order |
| 117 | array | `varUint(len)` then `len` × `any` |
| 116 | Uint8Array | `varUint8Array` |

Floats are big-endian while every varint is little-endian — easy to get backwards.

### 2.2 Update v1 grammar

Produced by `writeStateAsUpdate` (`yjs/src/utils/encoding.js:504`) = `writeClientsStructs`
(`:81`) followed by `writeDeleteSet` (`yjs/src/utils/DeleteSet.js:222`).

```
Update      := StructsSection DeleteSet
StructsSection := varUint(numClients) ClientBlock*
ClientBlock := varUint(numStructs) varUint(client) varUint(startClock) Struct*
Struct      := info:uint8 StructBody
DeleteSet   := varUint(numClients) DsClientBlock*
DsClientBlock := varUint(client) varUint(numRanges) (varUint(clock) varUint(len))*
```

Ordering rules that must be reproduced for byte-identical output:

- Client blocks are sorted by client id **descending** (`encoding.js:99`
  `.sort((a, b) => b[0] - a[0])`, with the source comment "Write items with higher client ids
  first. This heavily improves the conflict algorithm"). Same for the delete set
  (`DeleteSet.js:227`) and for state vectors (`encoding.js:603`).
- Within a client block, structs are in ascending clock order, contiguous.
- `writeStructs` (`encoding.js:57`) writes `structs.length - startIndex` as the count, then the
  client, then the **clock the block starts at**, and writes the first struct with an
  offset — see 2.3.
- `writeClientsStructs` skips clients where the target already knows everything
  (`getState(store, client) > clock`), and treats clients absent from the target state vector
  as clock 0. This is exactly the `SyncStep2` / `DiffUpdate` computation.

### 2.3 The info byte and item body

`Item.write` (`yjs/src/structs/Item.js:655`):

```js
info = (content.getRef() & 0x1F)
     | (origin      === null ? 0 : 0x80)   // BIT8
     | (rightOrigin === null ? 0 : 0x40)   // BIT7
     | (parentSub   === null ? 0 : 0x20)   // BIT6
```

| Bits | Meaning |
|---|---|
| 0–4 (`0x1F`) | content ref (or `0` = GC, `10` = Skip — see 2.4) |
| 5 (`0x20`) | `parentSub` is non-null |
| 6 (`0x40`) | `rightOrigin` is present |
| 7 (`0x80`) | `origin` is present |

Body, in exactly this order (write side `Item.js:663-697`, read side
`encoding.js:144-164`):

1. if `info & 0x80`: origin — `varUint(client) varUint(clock)`
2. if `info & 0x40`: rightOrigin — `varUint(client) varUint(clock)`
3. if **neither** 0x80 nor 0x40 (`cantCopyParentInfo`): parent —
   `varUint(parentInfo)` where 1 means "root type" and is followed by `varString(key)`,
   and 0 means "child of an item" and is followed by `varUint(client) varUint(clock)`
4. if `cantCopyParentInfo` **and** `info & 0x20`: `varString(parentSub)`
5. content, per the ref table in 2.4

**Trap:** `parentSub` is flagged in the info byte whenever it is non-null, but is only written
when the item has neither origin nor rightOrigin. When it is not on the wire the reader
inherits parent and `parentSub` from the resolved left/right neighbour
(`Item.js:397-403`). A decoder that unconditionally reads a string when bit 5 is set will
desynchronise the byte stream on the very first map update with a left neighbour.

**Trap:** when a client block starts mid-struct, `writeStructs` passes
`offset = clock - firstStruct.id.clock` and `Item.write` substitutes
`origin = ID(this.id.client, this.id.clock + offset - 1)` (`Item.js:656`), i.e. a
self-referencing origin, and the content is written sliced (`ContentString.js:95`
`str.slice(offset)`, `ContentDeleted.js:83` `len - offset`, `ContentAny.js:88`). The encoder,
not the decoder, does the splitting.

### 2.4 Content refs

`readItemContent` dispatches on `info & 0x1F` through `contentRefs` (`Item.js:705,712`).

| Ref | Content | V1 encoding | Source |
|---|---|---|---|
| 0 | *GC struct* | `varUint(len)` | `GC.js:7,47` |
| 1 | Deleted | `varUint(len)` | `ContentDeleted.js:82` |
| 2 | JSON *(legacy)* | `varUint(len)` then `len` × `varString` (`JSON.stringify`, literal `"undefined"`) | `ContentJSON.js:83` |
| 3 | Binary | `varUint8Array` | `ContentBinary.js:76` |
| 4 | String | `varString` | `ContentString.js:94` |
| 5 | Embed | `varString` (`JSON.stringify`) | `ContentEmbed.js:79` |
| 6 | Format | `varString(key)` `varString(JSON value)` | `ContentFormat.js:87` |
| 7 | Type | `varUint(typeRef)` + type extras | `ContentType.js:153` |
| 8 | Any | `varUint(len)` then `len` × `any` | `ContentAny.js:86` |
| 9 | Doc | `varString(guid)` `any(opts)` | `ContentDoc.js:121` |
| 10 | *Skip struct* | `varUint(len)` | `Skip.js:8,45` |

Refs 0 and 10 are **struct kinds, not content**: they occupy the same 5 bits but the reader
branches on them before constructing an `Item` (`encoding.js:130-143`). `Skip` is written with
a plain `varUint` rather than `writeLen` — identical in v1, deliberately different in v2
(`Skip.js:47` comment).

Type refs for ref 7 (`ContentType.js:18-33`):

| typeRef | Type | Extra |
|---|---|---|
| 0 | `YArray` | — |
| 1 | `YMap` | — |
| 2 | `YText` | — |
| 3 | `YXmlElement` | `varString(nodeName)` (`YXmlElement.js:248`) |
| 4 | `YXmlFragment` | — |
| 5 | `YXmlHook` | `varString(hookName)` (`YXmlHook.js:84`) |
| 6 | `YXmlText` | — |

In V1 all of `writeLen`, `writeTypeRef`, `writeClient`, `writeParentInfo` are just `varUint`,
`writeString`/`writeKey` are `varString`, `writeInfo` is a raw `uint8`, `writeBuf` is
`varUint8Array`, `writeJSON` is `varString(JSON.stringify(v))`, `writeAny` is lib0 `any`
(`UpdateEncoder.js:36-125`; decoder mirror at `UpdateDecoder.js:33-119`). Note `writeJSON`
(refs 5, 6) and `writeAny` (ref 8) are *different* encodings — embeds and formats are JSON
strings, map values are lib0-any.

### 2.5 State vector

`writeStateVector` (`encoding.js:601`) / `readStateVector` (`:565`):

```
StateVector := varUint(numClients) (varUint(client) varUint(clock))*
```

`clock` is the **next expected** clock, i.e. one past the last integrated struct. Clients are
written in descending id order. An empty document encodes as the single byte `0x00`, which is
also the default `encodedTargetStateVector` in `encodeStateAsUpdateV2` (`:522`).

### 2.6 Delete set

`writeDeleteSet` (`DeleteSet.js:222`) / `readDeleteSet` (`:248`). Ranges are built by
`createDeleteSetFromStructStore` (`:188`), which merges adjacent deleted structs into one
`(clock, len)` range per run. In v1 `writeDsClock` / `writeDsLen` are plain `varUint`
(`UpdateEncoder.js:24,31`); v2 delta-encodes them against a running value — another reason to
implement v1 only.

Applying a delete set splits items at range boundaries (`readAndApplyDeleteSet`, `:278`) and
records deletes for clocks it has not seen yet in `pendingDs`.

### 2.7 Worked example

`testdata/fixtures/text-insert-single/state.bin`, 19 bytes — one client (1001) inserting
`"Hello"` into root type `text`:

```
01                    numClients = 1
01                    numStructs = 1
e9 07                 client = 1001          (0xe9 & 0x7f) | (0x07 << 7) = 105 + 896
00                    startClock = 0
04                    info: ref 4 (ContentString), no origin/rightOrigin/parentSub
01                    parentInfo = 1 -> root type follows
04 74 65 78 74        varString "text"
05 48 65 6c 6c 6f     varString "Hello"
00                    delete set: 0 clients
```

`sv.bin`, 4 bytes: `01 e9 07 05` — one client, 1001, next clock 5.

`msg-sync-step1.bin`, 7 bytes: `00 00 04 01 e9 07 05` — outer `messageSync`, then
`messageYjsSyncStep1`, then the state vector as a `varUint8Array`.

### 2.8 Sync protocol and framing

Two layers. The outer byte is defined by `y-websocket/src/y-websocket.js:20-23`:

| Outer type | Meaning |
|---|---|
| 0 | sync (payload is a `y-protocols/sync` message) |
| 1 | awareness (`varUint8Array` of an awareness update) |
| 2 | auth (`y-protocols/auth`) |
| 3 | query awareness (no payload) |

Inner sync messages (`y-protocols/sync.js:38-40`):

| Sub type | Payload |
|---|---|
| 0 `SyncStep1` | `varUint8Array(state vector)` (`sync.js:48`) |
| 1 `SyncStep2` | `varUint8Array(update)` — `Y.encodeStateAsUpdate(doc, theirSV)` (`sync.js:59`) |
| 2 `Update` | `varUint8Array(update)` (`sync.js:96`) |

Client behaviour on connect (`y-websocket.js:196-220`): send `sync/SyncStep1` immediately,
then, if it has local awareness state, an `awareness` message for its own client id. The
client replies to a received `SyncStep1` with a `SyncStep2` computed against the received
state vector (`readSyncMessage`, `sync.js:118` — note the reply is written into the same
encoder, so the reply's outer byte is written by the caller).

Server behaviour per the protocol comment (`sync.js:23-28`): on `SyncStep1`, reply with
`SyncStep2` **immediately followed by our own `SyncStep1`**; broadcast subsequent edits as
`Update`. On `queryAwareness` (outer 3), reply with an `awareness` message carrying every
known state (`y-websocket.js:53-67`).

Auth (`y-protocols/auth.js`): only `messagePermissionDenied = 0`, payload
`varString(reason)`. That is the wire shape Phase 5 must use to reject a bad JWT.

### 2.9 Awareness

`encodeAwarenessUpdate` (`y-protocols/awareness.js:194`):

```
AwarenessUpdate := varUint(numClients) (varUint(clientID) varUint(clock) varString(JSON state))*
```

`state` is `JSON.stringify(state)`, and `"null"` marks a client as gone. `applyAwarenessUpdate`
(`:241`) accepts an entry when `currClock < clock`, or when `currClock === clock && state ===
null` and the client is currently known — so removals are idempotent at equal clocks. Each
client increments its own clock on every state change (`:104`).

There is **no TTL on the wire**: `outdatedTimeout = 30000` (`awareness.js:13`) is a client-side
interval that drops peers whose state is older than 30 s and re-broadcasts the local state
every 15 s. Consequence for us, recorded as concern C3 below.

### 2.10 YATA integration

`Item.integrate` (`Item.js:419`) after `getMissing` (`:372`) has resolved dependencies.

1. If the struct arrives with an offset, split: `id.clock += offset`, left becomes the item
   ending just before it, `origin = left.lastId`, content spliced (`:420-426`).
2. `origin` resolves via `getItemCleanEnd` and `rightOrigin` via `getItemCleanStart` — both
   **split** existing items so that the neighbour boundaries are exact (`:385-392`).
3. Parent: taken from `left` if it is an item, else from `right`, else from the decoded parent
   (root key or parent item id) (`:395-411`).
4. Conflict resolution runs only when the neighbourhood changed, i.e.
   `(!left && (!right || right.left !== null)) || (left && left.right !== right)` (`:429`).
   Scan from `o = left.right`, or the leftmost item of `parent._map.get(parentSub)`, or
   `parent._start`, while `o !== null && o !== this.right`:
   - add `o` to `itemsBeforeOrigin` and `conflictingItems`;
   - if `origin === o.origin` (case 1): if `o.id.client < this.id.client`, move left past `o`
     and clear `conflictingItems`; else if `rightOrigin === o.rightOrigin`, **break**;
   - else if `o.origin !== null` and `itemsBeforeOrigin` contains `getItem(o.origin)`
     (case 2): if `conflictingItems` does not contain it, move left past `o` and clear;
   - else **break**.
5. Relink left/right, update `parent._start` or `parent._map`, and if `parentSub !== null` and
   there is no right neighbour, delete the previous value (`:507-516`) — that is how YMap
   overwrite works.

The tie-break in step 4 is the one section 9 warns about: **the item with the smaller client id
ends up to the left**, and it is only consulted when both items share an origin. Getting the
comparison backwards produces documents that agree most of the time and diverge under
concurrent same-position inserts — which is why `text-concurrent-same-index` and
`text-concurrent-after-shared-origin` are separate fixtures (the second one shares
*both* origin and rightOrigin, exercising the `break` branch).

### 2.11 Trap list

1. `varInt` first byte holds 6 value bits and a sign bit; every later byte holds 7. Not zigzag.
2. `parentSub` is on the wire only when both origin and rightOrigin are absent, regardless of
   the info bit.
3. Client blocks, delete-set clients and state-vector clients are all sorted **descending**.
4. Refs 0 (GC) and 10 (Skip) are struct kinds, not content types.
5. `writeJSON` (embed, format) is a JSON string; `writeAny` (map values) is lib0-any binary.
6. Floats and bigints are big-endian; varints are little-endian.
7. The first struct in a client block may be written sliced, with a synthesised
   self-referencing origin.
8. `varUint` is limited to 2^53 − 1 on the JS side; Go must not emit anything larger.
9. String struct lengths are **UTF-16 code units** (`ContentString.getLength()` returns
   `str.length`, `ContentString.js:22`), while the `varString` length prefix is **UTF-8
   bytes**. `"🎉"` is 2 units of clock and 4 bytes on the wire. Using `len(s)` or
   `utf8.RuneCountInString` for clock arithmetic diverges on the first emoji.

---

## Part 3 — Open concerns (brief rule 7)

### C1. Redis Pub/Sub is at-most-once, and nothing else heals a missed update - resolved
The architecture has no document ownership, which is right for CRDTs, but it makes fanout the
only path by which replica A learns about replica B's writes. Redis Pub/Sub drops messages on
subscriber disconnect or slow-consumer buffer overflow with no retransmit. A replica that
misses one message would serve a permanently stale document to its clients: origin filtering
does not help, and the clients have no reason to reconnect.

Resolved by periodic anti-entropy ([D49]): a room with connections announces its state vector
every 15 s, and any replica holding more answers with the difference. Convergence is therefore
eventual with a bounded delay rather than immediate, which is the honest guarantee an
at-most-once bus can support. The integration test drives the loss directly - it edits a
document on one replica while a second replica has no room for that document at all - and the
second replica catches up with no database involved.

This project's own drop paths ([D51]) rely on the same mechanism, and so does the window
between a replica subscribing and finishing its load. That window is also why the subscription
is taken out before the document is read rather than after.

### C5. A pending deletion is invisible to peers until its structs arrive — resolved
Found by the convergence property test, not by reading the source. If replica A receives a
delete for structs it has not seen, the deletion sits in `pendingDeletes`, and Yjs leaves its
`pendingDs` out of what it sends — so a peer syncing with A learns the structs but not that they
are deleted, and the deletion arrives one exchange behind them.

Resolved by including pending deletions in what we send ([D40]). A peer that cannot resolve the
range holds it pending exactly as we do, and a peer that can resolve it applies a deletion it
was going to receive anyway, so the change costs nothing and removes a case where two replicas
that have exchanged everything still disagree. It also stops a pending deletion being dropped
when a room writes its snapshot and restarts.

What remains is a different and more fundamental thing, now stated separately as [C7]: a diff
is computed against a state vector, so it cannot carry structs that were never integrated. That
is why the convergence property still exchanges to a fixpoint rather than asserting a fixed
number of rounds, and why Phase 4's anti-entropy has to keep running rather than fire once per
reconnect. Related to [C1].

### C7. A diff cannot carry structs a replica has not integrated
`EncodeStateAsUpdate` walks the struct store, so structs held in `pending` — those whose
dependencies have not arrived — are not in the diff at all. Two replicas each holding a
different link of the same chain therefore need one exchange per link before they agree. The
convergence property measures this directly: it exchanges until the two replicas stop changing,
with the number of updates as the bound, and 1000 random splits per scenario stay inside it.

Consequences: anti-entropy must run periodically rather than once (which [C1] needs anyway),
and no single round trip can be treated as "synced" for the purpose of, say, dropping a
document from memory.

The fix, if single-round convergence is ever wanted: encode pending structs too, bridging the
gaps with `Skip` structs (ref 10), which is exactly what `Y.mergeUpdates` does and which our
decoder already reads. Deliberately not done now — it is surgery on the Phase 1 core to remove
a round trip from a path that heals itself, and Yjs has the same behaviour, so no client is
surprised by it.

### C2. Compaction can race across replicas — resolved for snapshot replacement
Any replica may compact any document (section 3, no ownership). A stale room snapshot can
otherwise replace a newer persisted snapshot and lose updates. `Store.Compact` takes a
per-document transaction advisory lock and compares owner, immutable document incarnation,
and monotonic `snapshot_generation`; every successful replacement advances that generation,
including replacements at the same `snapshot_seq`. The room writer reloads the persisted
snapshot and all remaining rows, merges them with its immutable candidate in a temporary CRDT
document, then retries a generation conflict a bounded number of times. It deletes only the
exact rows folded into the replacement. This protects persisted compaction state; it does not
change live-client convergence. Stale-room Append/version operations are fenced separately in
[D136]. All replicas must run
the new compaction code before this guarantee applies in a rolling upgrade. See [C6] for
out-of-order sequence allocation and row coverage.

### C3. Awareness needs a server-side timeout — resolved in Phase 2
`outdatedTimeout` is client-side only (2.9). A client that vanishes without sending
`state: null` leaves a ghost cursor in every other client that has already synced its state,
because the server keeps rebroadcasting it on join. Resolved by `protocol.Awareness.Sweep`,
which the room runs every 5 s: a client that has been silent for 30 s is dropped and the
removal is broadcast. A connection that closes cleanly has its states retracted immediately
rather than 30 s later. The removal goes out at the client's own clock, not `clock + 1`, for
the reason in [D30].

### C6. Identity columns can commit out of order — resolved
`seq` is `GENERATED ALWAYS AS IDENTITY`, so values are handed out before commit and a row with
a lower seq can become visible after one with a higher seq. Two consequences, both now handled:

Loading reads the whole remaining log rather than `seq > snapshot_seq` ([D35]), so a
late-committing row is still applied.

Compaction deletes exactly the rows its snapshot folded in, by seq (`seq = ANY($1)`), instead
of everything at or below a watermark ([D41]). A range delete would take a row the snapshot
never saw. That mattered little on one node and would have mattered a great deal at Phase 4,
where two replicas append to the same document; fixing it while the code was still small cost
an array instead of an integer.

### C4. Section 8 vs. TipTap
Resolved in D8, kept here as a pointer: section 8's "no `YXmlFragment`" and Phase 2's "TipTap
works" are in tension, and the resolution is decode-everything / expose-little.

### D136. Room writes belong to a document incarnation
A room captures owner and immutable incarnation when it loads a document. Append, version save,
and version pruning lock and validate that parent identity in the same transaction as their
write, so an old room cannot write into a deleted/recreated document or after an owner transfer.
`SetOwner` rotates incarnation only when ownership changes, preventing A→B→A from reviving an old
room. Internal cluster envelopes carry a versioned presence marker and the full signed BIGINT
incarnation; every message kind is checked after the actor loads, preserving subscribe-before-load
without a lost-update window. Legacy persisted incarnation zero remains distinct from a node
without a database. This does not evict old rooms immediately, prevent clients from retaining
old state, add persisted client acknowledgements, or change the public Yjs protocol. Private
envelope v0 is rejected, so replicas must be upgraded together. A failed resident admin merge is
not broadcast and reports its store error, but its already-applied in-memory CRDT state is not
rolled back. See [docs/DURABILITY.md](docs/DURABILITY.md).
