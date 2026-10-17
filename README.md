# ycollab

A self-hostable, Yjs wire-compatible collaboration server in Go.

The CRDT engine is written from scratch — YATA integration, struct store, state vectors,
delete sets and the lib0 codec, with no third-party imports in `internal/crdt` — and speaks the
Yjs v1 binary protocol byte for byte, so an unmodified `y-websocket` client works with no
client-side code at all.

Design decisions, the wire-format derivation and the open concerns are in
[DECISIONS.md](DECISIONS.md).

## Status

- **Phase 1 — CRDT core.** Done. Every committed fixture re-encodes byte-identically to the
  bytes Yjs produced, and `tools/verify/apply.mjs` accepts the Go output in a real `Y.Doc`.
- **Phase 2 — single-node WebSocket server.** Done: `internal/protocol`, `internal/room`,
  `internal/gateway`, `cmd/server`, the TipTap demo and the soak harness.
- **Phase 3 — persistence.** Done: PostgreSQL snapshot + append-only update log, compaction at
  500 updates, persist-on-evict, LRU cap on resident rooms.
- **Phase 4 — horizontal scale.** Done: Redis Pub/Sub fanout with origin filtering, periodic
  anti-entropy, and a three-replica deployment behind Caddy.
- **Phase 5 — authorisation.** Done: HS256 tokens that name the document they open, read-only
  and read-write permissions, key rotation.
- **Phase 6 — operating it.** Done: Prometheus metrics and pprof on a separate admin listener,
  and a load bot that reports propagation latency.
- **Deployment and housekeeping.** CI, Kubernetes manifests, alert rules and a Grafana
  dashboard, document deletion and retention, connection caps, optional durable writes.

## Run it

One command, nothing built, no Go toolchain — a server and a database for it:

```sh
docker compose -f deploy/docker-compose.quickstart.yml up
```

[**docs/QUICKSTART.md**](docs/QUICKSTART.md) takes that to two browser tabs
editing the same document, and then to the one flag that stops it being open to
everybody. Five minutes.

The image is `ghcr.io/contictus/crdt-server`, published on tags for `linux/amd64`
and `linux/arm64`. `ycollab -version` says which build it is, and so does the
first line of its log.

From source, with the dependencies in the other compose file:

```sh
docker compose -f deploy/docker-compose.yml up -d
go run ./cmd/server -addr :8080 -origins localhost:5173 \
  -database-url postgres://ycollab:ycollab@127.0.0.1:5433/ycollab
```

The document name is the URL path, so `ws://localhost:8080/my-doc` is document `my-doc`. Names
are mapped to document UUIDs by hashing, so they can be anything readable.

Without `-database-url` the server still works, but a document lives only as long as its room
is resident — five idle minutes by default. With it, updates are appended to a log, folded
into a snapshot every 500 updates, and written out when a room is evicted.

Writes are batched. Batching normally delays writes by `-flush-interval` (200 ms by default),
and database latency, failures, or queue backpressure can extend the unconfirmed window.
Room appends retry a narrow set of transient failures up to three attempts within one 30-second
batch deadline. `-durable-writes` waits for that bounded sequence before relaying each update,
at the cost of a database round trip per keystroke. It still relays after final failure: the
batch is logged, counted on `ycollab_store_failed_total`, and dropped from the write queue rather
than retained as a retryable batch; a later successful compaction may include resident state.
The flag is not a persisted acknowledgement or an outage-recovery guarantee; alert on the
failure counter [D73].

`-max-conns` and `-max-rooms` bound what one node will hold. Both default to unlimited, which
is right for a laptop and wrong for anything reachable; the Kubernetes manifests set them.

### Memory

`-max-rooms` counts documents, which says nothing about their size: two thousand
documents is forty megabytes or forty gigabytes depending on what is in them.
`-max-memory` is the bound written in the same unit as a container's limit.

```sh
ycollab -max-memory 4GiB     # or 4GB, or 4096MB, or a plain number of bytes
```

Each room measures its own document — a walk of the struct store, costing single
digit microseconds for a document of a realistic size — and re-measures only when
it has changed (every `UsageInterval`, 15 s by default). The total is
`ycollab_rooms_resident_bytes`, and it is the figure to put next to the pod's
limit on a dashboard.

The estimate is arithmetic over the structures, not a constant: item headers come
from `unsafe.Sizeof`, and what they point at is added separately (see
`internal/crdt/memory.go` and `internal/room/memory.go`). Measured against real
heap growth it lands at **0.91–1.00x** (DECISIONS D129). It is a floor by
construction — it does not model size classes, map buckets or the collector — so
leave headroom (25–30% is the guidance the runbook gives).

**Two honest limits.**

It is not a hard guarantee. A room with somebody connected to it is never
evicted: the alternative is disconnecting the person who is typing to make room
for somebody who has just arrived. Under sustained load with every document busy,
the budget is exceeded and the server keeps serving. That is a bill rather than
an outage, and the gauge shows it happening.

**It does not lift the ceiling.** A replica serving a document holds all of it —
sync step 2 needs the whole state and YATA integration needs the struct store, so
there is no partial residency to be had. What changes is that the ceiling is
stated, enforced, and visible before the out-of-memory killer finds it.

Raising the ceiling is a deployment question rather than a code one: **route each
document to one replica consistently**, and N replicas hold N times the documents
instead of each holding whatever its clients happened to open. Any ingress that
can hash on the URL path does it — with nginx, `hash $uri consistent`. Nothing in
the server needs to change; documents already work from any replica, so a hash
that is occasionally wrong costs a duplicate copy rather than a correctness
problem.

### Limits

Two things a connected client could otherwise do without permission, both bounded by default:

- **Cursors.** An awareness state is a name, a colour and a couple of offsets. Without a bound,
  one client could publish a 16 MiB "cursor" that this server holds in memory, relays to every
  peer and publishes to every replica. `-awareness-max-state` (4 KiB) and
  `-awareness-max-clients` (1024) bound the size and the count; a client that exceeds them is
  told which rule it broke and closed with 1008.
- **Rate.** A token bucket per connection on messages per second (`-rate-messages`, 200) and
  bytes per second (`-rate-bytes`, 8 MiB). Over the limit the server *waits* rather than
  disconnecting: it stops reading, TCP slows the sender, and only that connection is affected.
  A person typing produces ten to thirty messages a second, so the defaults are there to catch
  a client that has gone wrong, not to shape normal traffic.

For every one of these, zero means the default and a negative value means no limit.

`-max-conns-per-ip` bounds one address, because `-max-conns` on its own is a limit a single
client can reach alone — which makes "the node is full" something one actor decides. It is off
by default and the reason is NAT: an office, a school or a mobile carrier is *one address* to
this server, so a live default would be a live default on how many colleagues may edit at once.
The deployment knows its clients; the Kubernetes manifests set it.

### Who is connecting

Client addresses appear on every connection log line and on every refusal, so an incident has an
answer to "where is this coming from". Addresses are never a metric label — that would make the
endpoint's cardinality the number of addresses on the internet.

Behind a load balancer, tell the server which hops to believe:

```
-trusted-proxies=private          # or loopback, or 10.0.0.0/8,192.0.2.7
```

**Without this the `X-Forwarded-For` header is ignored entirely**, and that is deliberate. It is
a request header like any other — anyone can send one saying anything — so a server reached
directly must not read it, or clients choose their own identity and `-max-conns-per-ip` becomes
a control an attacker opts out of. With trusted proxies configured, the header is read only when
the machine on the other end of the socket is one of them, and the address taken is the
*rightmost* entry that is not itself trusted: the last address a machine we trust actually saw.
The leftmost entry is the part the client wrote.

### The admin listener

`-admin-token` requires `Authorization: Bearer <token>` on everything on the admin address
except `/healthz`, which stays open so a load balancer can probe liveness without holding an
operator credential.

```bash
ycollab -admin-token "$(openssl rand -hex 32)"
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:6060/documents/my-doc
```

It is not the JWT machinery clients use: those tokens are per-document capabilities minted for
editors, and "may administer this server" is not a document capability. Only the header is
accepted, never a query parameter — a token in a URL ends up in access logs, browser history and
the `Referer` of anything the response links to. The WebSocket endpoint accepts `?token=` because
a browser cannot set a header on a WebSocket handshake; nothing here has that excuse.

Without the flag the endpoints stay open and the server says so at startup. That was defensible
while the listener was read-only. It now serves `POST` and `DELETE`, so a request that reaches it
can rewrite or destroy any document — network isolation is still the first control, but it should
not be the only one.

`-admin-token a,b` accepts both at once, which is how one is rotated without a window where
either the old holders are broken or the new token is not accepted yet. **A token containing a
comma is therefore read as several**, so tokens must be at least 16 characters and the server
refuses to start on anything shorter — otherwise `sk_live_abcdef,ghi` would have quietly made
`ghi` an administrator password on upgrade. The audit trail below is
what makes the last step of that rotation safe: it says whether the old token is still being used
before anybody removes it.

### Multi-tenancy

Without it, a token that names a document is enough to open it — so any user of
any customer who learns a document name can read it. `owner` closes that.

A document is stamped with the owner of whoever opens it first, and every later
connection must match. The owner comes from the grant: the `own` claim in a JWT,
or `"owner"` in the authorization callback's answer.

```json
{ "doc": "notes", "perm": "write", "own": "acme", "sub": "ada", "exp": 1785400000 }
```

A tenant name is any string — a slug, an account number, a subdomain — hashed into
the `owner_id` column, so no deployment needs a lookup table to satisfy a column
type. A name that already is a UUID is used as one.

**No owner is an owner.** A token with no `own` claim reaches only documents that
have no owner, and a tenant does not inherit the documents that have none. That is
what makes turning tenancy on safe: every document in a database that predates it
stays exactly where it was, reachable by the tokens that already worked, and no
tenant silently acquires them by connecting first. Moving them is a deliberate
operator action:

```bash
curl -X PUT -H "Authorization: Bearer $TOKEN" -d '{"owner":"acme"}'   http://127.0.0.1:6060/documents/notes/owner
```

A document somebody is editing is refused rather than moved under them; close the
connections, or wait for the room to go idle.

The refusal a client sees is the same one a bad token gets, word for word.
Distinguishing "this is not yours" from "there is no such document" would make the
boundary a way to enumerate other customers' document names, one guess at a time.

The admin listener is above the boundary by design — it can already delete any
document — which is why it needs its own credential and why everything on it is
audited.

### Listing documents

```bash
curl -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:6060/documents?owner=acme&limit=50"
```

```json
{
  "documents": [
    { "name": "notes", "id": "f81d4fae-...", "owner_id": "3b1e...", "resident": true,
      "updated_at": "2026-07-31T09:14:02Z", "snapshot_bytes": 4210, "updates": 17 }
  ],
  "next": "notes/f81d4fae-..."
}
```

Sizes, never content: a listing is for deciding what to fetch. Omit `owner` for
the operator's view of everything; pass `owner=` with an empty value for the
documents that belong to nobody.

`next` is a keyset cursor — pass it back as `?after=`. Keyset rather than an
offset because a tenant's documents change while somebody pages through them, and
an offset would skip or repeat rows as they do. It also costs the same on page a
thousand as on page one, which matters because paging this is how "delete
everything belonging to this account" is carried out.

`resident` is about *this* replica. Another one may hold the document and this one
would not know.

**One caveat for existing databases.** Document ids are a one-way hash of the
name, so rows written before the server recorded names have an empty `name` and
cannot be opened from a listing alone. `PUT .../owner` records the name as a side
effect, and the runbook has a bulk statement.

### What storage costs

Snapshots and version payloads are deflated before they are written; the update
log is not, because a keystroke is around twenty bytes and deflate's own header is
about the same.

**Measured, not estimated: about 2x.** Across five corpora built with real Yjs —
natural English prose from this repository's own documentation, heavily revised
prose, and a five-client collaborative document — the ratio is **1.97x to 2.70x**,
and a live server storing all five reported 583 KB in and 274 KB out (2.13x).
`tools/fixturegen/corpus.mjs` rebuilds those corpora if you want to check.

It is only 2x because a Yjs update is mostly varint-encoded client ids and clocks,
which are high entropy by construction; the text is a minority of the bytes. Any
number much above that comes from a benchmark that types the same few words in a
loop. `flate.BestCompression` was measured too and buys **nothing** — identical
ratios for up to five times the CPU.

Compression never makes a payload bigger: if deflate does not help, the bytes are
stored as they came. The codec is a column beside the blob, not a marker inside
it, because a Yjs update starts with a varUint that could be any prefix we chose.
Rows written before this existed have codec `0` and keep reading.

Watch it in production:

```
rate(ycollab_store_blob_bytes_total{state="raw"}[1h])
  / rate(ycollab_store_blob_bytes_total{state="stored"}[1h])
```

### Object storage

`-s3-bucket` moves snapshots and version payloads out of PostgreSQL. Everything
that is *queried* stays in the database — the document row, the owner, the name,
the sequence numbers, the state vectors, the update log. What moves is what is
only ever read whole, by primary key, and never joined against anything.

```sh
ycollab -database-url ...   -s3-bucket ycollab -s3-region eu-west-1   -s3-access-key ... -s3-secret-key ...

# Anything that speaks S3: MinIO, R2, Backblaze, Ceph.
ycollab ... -s3-bucket ycollab -s3-endpoint http://minio:9000 -s3-prefix prod/
```

The reason is history. A version is a whole document and the default is
twenty-four per document; at scale that is a database holding terabytes of blobs
it never queries, with the vacuum pressure and backup times that implies.

**Turning it on migrates nothing.** Each row says where its own bytes are, so a
database can hold both kinds at once: rows written before are read from their
columns, rows written after from the bucket. Turning it off again leaves the
objects readable — but a server with no bucket configured that meets a row naming
one fails loudly rather than serving an empty document, which would look like a
document somebody had emptied.

**The order of operations is the correctness argument.** Writing: the object
first, then the row. Deleting: the row first, then the object. Either way a
failure in between leaves an object nothing points at — wasted storage — rather
than a row pointing at bytes that are not there, which is a document that cannot
be read. Orphans are bounded and the runbook says how to find them.

A snapshot's key contains its sequence number. Two replicas compacting the same
document at once therefore write two different objects, and the row decides which
one counts; a shared key could have left the loser's bytes under the winner's row.

**What this client does not do**, because it is 450 lines of SigV4 and HTTP rather
than an SDK: no IRSA, no EC2 instance roles, no SSO, no shared config file. Keys
come from the flags or from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
`AWS_SESSION_TOKEN` — on Kubernetes, a Secret rather than a service account
annotation. No multipart upload, so a single object is capped at S3's 5 GB. The
signing is checked against a real S3-compatible service in the test suite, not against a mock that
would agree with its own mistakes.

### The audit trail

The admin listener can read, overwrite and delete every document, so what happens on it is
recorded — by default, with no configuration. One JSON object per line on **stdout**, while the
process log is on stderr, so the two are already separate wherever they are collected:

```json
{"time":"2026-07-31T09:14:02Z","action":"document.delete","result":"ok","status":204,
 "document":"notes","credential":"a1b2c3d4","ip":"10.1.4.7","method":"DELETE",
 "path":"/documents/notes","bytes":0,"duration_ms":12}
```

`action` is the server's vocabulary rather than HTTP's — `document.read`, `document.write`,
`document.delete`, `version.list`, `version.read`, `version.take`, `profile.read`, `stats.read`,
`unknown`. `result` is derived from the status: `ok`, `denied` (401/403), `refused` (any other
4xx) or `failed` (5xx). `credential` is the first eight hex digits of the SHA-256 of the token
that was used — enough to tell two tokens apart during a rotation, and not the token.

Three things are deliberate:

- **Attempts are recorded, not just successes.** A `401` is the most interesting line in the
  file. The token that was *tried* is never written, though: that would make the trail an oracle
  for guessing it.
- **`/debug/pprof` is audited.** A heap profile is a copy of every document the process is
  holding, which makes it the least obvious way to read documents off this surface.
- **No document content, ever.** Names, byte counts, status codes and the server's own error
  sentence. A trail that carried what was written would be a second copy of the data with none of
  the protection the first copy has.

`/metrics` is the one route not recorded when it succeeds — it is scraped every few seconds
forever, and a record per scrape would bury everything worth reading. A *refused* scrape is
recorded, because a wrong credential there is somebody finding out whether the surface is open.

`-audit-log /var/log/ycollab/audit.jsonl` writes to a file instead, appending rather than
truncating so a restart is not a way to lose the trail. Nothing here rotates that file —
`logrotate` already exists, and a server that truncates its own audit trail on a schedule is a
strange thing to hand an auditor. `-audit-log ""` turns it off, and the server warns at startup
when it is.

With one shared `-admin-token` this records *which credential*, not which person. That is the
honest limit of what the server knows about an operator surface.

### More than one replica

`-redis-url` makes a process one replica of a cluster rather than a server on its own. Replicas
relay each other's updates and cursors over Redis Pub/Sub, and each announces its state vector
every `-anti-entropy` seconds so a dropped message is repaired rather than becoming a permanent
divergence. Without it, two processes behind the same load balancer serve the same document
name as two unrelated documents, and the server says so at startup.

The whole thing — Postgres, Redis, three replicas and Caddy in front — comes up with:

```sh
docker compose -f deploy/docker-compose.cluster.yml up -d --build
```

Caddy is on <http://127.0.0.1:8080> and each replica is also published on 8081, 8082 and 8083,
which is what lets a test put one client on one replica and another on another; their admin
listeners are on 6081, 6082 and 6083. There is no
sticky session: any replica can serve any document, and a client that reconnects resyncs from
its state vector.

### Watching it

`-admin-addr` (default `127.0.0.1:6060`) serves the endpoints that are for operators rather
than for clients:

- `/metrics` — Prometheus. Connections, rooms, message counts by type, how long integrating an
  update takes, how long the database takes, and what the server refused.
- `/statsz` — the cluster counters as JSON: how many envelopes this node published and how many
  it filtered out as its own.
- `/debug/pprof` — the Go profiler. `-pprof=false` turns it off.
- `GET /documents/{name}` — reads a document without opening a WebSocket. See below.
- `DELETE /documents/{name}` — removes a document and its log. Refused with 409 while somebody
  is editing it. The listener is the authorisation: this is an operator action, and the tokens
  the server understands are per-document capabilities for editors, not operator credentials.

Alert rules and a Grafana dashboard are in `deploy/observability/`, and
[`docs/RUNBOOK.md`](docs/RUNBOOK.md) says what to do about each alert, plus backup and restore.
`-retention 720h` deletes documents nothing has touched for a month; it is off unless asked
for.

They are deliberately not on the port clients connect to. pprof dumps the heap, prints the
command line and will block the process for a thirty-second CPU profile on request, so the
deployment gets to decide who can reach it. `/healthz` is on both, because a load balancer
probes the port it sends traffic to.

### Reading a document

`GET /documents/{name}` on the admin listener returns the document as a Yjs update — the same
bytes a client gets from a sync with an empty state vector, so `Y.applyUpdate` reads it
directly:

```bash
curl -s http://127.0.0.1:6060/documents/my-doc > my-doc.bin
```

Every response carries `X-Ycollab-State-Vector` (base64) for Yjs sync and an `ETag` for the
selected HTTP representation. The ETag changes when those response bytes change, including a
delete-only update that leaves the state vector unchanged. JSON and `Accept` negotiation select
their own response representation; `?sv=` hashes the returned diff bytes. Thus:

- `If-None-Match: <etag>` → `304` when that selected response has not changed.
- `?sv=<base64 state vector>` → only what you are missing, rather than the whole document. This
  is the pair to the webhook: the event hands you a state vector, this turns it into a diff. The
  state vector tracks Yjs struct clocks; it is not an HTTP content validator.

`X-Ycollab-Resident` says where the answer came from. A document somebody is editing is read
from the room, so it is exactly what those clients see. One nobody is editing is read from the
database *without starting a room* — waking one for a read would hold the document in memory and
join it to the cluster as a side effect of somebody looking. The consequence in a cluster: an
edit that is still sitting in another replica's flush window (200 ms by default) is not visible
yet. Making it exact would mean a bus round trip on every read.

`?format=json` (or `Accept: application/json`) gives a readable view instead:

```json
{
  "document": "my-doc",
  "state_vector": "AQKf1Y…",
  "resident": true,
  "clients": 2,
  "bytes": 1841,
  "roots": [{ "name": "notes", "text": "hello world", "keys": [] }]
}
```

There is no `type` field on a root, and that is the honest answer rather than an omission: **the
v1 wire format never records what kind a root type is.** `doc.getText('x')` and `doc.getMap('x')`
read the same bytes two ways, and Yjs decides when the client asks — a server that only ever saw
the updates cannot know which was meant. So both readings are offered: `text` is the root read
as a sequence (empty for a map), `keys` is it read as a map (empty for text). The binary form is
the complete one; this view is a convenience over it, and for an XML root — TipTap, ProseMirror
— it will show a named root and no content, because the engine decodes XML but exposes no reader
for it.

A name nothing has ever written is `404`, not an empty document.

`POST /documents/{name}` is the other half: it takes a Yjs update as the body and applies it.
That is what makes a `GET` a backup rather than a souvenir, and what seeds a document from a
template.

```bash
curl -sf $ADMIN/documents/my-doc > my-doc.bin          # back it up
curl -sf -X POST --data-binary @my-doc.bin $ADMIN/documents/my-doc   # put it back
```

It **merges**; it does not replace, and the method says so. These are CRDT updates: applying one
adds what it carries to what is already there, and the format has no operation that makes a
document forget. Restoring over a document that has moved on gives the union of the two —
`DELETE` first when that is not what you want.

A merge into a document people are editing reaches them: it is broadcast to every connection and
published to the other replicas, so nobody is left building on a version the server no longer
has. A body that is not an update, or one that carries nothing, is refused with 400 rather than
written and reported as a success.

### Subdocuments

**A subdocument is an ordinary document here, named by its guid.** That is not a decision this
server made — it is how Yjs works. `y-websocket` does not mention subdocuments at all: `Y.Doc`
emits a `subdocs` event and the application opens a second provider for each one, naming the
room after the guid:

```js
doc.on('subdocs', ({ added }) => {
  added.forEach(sub => new WebsocketProvider('ws://localhost:8080', sub.guid, sub))
})
```

So syncing works with no server-side feature, and the honest thing was to prove that rather than
claim it — `TestASubdocumentSyncsAsADocumentOfItsOwn` does, against a real Yjs fixture carrying
a `ContentDoc`.

What the server *does* add is the link between the two, because **a parent document is the only
thing that names its subdocuments**:

```bash
curl -s "$ADMIN/documents/my-book?format=json" | jq .subdocs
# ["chapter-one"]
```

Removed references do not appear — a subdocument whose reference was deleted is no longer part
of the document, which is exactly the distinction someone deciding what to delete needs.

**Two traps worth stating plainly:**

- **A backup of a parent is not a backup of the whole.** `GET /documents/my-book` returns the
  parent only; its subdocuments are separate documents with their own bytes and their own
  version history. Back up the guids too.
- **Deleting a parent orphans its subdocuments.** `DELETE` does not cascade across documents,
  because it cannot: the server would have to read the parent to find out what to delete, and a
  delete that quietly removes documents you did not name is worse than one that does not. Read
  `subdocs` first and delete them yourself.

**Tokens.** With `-jwt-secret` set, a token names one document, so a client needs one per
subdocument. That is the same round trip the application already makes to get the parent's
token, made again when the `subdocs` event fires — the app knows the guid at that point, because
it just received it. The server deliberately does not infer "and its subdocuments" from a parent
token: it would have to load the parent on every connection to check, and a token whose scope
grows as the document changes is not a scope anybody can reason about.

If minting one token per guid is the wrong shape for your application, `-auth-url` is the other
answer: the server asks about each guid as it is opened, and the application decides then, with
no token to mint. See [Asking your application instead](#asking-your-application-instead).

### Version history

`-version-interval 1h` keeps a copy of each document as it changes, so there is an answer to
"what did this say before somebody pasted over it". Nothing else in the server can answer that:
a CRDT update log records what was *added*, and compaction folds it away by design.

```bash
# What versions are there?
curl -s $ADMIN/documents/my-doc/versions | jq

# Take one now, before doing something risky.
curl -s -X POST "$ADMIN/documents/my-doc/versions?label=before+the+migration"

# Read one. Same form as the document read API, so one piece of client code opens both.
curl -s $ADMIN/documents/my-doc/versions/42 > yesterday.bin
```

The listing carries when, how big, the label and the state vector — enough to choose from
without downloading anything:

```json
{
  "document": "my-doc",
  "versions": [
    { "id": 42, "created_at": "2026-07-30T09:00:00Z", "state_vector": "AQKf1Y…", "label": "before the migration", "bytes": 1841 }
  ]
}
```

`bytes` on a listing is the **stored** size — what the version costs, after
compression — so it is smaller than the document it holds. Fetching the version
returns the document itself, at its full size. If you are sizing a download,
`state_vector` is the better thing to compare; if you are sizing a bill, this is.

**Restoring is two steps, on purpose:**

```bash
curl -sf -X DELETE $ADMIN/documents/my-doc
curl -sf -X POST --data-binary @yesterday.bin $ADMIN/documents/my-doc
```

There is no one-call restore, because `POST` merges and cannot remove what the document has
since gained. Skipping the `DELETE` gives you the union of the good version *and* the damage —
which is exactly what you were trying to undo. Two visible steps beat one endpoint whose name
promises more than it does.

**On storage.** Each version is a whole document, so hourly versions of a 1 MB document would be
24 MB a day if they were all written. Two things stop that:

- A version is only stored when its **state vector differs** from the newest one. A document
  nobody edited gets one row however long the timer runs.
- `-version-keep` (24 by default) bounds the count per document; older ones go after each write.
  Negative keeps everything, which is unbounded storage — say so out loud before choosing it.

In a cluster every replica holding a document runs its own timer. They do not produce three
versions per interval: the "is one already this recent" check is a condition on the insert, so
whichever replica gets there first wins and the others write nothing. Deleting a document takes
its history with it.

### Webhooks

`-webhook-url` makes the server POST a JSON body when something happens to a document, which
is how a backend finds out about edits without holding a WebSocket open for every document.

| Event | Raised when |
| --- | --- |
| `document.change` | clients connected to *this* node changed the document |
| `document.store` | updates were written to the database |

```json
{
  "event": "document.change",
  "document": "my-doc",
  "at": "2026-07-30T09:14:02.481Z",
  "node": 8146073922814,
  "clients": 3,
  "updates": 12,
  "state_vector": "AQKf1Y…",
  "state": "AQKf1Y…"
}
```

`state_vector` is always there and lets a receiver tell whether it is behind. `state` is the
whole document as a Yjs update, and only appears with `-webhook-state`; a receiver reads it
with one `Y.applyUpdate(doc, new Uint8Array(atob(body.state).split('').map(c => c.charCodeAt(0))))`.

Four things worth knowing before you build on it:

- **Events are coalesced onto the room's tick.** Typing produces tens of updates a second and
  you get one event saying `"updates": 12`, not twelve events. The delay is bounded by `-tick`
  (5 s by default), so lower it if you want faster notifications.
- **A hook cannot block or refuse an edit.** The queue is bounded and a full queue drops events
  and counts them in `ycollab_hooks_dropped_total`; a slow receiver never becomes a slow
  document. `-webhook-queue` sizes it.
- **Be idempotent.** Two people editing on two replicas produce an event from each, and a
  retried delivery repeats one. `X-Ycollab-Delivery` is generated once per event and reused
  across its retries, so a repeat is recognisable.
- **`document.change` is per replica, and only for local edits.** An update that arrived over
  the cluster bus is not reported again here — otherwise one keystroke would be one webhook per
  replica.

Failures are retried (`-webhook-retries`, default 2) with a doubling backoff on a timeout, a
connection error, 429 or 5xx. A 4xx is taken at face value and not retried.

**Verify the signature.** With `-webhook-secret` set, every request carries

```
X-Ycollab-Signature: t=1750000000,v1=<hex of HMAC-SHA256(secret, "<t>.<body>")>
```

The timestamp is inside the signed text, so a captured request cannot be replayed later with a
fresh one. Reject a `t` far from your own clock. In Node:

```js
import { createHmac, timingSafeEqual } from 'node:crypto'

// body is the raw bytes, not the parsed object.
function verify(secret, header, body, tolerance = 300) {
  const parts = Object.fromEntries(header.split(',').map(p => p.trim().split('=')))
  if (Math.abs(Date.now() / 1000 - Number(parts.t)) > tolerance) return false
  const want = createHmac('sha256', secret).update(`${parts.t}.`).update(body).digest()
  const got = Buffer.from(parts.v1, 'hex')
  return got.length === want.length && timingSafeEqual(got, want)
}
```

Without a secret the requests are unsigned and the server says so at startup, because a
receiver then has no way to tell this server's events from anybody else's.

### Tokens

`-jwt-secret` makes the server require a signed token. Without it every client may read and
write every document, which is fine for the demo and nothing else — the server warns about it
at startup.

A token is a JWT signed with HS256 that names the document it opens and the permission it
grants:

```json
{ "doc": "notes", "perm": "write", "sub": "ada", "exp": 1785400000 }
```

Naming the document is what makes it a capability rather than a login: a token that leaks opens
one document until it expires. `perm` is `read` or `write`; a read-only connection receives the
document and publishes its cursor, and is refused with a permission-denied message and a 1008
close if it tries to edit. An absent `perm` means `read`.

The token travels as `?token=...`, which is what `y-websocket`'s `params` option writes and the
only place a browser can put one — the WebSocket API cannot set a header. An `Authorization:
Bearer` header is accepted too, for clients that are not browsers. Because the token ends up in
URLs, expiry is required by default (`-jwt-require-expiry`) and can be capped
(`-jwt-max-lifetime`).

In a real deployment the application that knows who its users are mints these. For local use:

```sh
export YCOLLAB_JWT_SECRET=$(go run ./cmd/ycollab-token -gen-secret)
go run ./cmd/ycollab-token -doc demo -perm write -ttl 1h
go run ./cmd/ycollab-token -doc demo -perm read -url ws://127.0.0.1:8080   # a full URL
```

`-jwt-secret a,b` accepts both keys at once, which is how a key is rotated without an outage.

### Asking your application instead

A token is a capability: the application decides once, at the moment it mints one. That is the
wrong shape when the application already has a session and no token endpoint, when permissions
change while a document is open, or when subdocuments are in play — Yjs opens a provider per
guid, and those guids are names the application only learns about when Yjs tells it.

`-auth-url` moves the decision to the application. The server POSTs one JSON body per
connection, before the WebSocket upgrade:

```json
{ "document": "notes", "token": "session=abc", "ip": "203.0.113.7", "origin": "https://app.example.com" }
```

and expects:

```json
{ "allow": true, "write": true, "subject": "user_42" }
```

`allow` is required — an answer without it is refused, whatever else it says. `write` false
grants a read-only connection, the same one a `read` token gets. `subject` is for logging.
`reason` on a refusal is written to the client in the permission-denied frame, so it is the
place to say "your trial ended" rather than leaving somebody staring at a closed socket. A
refusal may also be a plain `401` or `403`.

The token is forwarded verbatim and is never inspected here: it is a session cookie value, an
opaque id, your own JWT, or empty. `-jwt-secret` and `-auth-url` are alternatives, and setting
both is a startup error — running them together would mean picking a winner when they disagree,
and an endpoint that wants JWTs can verify them itself.

**Sign the requests.** With `-auth-secret` set, every request carries
`X-Ycollab-Signature: t=<unix>,v1=<hex hmac-sha256 of "<t>.<body>">`, the same scheme the
webhook uses. Without it, your authorisation endpoint answers "does this token work" to anybody
who can reach it.

**When the endpoint is down**, connections are refused. `-auth-fail-open` inverts that and lets
everybody read and write for the duration of the outage; it is off by default because not
knowing who somebody is, is a worse reason to serve a document than to withhold it. Fail-open
covers an endpoint that is *down* — a timeout, a refused connection, a 5xx. An endpoint that is
up and answering a `404`, a redirect, or a 200 this server cannot read is a misconfiguration,
and those connections are refused whatever `-auth-fail-open` says: otherwise a typo in the URL
is an open server.

**Cost.** One request per connection, on the path a client is waiting on — watch
`ycollab_auth_duration_seconds`. Identical questions arriving at once (the reconnect storm after
a deploy) collapse into a single request. `-auth-cache-ttl` additionally reuses a decision for
the same token and document; it is `0` by default, because a remembered decision is a revocation
that has not taken effect yet. An endpoint may return `"ttl": <seconds>` to be re-asked sooner
than that, but never later.

### The JavaScript client

Optional. The server is plain `y-websocket`, and this is the whole integration:

```js
new WebsocketProvider('wss://collab.example.com', 'notes', new Y.Doc(), { params: { token } })
```

[`ycollab-client`](client/) is for the three places that setup leaves something to you, and
nothing else: tokens that expire (`y-websocket` reconnects forever with the ones it was given,
so the document quietly stops syncing an hour after page load), refusals you cannot see (its
permission-denied handler is a module-level `console.warn` that is not an option and cannot be
replaced), and subdocuments, which need a provider and a token each.

```js
import { connect } from 'ycollab-client'

const client = connect({
  url: 'wss://collab.example.com',
  name: 'notes',
  token: (document) => fetch(`/api/collab-token?doc=${document}`).then((r) => r.text())
})
client.on('denied', ({ reason }) => showBanner(reason))
const text = client.doc.getText('body')
```

The token function is called again before the token expires and once after a refusal; refreshing
writes `provider.params` rather than reconnecting, because this server authorises at the upgrade
and never again. `client.provider` is the real provider, so nothing here can get in the way.

Its tests build `cmd/server` and run against real server processes — see [client/](client/).

### The demo

```sh
cd web && npm install && npm run dev
```

Then open <http://localhost:5173/#demo> in two tabs and type in both. If the server requires
tokens, open <http://localhost:5173/?token=...#demo> instead — the page passes the token
through to the provider.

The header shows the connection state, the peer count and the document's state vector, so the
two tabs can be compared at a glance.

### Tests

```sh
go test ./... -race
```

Some tests need real infrastructure and skip without it. All three come up together:
Creating the test bucket also requires AWS CLI v2.
Compose stores RustFS data in Docker's anonymous volume, which survives a stop and restart.

```sh
docker compose -f deploy/docker-compose.yml up -d
for _ in $(seq 30); do curl -fsS http://127.0.0.1:9002/health/ready >/dev/null && break; sleep 1; done
curl -fsS http://127.0.0.1:9002/health/ready >/dev/null
export AWS_ACCESS_KEY_ID=ycollab AWS_SECRET_ACCESS_KEY=ycollab-secret AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:9002 s3api head-bucket --bucket ycollab >/dev/null 2>&1 || \
  aws --endpoint-url http://127.0.0.1:9002 s3api create-bucket --bucket ycollab

YCOLLAB_TEST_DATABASE_URL=postgres://ycollab:ycollab@127.0.0.1:5433/ycollab YCOLLAB_TEST_REDIS_URL=redis://127.0.0.1:6380 YCOLLAB_TEST_S3_ENDPOINT=http://127.0.0.1:9002   go test ./... -race
```

RustFS is there so the hand-rolled SigV4 signer is checked against a real S3
implementation rather than a mock that would agree with its own mistakes — one of
those tests signs with a deliberately wrong secret to prove the service checks it.

Those tests build the server, kill the process outright, restart it and reconnect, because a
graceful shutdown gets to flush and a crash does not.

Convergence is also checked against real clients. With a server running:

```sh
node tools/soak/soak.mjs --clients 6 --duration 300
```

Six real `y-websocket` clients edit the same document concurrently for five minutes, one of
them reconnecting every twenty seconds, and the run fails unless every client ends up with the
same text, the same state vector and the same view of who is in the room.

Against the cluster, the same harness spreads its clients over the replicas and reads each
replica's counters at the end, which is where "no update looped" is checked:

```sh
node tools/soak/soak.mjs --clients 6 --duration 300 \
  --urls ws://127.0.0.1:8081,ws://127.0.0.1:8082,ws://127.0.0.1:8083 \
  --stats http://127.0.0.1:6081,http://127.0.0.1:6082,http://127.0.0.1:6083
```

### Load

`cmd/ycollab-load` opens a lot of connections and reports how long an update takes to travel
from one client to another in the same room. It speaks the protocol directly rather than
running real Yjs clients — a real client costs a document and a provider, so a few hundred of
them make the generator the bottleneck. Correctness is what `tools/soak` is for.

```sh
go run ./cmd/server -addr 127.0.0.1:8080 &
go run ./cmd/ycollab-load -url ws://127.0.0.1:8080 -clients 1000 -rooms 100 -duration 30s -rate 4
```

On the development machine (Windows, 1000 connections over 100 rooms, four updates per second
each):

```
updates sent       118984 (3966/s)
updates delivered  1070856 (35694/s), expected 1070856
delivered ratio    1.0000
errors             0
propagation p99    511µs
propagation max    4.078ms
clock resolution   530.5µs
```

and with the fanout concentrated — 200 clients in two rooms, so every update goes to 99 peers:

```
updates delivered  1960101 (98003/s), expected 1960101
delivered ratio    1.0000
propagation p95    670µs
propagation p99    1.926ms
```

`delivered ratio 1.0000` is the part that matters: every update reached every other client in
its room, so nothing was dropped for backpressure. The server's own metrics put the mean cost
of integrating one update at about a microsecond.

The Go integration tests cover the same ground by starting real server processes; they need
Redis and skip without it:

```sh
docker compose -f deploy/docker-compose.yml up -d
YCOLLAB_TEST_REDIS_URL=redis://127.0.0.1:6380 go test ./... -race
```

### Running it for real

`deploy/k8s/ycollab.yaml` is a working deployment: three replicas, secrets for the database,
Redis and the signing key, an ingress with the WebSocket timeouts a long-lived connection
needs, an autoscaler on CPU and a disruption budget. The admin port is deliberately absent
from the public Service.

`.github/workflows/ci.yml` runs gofmt, vet, staticcheck, the race-detector tests against a
real Postgres and Redis, govulncheck, the byte-for-byte wire check against real Yjs, a
two-replica soak with real clients, and a short load run.

## Layout

```
cmd/server            the server binary
cmd/ycollab-dump      re-encodes fixtures with the Go engine, for tools/verify
cmd/ycollab-token     mints tokens for local use
cmd/ycollab-load      opens many connections and measures propagation latency
internal/crdt         the CRDT engine; standard library only
internal/crdt/lib0    varUint, varInt, varString, varUint8Array and the any codec
internal/protocol     sync and awareness framing; pure bytes, no I/O
internal/room         one actor goroutine per document
internal/gateway      WebSocket lifecycle, pumps, backpressure
internal/store        PostgreSQL: snapshots and the append-only update log
internal/cluster      Redis Pub/Sub fanout between replicas
internal/auth         token verification: who may open which document, and how
internal/metrics      Prometheus collectors
tools/fixturegen      Node: generates the binary fixtures from real yjs
tools/verify          Node: applies Go-produced updates in real yjs
tools/soak            Node: drives real clients at a running server
client                the optional npm package: token refresh, refusals, subdocuments
web                   TipTap + y-websocket demo
deploy                docker-compose for local Postgres and Redis, plus the cluster
deploy/k8s            Kubernetes manifests
deploy/observability  Prometheus alert rules and a Grafana dashboard
testdata/fixtures     committed binary fixtures
```

## Licence

MIT — see [LICENSE](LICENSE). The same licence as `yjs`, `y-protocols`, `y-websocket` and
`lib0`, so nothing here adds a second licence to reason about on the client side. The
reasoning, including why not Apache-2.0, is [D131](DECISIONS.md).

This server has not run in production and has no users yet. The engineering is verified —
byte-for-byte against real Yjs, under `-race` against real Postgres, Redis and RustFS — but
verified is not the same as proven by traffic, and the limits that are known are stated in
[Limits](#limits) and in the concerns at the end of [DECISIONS.md](DECISIONS.md) rather than
left for you to find.
