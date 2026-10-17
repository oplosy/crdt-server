# ycollab runbook

For whoever is on call. Every command here has been run against a real server
and a real database; the backup and restore section in particular is a
transcript, not a suggestion.

Two exceptions, stated so you know what you are relying on. The `jq` filters are
written for your machine and were composed rather than executed — the API shapes
they read are the ones the tests assert, but the pipelines themselves are not
transcripts. And the bulk `UPDATE` in the multi-tenancy section is deliberately
not something this runbook ran against anything but a scratch database.

## Where the state is

Three places, and knowing which one is which decides most incidents:

| What | Where | Survives |
| --- | --- | --- |
| The document being edited | a room's memory, one goroutine per document | nothing — it is a cache of the two below plus what clients hold |
| The durable copy | PostgreSQL: a `documents` row with a snapshot, plus an append-only `doc_updates` log | everything |
| Live fanout between replicas | Redis Pub/Sub | nothing, deliberately — a lost message is repaired by anti-entropy |

The clients are the fourth copy, and the most important one during an incident:
every connected browser holds the whole document. A server that restarts is a
resync, not a data loss, as long as the clients are still there.

## Deciding how bad it is

| Symptom | Severity | Why |
| --- | --- | --- |
| A replica is down, clients reconnect to another | low | reconnect and resync costs a diff |
| Redis is down | medium | replicas stop seeing each other; each one is internally consistent |
| Postgres is down | high | edits live only in memory and in the clients; a restart now loses them |
| `ycollab_frames_dropped_total` climbing | medium | clients are being disconnected to protect documents |
| Documents readable but wrong | stop and read "A document is wrong" below | do **not** restart; memory may hold the only good copy |
| `-auth-url` endpoint down, `-auth-fail-open` off | high | nobody can open a document |
| `-auth-url` endpoint down, `-auth-fail-open` on | high | everybody can open every document |

## Alerts

### YcollabDown

A replica is not being scraped.

1. `kubectl get pods -l app=ycollab` — is it restarting, or gone?
2. `kubectl logs <pod> --previous` — a startup failure says which flag or
   dependency: the server refuses to start on a bad `-database-url`, an
   unreachable Redis, a malformed `-webhook-url` or an unknown
   `-webhook-events` name.
3. If the process is up but not scraped, check `-admin-addr`. It defaults to
   `127.0.0.1:6060`, which is unreachable from another pod; the manifests set
   `0.0.0.0`.

Clients on that replica have already reconnected elsewhere. If the room wrote
its snapshot on the way out, no recovery is needed. If it was killed during the
unconfirmed window, edits may be missing from PostgreSQL; clients that still
hold those edits may resend them, but do not assume a client copy survived.

### YcollabWritesFailing

`ycollab_store_failed_total` is climbing. Some edits may exist only in room
memory or in the clients that made them; an ambiguous commit can also mean the
database wrote an update but the server did not receive confirmation.

1. Is Postgres up and reachable? `kubectl exec <pod> -- /bin/sh -c 'nc -z <host> 5432'`.
2. Is it out of connections? `SELECT count(*) FROM pg_stat_activity;` against
   `max_connections`.
3. Is it out of disk? A full volume shows up here first.

**Do not restart the replicas while this is firing.** Their memory holds edits
that may not be in the database. Room appends retry a narrow set of transient
failures up to three attempts within one 30-second batch deadline. After those
attempts fail, that batch is dropped from the writer queue; fixing PostgreSQL
does not retry it. A later successful compaction may include resident state,
but is not a replay guarantee. The queue holds 4096 waiting updates per document,
and the room blocks rather than dropping new edits when it fills. If you must restart,
take a per-document backup first (below) — that read comes from room memory,
which is the copy at risk. After storage recovers, verify and restore missing
state from a backup or surviving client copies; reconnect sync is not guaranteed
to recover an edit no client still holds.

### YcollabDatabaseSlow

`ycollab_store_duration_seconds` p99 over a second. A slow database becomes a
stalled document, because the room blocks once its write queue fills.

1. `SELECT query, state, wait_event_type, now() - query_start AS age FROM
   pg_stat_activity WHERE state <> 'idle' ORDER BY age DESC LIMIT 10;`
2. Look for `ycollab_compactions_total` climbing at the same time. Compaction
   writes a whole snapshot and deletes the folded rows; a document being edited
   hard compacts every `-compact-after` updates (500 by default). Raising it
   trades a longer replay at load time for fewer big writes.

### YcollabIntegrationSlow

`ycollab_apply_duration_seconds` p99 over 50 ms. A healthy server is in the
microseconds — the benchmarks put one `ApplyUpdate` at about 630 ns.

Almost always CPU starvation rather than the engine: check the pod's CPU
throttling before anything else. If the CPU is fine, a single document has grown
far beyond what a person edits by hand, and `ycollab_update_bytes` will show it.

### YcollabDroppingFrames

`ycollab_frames_dropped_total` climbing. Each drop closes a connection with
1008: a client could not keep up and was disconnected to protect the document.

1. Is it one client or many? A single one is usually a browser tab that has been
   backgrounded and throttled. The connection log lines carry `ip=`, so
   `grep 'dropping slow connection' | awk` over them answers whether it is one
   address or a population.
2. Many, on one replica, means that replica is CPU-bound and its rooms cannot
   drain their outbound queues. Check `ycollab_apply_duration_seconds` and the
   pod's CPU.

Clients reconnect and resync, so this is a quality problem rather than a
correctness one — but a client that is dropped repeatedly is a person whose
editor keeps freezing.

### YcollabThrottlingHeavily

`ycollab_throttled_seconds_total` growing by more than a second per second. The
rate limiter is holding connections back; they experience it as the document
going slow.

1. `ycollab_throttled_total` divided by `ycollab_messages_received_total` says
   whether it is everybody or somebody.
2. Everybody: `-rate-messages` (200/s) is too low for what these clients do.
   A person typing produces ten to thirty a second, so a whole population over
   the limit means an automated client.
3. Somebody: one connection is flooding. It is being slowed, not disconnected,
   and only its own traffic is affected.

### YcollabClusterRepairing

Anti-entropy is answering more than one state vector a second: the Redis fanout
is losing messages rather than delivering them. Clients on different replicas
see each other late.

1. Check `ycollab_cluster_publish_failed_total` — if that is also climbing, this
   is a connection problem, not a loss problem.
2. Redis Pub/Sub drops messages for a subscriber whose output buffer fills.
   `redis-cli info clients` and `client-output-buffer-limit pubsub` in the Redis
   config are where that shows.
3. The documents are still correct. Anti-entropy is the mechanism working; a
   sustained stream means it is working constantly.

### YcollabClusterPublishFailing

A replica cannot publish. Its clients' edits are not reaching the other
replicas, and will only be repaired if the connection comes back.

If Redis is gone entirely, each replica is internally consistent and mutually
blind. Two people on the same document, on different replicas, will not see each
other and will both keep editing — which converges when the bus returns, because
that is what a CRDT is for. Nothing is lost; it is confusing while it lasts.

### YcollabHooksDropped / YcollabHooksFailing

Events are being discarded because the delivery queue is full, or given up on
after their retries. Whatever the webhook feeds is missing edits.

1. Is the receiver up? `ycollab_hooks_failed_total{reason}` says `transport`
   (could not reach it), `server_error`, `throttled` or `rejected` (a 4xx — it
   is refusing the request, so check the signature and the body).
2. Dropped rather than failed means the queue filled: the receiver is answering,
   too slowly. Raise `-webhook-queue` or make the receiver faster.
3. The events are gone and nothing will retry them. To catch up, read the
   documents directly: `GET /documents/{name}`.

### YcollabAuthEndpointFailing

Only fires with `-auth-url`. The endpoint your application serves is not
answering, and every connection waits on it.

1. **First establish which way the server is failing**, because the two are
   opposite emergencies. `ycollab_auth_requests_total{result="error"}` climbing
   with `ycollab_connections_total` flat means connections are being refused —
   an outage, users locked out. The same counter climbing while connections keep
   being accepted means `-auth-fail-open` is set and **every client may currently
   read and write every document**. Check the flag on the running process before
   anything else.
2. Is the endpoint up at all? From a replica:
   `curl -sv -X POST -d '{}' <the -auth-url>`. A connection refused or a hang is
   the endpoint; a 404 is the next alert down, not this one.
3. `ycollab_auth_duration_seconds` p99 near `-auth-timeout` (default 3 s) means
   it is answering, too slowly, and each answer arrives after the server has
   given up.
4. If this will take a while to fix and users are locked out, `-auth-cache-ttl`
   does **not** help — it never had an answer to remember. The options are fixing
   the endpoint, or restarting with `-auth-fail-open` as a deliberate,
   time-boxed decision that documents are readable by anyone who can reach the
   server.

### YcollabAuthEndpointMisconfigured

The endpoint is reachable and answering something this server cannot act on: a
404 from the wrong path, a redirect, a 204, or a 200 whose body has no `allow`
field. Every connection is refused, and — unlike the alert above — this is
refused whatever `-auth-fail-open` says, so it will not clear on its own.

1. Almost always a wrong `-auth-url` or an endpoint that was redeployed behind a
   new path. Compare the flag on the running process against the route the
   application actually serves.
2. Reproduce with the body the server sends:

   ```sh
   curl -si -X POST -H 'Content-Type: application/json' \
     -d '{"document":"notes","token":"","ip":"127.0.0.1"}' <the -auth-url>
   ```

   A correct endpoint answers `200` with `{"allow":true|false, ...}`. A `301`
   here is the usual cause: the server does not follow redirects for this.
3. If the body is right but the server still refuses, check for `"allow"`
   specifically. `{"write":true}` is not an answer — the field is required, and
   its absence is deliberately not read as either yes or no.

## Common tasks

All on the admin listener, which defaults to `127.0.0.1:6060`. If the server was
started with `-admin-token`, every one of these needs the header — add
`-H "Authorization: Bearer $YCOLLAB_ADMIN_TOKEN"` to each `curl` below. Only
`/healthz` is open without it.

```bash
ADMIN=127.0.0.1:6060

# What is this node doing?
curl -s $ADMIN/statsz | jq

# Read a document. The bytes are a Yjs update; Y.applyUpdate reads them.
curl -s $ADMIN/documents/my-doc > my-doc.bin

# Read it as text.
curl -s "$ADMIN/documents/my-doc?format=json" | jq

# What subdocuments does it reference? They are separate documents, under their
# guids, and this is the only place they are named.
curl -s "$ADMIN/documents/my-doc?format=json" | jq -r '.subdocs[]'

# Delete it, from memory and from the database. Refused with 409 while somebody
# is editing it. This does NOT delete subdocuments - see below.
curl -s -X DELETE $ADMIN/documents/my-doc

# Merge an update into it - restore, or seed from a template.
curl -s -X POST --data-binary @my-doc.bin $ADMIN/documents/my-doc
```

`POST` **merges**, it does not replace. These are CRDT updates: applying one adds
what it carries to what is there. Restoring over a document that has moved on
gives the union of the two. `DELETE` first when that is not what you want.

## One address is misbehaving

`ycollab_denied_total{reason="too_many_connections"}` says the node is full;
`{reason="too_many_connections_per_ip"}` says one address hit its own limit.
Neither carries the address — that would make the metric's cardinality the
number of addresses on the internet — so the address comes from the logs:

```bash
kubectl logs -l app=ycollab --since=15m \
  | grep -E 'refusing a connection|dropping slow connection' \
  | grep -o 'ip=[^ ]*' | sort | uniq -c | sort -rn | head
```

If one address dominates, `-max-conns-per-ip` is the lever; it is off by
default and set to 100 in the manifests. Before believing the numbers, check
that `-trusted-proxies` is set for your topology — behind a load balancer
without it, every client is logged as the load balancer and the counts are
meaningless.

Blocking belongs upstream, in the ingress or the firewall. This server slows and
refuses; it does not keep a ban list, and a per-address block list in a process
that gets rolled every deploy is not one.

## Backup and restore

### The whole database

Everything is in two tables, so this is an ordinary Postgres backup.

```bash
pg_dump -U ycollab -d ycollab --format=custom --compress=9 > ycollab-$(date +%F).dump
```

Restoring:

```bash
psql  -U ycollab -d ycollab -c 'DROP TABLE IF EXISTS doc_updates, documents;'
pg_restore -U ycollab -d ycollab --no-owner --exit-on-error < ycollab-2026-07-31.dump
```

Two things worth knowing, both checked rather than assumed:

- **The identity sequence comes back.** `doc_updates.seq` is
  `GENERATED ALWAYS AS IDENTITY`, and the primary key is `(doc_id, seq)`. If a
  restore reset the sequence, the next write to any restored document would
  collide. It does not: after the round trip above, `max(seq)` and the
  sequence's `last_value` were both 76, and the next insert got 77. Verify it
  yourself after any restore:

  ```sql
  SELECT (SELECT max(seq) FROM doc_updates) AS max_seq,
         (SELECT last_value FROM pg_sequences WHERE sequencename = 'doc_updates_seq_seq') AS sequence;
  ```

- **Restore into a stopped cluster, or into an empty database.** A replica
  holding a document in memory does not reread it, so a restore underneath a
  running server is invisible until that room evicts — and when it does, it
  writes its own snapshot over what you restored.

The schema is created on startup if it is missing (`Migrate` is idempotent), so
restoring into a fresh database works without running anything by hand.

### One document

The read API is the backup, and the merge API is the restore. This is the
procedure `TestADocumentCanBeBackedUpAndRestored` runs on every CI build:

```bash
# 1. Take it. A resident document is read from the room, so this is exactly
#    what the connected clients are looking at.
curl -sf $ADMIN/documents/my-doc > my-doc.bin

# 2. Lose it, or corrupt it, or let somebody paste over it.

# 3. Put it back. DELETE first if the document has moved on and you want the
#    backup rather than the union of the two.
curl -sf -X DELETE $ADMIN/documents/my-doc
curl -sf -X POST --data-binary @my-doc.bin $ADMIN/documents/my-doc

# 4. Check.
curl -s "$ADMIN/documents/my-doc?format=json" | jq -r '.roots[].text'
```

A merge into a document people are already editing reaches them: the update is
broadcast to every connection and published to the other replicas, so nobody is
left building on a version the server no longer has.

### One document, back to what it said before

If `-version-interval` is set, the server has been keeping copies as the
document changed, and you do not need a backup file at all:

```bash
# 1. Find the moment. Labels are on the versions somebody took by hand.
curl -sf $ADMIN/documents/my-doc/versions | jq '.versions[] | {id, created_at, label, bytes}'

# 2. Read it out and check it before you commit to it.
curl -sf $ADMIN/documents/my-doc/versions/42 > yesterday.bin

# 3. Put it back. The DELETE is not optional - see below.
curl -sf -X DELETE $ADMIN/documents/my-doc
curl -sf -X POST --data-binary @yesterday.bin $ADMIN/documents/my-doc
```

**The `DELETE` is not optional.** `POST` merges, and a CRDT update cannot remove
what the document has since gained, so restoring without it gives you the union
of the good version *and* whatever you were trying to undo.

Take one before anything risky, and label it:

```bash
curl -sf -X POST "$ADMIN/documents/my-doc/versions?label=before+the+migration"
```

`201` means a version was written, `200` means the document has not changed
since the last one and there was nothing new to store. Both are success.

### Documents with subdocuments

A subdocument is a separate document under its guid, so neither backing up nor
deleting a parent touches them. Read the guids first and treat each as a
document in its own right:

```bash
for guid in $(curl -sf "$ADMIN/documents/my-book?format=json" | jq -r '.subdocs[]'); do
  curl -sf "$ADMIN/documents/$guid" > "$guid.bin"
done
```

The same list is what to walk before a delete. Nothing cascades: the server will
not remove documents the caller did not name.

Note this only finds the *direct* references. A subdocument can itself have
subdocuments, so walk the tree if your application nests them.

### What a backup does not cover

- **Awareness.** Cursors and presence are never persisted, by design. They are
  rebuilt in seconds by the clients that are still connected.
- **Unconfirmed edits.** In healthy operation, up to `-flush-interval` (200 ms)
  of edits may be in memory and not yet written. Database latency, failures, or
  queue backpressure can extend that window. `-durable-writes` waits for a bounded
  append retry sequence before relaying, but still relays after final failure;
  the failed batch is not retained for replay. Clients may hold a copy, but
  this is not guaranteed after they disconnect.
- **Redis.** Nothing in it needs backing up. It carries live fanout only.
- **Object storage**, if `-s3-bucket` is set. `pg_dump` no longer contains the
  documents: it holds the rows, and the rows name objects. **A database backup
  alone is not a backup.** Either use the bucket's own versioning and replication,
  or take both together and restore them together. A `pg_dump` restored next to a
  bucket that has moved on is a set of rows naming objects that are not there.

## Restarts and upgrades

A rolling restart is cheap and safe:

1. Connections get a 1001 close frame, not a dead socket, because the HTTP
   server is drained before the rooms are told to stop.
2. Every room writes a final snapshot before its goroutine returns. `-shutdown-timeout`
   (10 s) bounds the drain.
3. Clients reconnect, send their state vector, and get back a diff. A restart
   costs one diff per client.

Roll one replica at a time and watch `ycollab_rooms_resident` on the others:
documents move to whichever replica the clients land on.

Rolling back is the same operation. The wire format is Yjs v1 and the schema has
not changed shape, so an older binary reads what a newer one wrote.

## Losing a dependency

**Redis goes away.** The replicas keep serving. Each is internally consistent;
they stop seeing each other. Rooms that were already up log publish failures and
keep going. When it comes back, anti-entropy reconciles them within one
`-anti-entropy` interval (15 s). A room that starts while Redis is down refuses
the document with 1011 rather than serving a copy that silently ignores everyone
on the other replicas.

**Postgres goes away.** Rooms already in memory keep serving; nothing is lost
while the process lives. New documents cannot be loaded, so a client asking for
one that is not resident is closed with 1011. Writes queue and then block. See
YcollabWritesFailing above: do not restart.

## Capacity

Measured on one machine, single replica, so treat them as shape rather than
promise:

| Shape | Throughput | p99 | Notes |
| --- | --- | --- | --- |
| 1000 clients, 100 rooms | 35 694 updates/s | 511 µs | delivery ratio 1.0000 |
| 200 clients, 2 rooms | 98 003 updates/s | 1.93 ms | 0 dropped frames |

One `ApplyUpdate` is about 630 ns; encoding a document as an update is about
26 µs. The engine is not the limit — network and fanout are. These numbers were
taken with `go test -race` off; the race detector adds ~5-10x overhead and is
not representative of production latency.

Bounds a deployment should set, because the defaults are unlimited and that is
right for a laptop and wrong for anything reachable: `-max-conns`, `-max-rooms`
and `-max-memory`. The Kubernetes manifests in `deploy/k8s/` set all three, and
the README's [Memory](../README.md#memory) section explains how the byte budget
relates to the container limit.

## A document is wrong

Rare, and worth being careful with, because the recovery options destroy
evidence.

0. If `-version-interval` is set, the answer may already be stored: see
   "One document, back to what it said before" above. The rest of this section
   is for when it is not.
1. **Do not restart the replica.** Its memory may hold the only correct copy.
2. Take a backup of what it currently holds: `curl -sf $ADMIN/documents/my-doc > now.bin`.
   That read comes from the room.
3. Compare it with what the database has, without disturbing the room. Ask a
   replica that does *not* hold the document: `X-Ycollab-Resident: false` on
   the response proves the answer came from the store.

   ```bash
   curl -sD- -o stored.bin http://other-replica:6060/documents/my-doc | grep -i resident
   ```

   With one replica, read the database directly rather than evicting the room,
   which would overwrite the stored copy with the one you are investigating.
4. If the memory copy is right and the stored one is wrong, the fix is to let
   the room write out: it does that on eviction, which happens
   `-idle-timeout` (5 min) after the last client leaves, and on a clean
   shutdown.
5. If the stored copy is right, `DELETE` the document to drop the room, then
   `POST` the good bytes back.

Keep `now.bin`. A Yjs update is self-contained and applies to an empty document,
so it is a complete record of what the server believed at that moment.

## Who did that

The audit trail is on the server's **stdout**, one JSON object per line, or in
the file named by `-audit-log`. Wherever your logs land, these are `jq`
one-liners. The examples read a file; substitute your collector's query.

**Who deleted a document, and when.**

```sh
jq -r 'select(.action=="document.delete") | "\(.time) \(.document) \(.result) cred=\(.credential) from=\(.ip)"' audit.jsonl
```

**Everything that touched one document**, in order, including the reads:

```sh
jq -r 'select(.document=="my-doc") | "\(.time) \(.action) \(.result) \(.credential)"' audit.jsonl
```

**Somebody is trying credentials.** Every refusal, grouped by source:

```sh
jq -r 'select(.result=="denied") | .ip' audit.jsonl | sort | uniq -c | sort -rn
```

The trail never records the token that was tried, so there is nothing here to
correlate beyond the address and the timing. If one address is responsible, the
"One address is misbehaving" section above applies — but note that the admin
listener has no per-address cap of its own, because the answer for that surface
is not to route to it.

**Who read a heap profile.** A profile is a copy of every document the process is
holding, which makes this the least obvious way to read documents off the server:

```sh
jq -r 'select(.action=="profile.read") | "\(.time) \(.path) cred=\(.credential) from=\(.ip)"' audit.jsonl
```

### Finishing a token rotation

`-admin-token a,b` accepts both while holders are updated. The trail is how you
find out whether the old one has actually stopped being used before removing it:

1. Note the fingerprint of the old token: it is the first eight hex digits of its
   SHA-256.

   ```sh
   printf %s "$OLD_TOKEN" | sha256sum | cut -c1-8
   ```

2. Ask whether anything has used it lately, across every replica:

   ```sh
   jq -r --arg c "$(printf %s "$OLD_TOKEN" | sha256sum | cut -c1-8)" \
      'select(.credential==$c) | .time' audit.jsonl | tail -5
   ```

3. Nothing for a period longer than your slowest scheduled job — a nightly backup
   means waiting more than a day — means it is safe to drop from the flag and
   restart.

If step 2 returns lines you cannot account for, the old token is somewhere you
did not know about, and that is the answer the rotation was for.

### What is not in the trail

- **Editing.** Clients connecting and typing are not audited. That is per
  keystroke traffic, and the record of what a document contains is the document
  and its version history. What clients were refused is in
  `ycollab_denied_total` and the process log.
- **Who, as a person.** With one shared `-admin-token` the trail records which
  credential, not which human held it.
- **Content.** Document names and byte counts only, deliberately.
- **Successful `/metrics` scrapes**, which would be most of the file. Refused
  ones are recorded.

## Object storage

### Orphaned objects

An object nothing points at costs storage and nothing else, which is why the
write and delete order is arranged to produce those rather than the opposite.
They come from a failure between the two systems - a process killed between
writing an object and committing its row, or between deleting a row and deleting
its object.

Nothing accumulates while the server is working, so this is a periodic check
rather than an alert. To find them, list the bucket and compare against the rows:

```sh
# What the database says it owns.
psql "$DATABASE_URL" -t -A -c   "SELECT snapshot_key FROM documents WHERE snapshot_key <> ''
   UNION ALL
   SELECT blob_key FROM doc_versions WHERE blob_key <> ''" | sort > /tmp/referenced

# What is actually there. Strip the -s3-prefix if you set one.
mc ls --recursive myminio/ycollab/ | awk '{print $NF}' | sort > /tmp/present

comm -13 /tmp/referenced /tmp/present   # orphans: present, nothing points at them
comm -23 /tmp/referenced /tmp/present   # far worse: rows naming objects that are gone
```

The second list should be **empty**. Anything in it is a document or a version
that cannot be read, and it means something deleted objects out from under the
rows — a lifecycle rule on the bucket, or somebody clearing it by hand. Restore
those objects from the bucket's version history before anything else.

Delete orphans only after reading the first list and satisfying yourself the
server is not mid-write: a snapshot object is written before its row commits, so
an object created seconds ago may be perfectly legitimate.

### "Documents will not load and the database is fine"

With `-s3-bucket` set, the bucket is a hard dependency for reading any document
whose bytes are there.

1. `ycollab_store_failed_total{op="load"}` climbing while the database is healthy
   points here.
2. The process log carries the object key and the underlying error. `403` is
   credentials or a bucket policy; `404` is the object missing, which is the
   serious case above; a timeout is the endpoint.
3. From a replica: `curl -sI "$S3_ENDPOINT/$BUCKET/"` says whether the address is
   reachable at all.
4. A server started **without** `-s3-bucket` against a database whose rows name
   objects reports `is in object storage, and this server has none configured`.
   That is a configuration mistake, not an outage — the flag was dropped.

### Turning object storage on, or off

On: add the flags and restart. Nothing migrates. The next snapshot and the next
version of each document go to the bucket; everything already in the database
stays there and stays readable. There is no moment where the two disagree.

Off: remove the flags and restart, **but the objects already written stay
authoritative** — the rows still name them, and those documents will not load.
There is no built-in migration back. If you need one, read each affected document
through `GET /documents/{name}` while the bucket is still configured and `POST` it
back to a server without it.

## Multi-tenancy

### Turning it on for a database that predates it

Every existing document is owned by nobody. Tokens that already worked keep
working; tokens carrying an `own` claim reach none of them. Nothing breaks and
nothing moves on its own — which is the point, but it does mean the move is
yours to make.

1. See what is there. Documents owned by nobody are `owner=` with an empty
   value:

   ```sh
   curl -s -H "Authorization: Bearer $TOKEN" \
     "http://127.0.0.1:6060/documents?owner=&limit=1000"
   ```

2. Rows written before the server recorded names come back with `"name": ""`.
   They can be read by id but not opened, because the id is a one-way hash of the
   name. If your application knows the names, `PUT .../owner` records the name as
   it moves the document. If it does not, the names are recoverable only from
   your own records — the server cannot invert the hash.

3. Move them one at a time, which also records the name:

   ```sh
   curl -X PUT -H "Authorization: Bearer $TOKEN" -d '{"owner":"acme"}' \
     http://127.0.0.1:6060/documents/notes/owner
   ```

   A `409` means somebody is editing it. The room is refused rather than moved
   under them; retry after the connections close, or after `-idle-timeout`.

4. For a bulk move where every document belongs to one tenant, SQL is honest and
   faster than a thousand requests. **Take a backup first, and do it while the
   server is stopped** — a running replica holds owners in memory for its
   resident rooms and will not notice:

   ```sql
   -- The owner id is a UUIDv5 of the tenant name under ycollab's owner
   -- namespace. Get it from the server rather than deriving it by hand: open one
   -- document as that tenant and read owner_id out of the listing.
   UPDATE documents SET owner_id = '<the owner_id from a listing>'
   WHERE owner_id = '00000000-0000-0000-0000-000000000000';
   ```

### Deleting everything belonging to one account

Paging is keyset, so it is safe to delete as you go — rows leaving the set do not
shift the ones after them.

```sh
after=""
while :; do
  page=$(curl -s -H "Authorization: Bearer $TOKEN" \
    "http://127.0.0.1:6060/documents?owner=acme&limit=100&after=$after")
  echo "$page" | jq -r '.documents[].name' | while read -r name; do
    [ -n "$name" ] || continue
    curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
      "http://127.0.0.1:6060/documents/$name"
  done
  after=$(echo "$page" | jq -r '.next // empty')
  [ -n "$after" ] || break
done
```

A `409` on a `DELETE` means somebody is still connected to that document. Deal
with those after the loop rather than blocking it.

Every one of those deletions is in the audit trail as `document.delete`, which is
what you show somebody who asks whether the request was carried out.

### "A tenant says they cannot open their document"

1. Is the refusal about tenancy at all? The client is told the same thing a bad
   token is told, deliberately — so the client's report cannot distinguish them.
   The server's log line can: look for `refusing a connection: the document
   belongs to another owner`, which carries `room=`, `ip=` and `owner=`.
2. Compare who they claim to be with who owns it:

   ```sh
   curl -s -H "Authorization: Bearer $TOKEN" \
     "http://127.0.0.1:6060/documents?limit=1000" | jq -r '.documents[] | select(.name=="notes")'
   ```

   The `owner_id` there is the hash of the tenant name. To check a guess, open
   any document as that tenant and compare the `owner_id` the listing reports.
3. The usual cause is a document created before the application started sending
   the `own` claim — it is owned by nobody, and the now-tenanted token cannot
   reach it. `PUT .../owner` is the fix.
4. The other cause is two tenants that genuinely want the same document name.
   They cannot have it: names are global. Namespace them in the application
   (`acme/notes` is not allowed — the path is the whole name — but `acme-notes`
   is).

## YcollabOverMemoryBudget

`-max-memory` is set and the replica cannot evict its way back under it.

The budget is not a hard guarantee, and this alert is what that means in
practice: **a room with somebody connected to it is never evicted.** Firing means
every resident document is busy. The server keeps serving; the next thing to
happen, if it keeps growing, is the out-of-memory killer.

1. How far over, and is it growing?

   ```
   ycollab_rooms_resident_bytes / ycollab_max_memory_bytes
   ```

   Flat and slightly over is a budget set a little too tight. Climbing steadily
   is documents growing, and it will not stop on its own.

2. `ycollab_rooms_evicted_total{reason="budget"}` flat while the gauge is over
   confirms it: nothing is evictable.
3. `ycollab_rooms_resident` divided into the byte figure gives the average
   document. A handful of very large documents is a different problem from a lot
   of ordinary ones — the eviction log lines carry `bytes=` and `structs=` per
   room, so `grep 'evicting room'` ranks them.

**What to do, in order of how quickly it works:**

- Raise `-max-memory` if the pod has headroom. This is a bound, not a
  measurement: setting it above what the container allows just moves the failure.
- Add replicas. Documents move to whichever replica their clients land on, so
  more replicas spread the load — but each still holds whatever it is asked for,
  which is the next point.
- **Route each document to one replica consistently.** This is the real fix and
  it needs no change here: with an ingress that hashes on the URL path (`hash
  $uri consistent` in nginx), N replicas hold N times the documents rather than
  each holding an arbitrary share. A hash that is occasionally wrong costs a
  duplicate copy, not a correctness problem.

**What not to do:** lowering `-idle-timeout` does not help while the rooms are
busy — idle eviction only applies to rooms nobody is in, which is precisely the
set that is already empty when this fires.
