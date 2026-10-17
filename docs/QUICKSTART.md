# Five minutes

A running server, two browser tabs editing the same document, and then the one
flag that stops it being open to everybody. Nothing is built and no Go toolchain
is needed.

## 1. Start it

```sh
docker compose -f deploy/docker-compose.quickstart.yml up
```

That is the server and a PostgreSQL for it to keep documents in. When the log
says `listening`, it is ready:

```sh
curl localhost:8080/healthz     # ok
```

It will also warn that **every client may read and write every document**. That
is true and it is step 4.

Without Docker, and with Go installed, the equivalent is:

```sh
go run ./cmd/server -addr :8080 -origins localhost:5173
```

which works too — a document just lives only as long as somebody is connected to
it, because there is no database.

## 2. Edit it in two tabs

```sh
cd web && npm install && npm run dev
```

Open <http://localhost:5173/#hello> twice and type in both. The header shows the
connection state, how many people are in the document, and its state vector —
which will be identical in the two tabs, because that is the thing being
demonstrated.

The demo is ordinary TipTap over `y-websocket` with nothing ycollab-specific in
it. The document name is the URL fragment, so `#notes` is a different document.

## 3. Talk to it from your own code

The document name is the path, so `ws://localhost:8080/notes` is the document
`notes`. Any `y-websocket` client works, unchanged:

```js
import * as Y from 'yjs'
import { WebsocketProvider } from 'y-websocket'

const doc = new Y.Doc()
new WebsocketProvider('ws://localhost:8080', 'notes', doc)
doc.getText('body').insert(0, 'hello')
```

And from a shell, through the admin port — the same document, as JSON:

```sh
curl -s localhost:6060/documents/notes | head -c 200
```

## 4. Close it

Everything above is open to anyone who can reach port 8080. Two flags fix that,
and neither needs a code change.

**A signing key**, so a client needs a token naming the document it may open:

```sh
export YCOLLAB_JWT_SECRET=$(openssl rand -hex 32)
```

Add it to the server — in the compose file, under `command:`:

```yaml
      - -jwt-secret=${YCOLLAB_JWT_SECRET}
      - -admin-token=${YCOLLAB_ADMIN_TOKEN}
```

and pass both through from your shell:

```sh
export YCOLLAB_ADMIN_TOKEN=$(openssl rand -hex 32)
docker compose -f deploy/docker-compose.quickstart.yml up
```

`-admin-token` is the second one, and it matters as much: the admin port can
rewrite and delete any document, and `/debug/pprof` will dump the heap, which is
a copy of every document the process is holding.

Now mint a token and open the demo with it:

```sh
go run ./cmd/ycollab-token -doc hello -perm write -ttl 1h
```

```
http://localhost:5173/?token=<the token>#hello
```

A token names one document and expires, so one that leaks opens one document
until it does. In a real deployment the application that knows who its users are
mints these — it holds the same secret and signs
`{ "doc": "notes", "perm": "write", "exp": … }` with HS256. The `doc` claim is
matched verbatim against the URL path, so a token for `hello` cannot open
`notes`. If you would rather your application answer a question per connection
than mint tokens, that is `-auth-url`, in the README.

## 5. The client package, if you want it

Optional, and worth knowing why it exists before installing it. `y-websocket`
reconnects forever with the token it was given, so a document quietly stops
syncing when that token expires; its permission-denied handler is a
`console.warn` your code cannot replace, so a refusal is invisible; and
subdocuments need a provider and a token each.

```sh
npm install ycollab-client yjs y-websocket
```

```js
import { connect } from 'ycollab-client'

const client = connect({
  url: 'ws://localhost:8080',
  name: 'notes',
  token: (document) => fetch(`/api/collab-token?doc=${document}`).then((r) => r.text())
})
client.on('denied', ({ reason }) => showBanner(reason))
```

[client/README.md](../client/README.md) has the rest.

## Where to go next

- [README](../README.md) — every flag, and what each one is for.
- [Running it for real](../README.md#running-it-for-real) — the Kubernetes
  manifests, which set the limits this quickstart leaves off (`-max-conns`,
  `-max-rooms`, `-max-memory`, `-trusted-proxies`).
- [RUNBOOK](RUNBOOK.md) — what to do when it misbehaves, including backup
  and restore transcripts.
- [DECISIONS](../DECISIONS.md) — why it is built this way, and what is still
  open.

## Two things this does not do

It has no users and has never run in production. The engineering is checked —
byte-for-byte against real Yjs, under the race detector against real PostgreSQL,
Redis and RustFS — but that is a different claim from "proven by traffic", and
you should read [Limits](../README.md#limits) before putting it in front of
anybody.

And a replica holds every document it serves, entirely, in memory. `-max-memory`
bounds and reports that; it does not remove it. If you expect more documents than
one machine's memory, route each document to one replica consistently — any
ingress that can hash on the URL path does it.
