# Persistence and delivery guarantees

Ycollab keeps document state in several places with different failure properties. Treating them as equivalent can turn a recoverable outage into data loss.

| Copy | Role | Failure behavior |
| --- | --- | --- |
| Resident room memory | Active document state on one replica | Lost when the process exits before its writes are stored |
| PostgreSQL snapshot and append-only update log | Durable document history | Source used to restore a room after restart |
| Redis Pub/Sub | Cross-replica live fanout | Messages may be lost; anti-entropy repairs replica differences |
| Connected clients | Local replicas of the document | Clients reconnect and resync after a server restart |

Without -database-url, a document lasts only while its room remains resident. With PostgreSQL, updates are batched by default; a crash can lose edits from the last flush interval that are still present in clients and can be resent on reconnect.

-durable-writes attempts to store each update before relaying it, reducing the unflushed window. A failed store attempt is logged and counted but the update is still relayed, so the option does not make editing fail closed when PostgreSQL is unavailable. Monitor ycollab_store_failed_total and follow the runbook before restarting replicas during a storage failure.

Compaction persists a rebased CRDT state. Before replacing a snapshot, the room writer loads the current snapshot and every remaining update row, merges those with its immutable candidate in a temporary document, and asks PostgreSQL to replace the snapshot only if its owner, document incarnation, and snapshot generation still match. A generation conflict triggers a bounded reload-and-merge retry. Successful compaction advances the generation even when `snapshot_seq` stays the same, and removes only the exact update rows included in the replacement. Unreadable updates or unresolved Yjs dependencies prevent pruning; the pending candidate is retried after a later successful update batch, a newer compaction request, or during shutdown.

Room appends, version saves, and version pruning are guarded by the owner and incarnation captured when the room loaded the document. The parent row stays locked through each write, so deletion/recreation or an owner transfer cannot redirect an old room's operation to the replacement. `SetOwner` rotates the incarnation only when the owner actually changes; a no-op assignment preserves it, while A→B→A cannot revive an old room.

Internal Redis envelopes carry an explicit persisted/no-database marker and the full signed 64-bit incarnation. Rooms filter updates, awareness, and state-vector messages after loading, so envelopes queued during startup are checked against the identity they actually loaded. The public Yjs client protocol is unchanged. The internal envelope format is versioned; old private envelopes are rejected, and all replicas must be upgraded together. A stale room can still retain local clients until eviction; this does not guarantee immediate remote eviction or control state already held by clients.

The change does not add client-visible persisted acknowledgements or retry failed appends. `-durable-writes` waits for the append attempt, then preserves the existing relay behavior even if storage failed; Yjs sync messages are not persistence acknowledgements. A failed resident administrative merge returns an error and is not broadcast, but the room's in-memory CRDT has already integrated that update and is not rolled back. The lifetime fence covers Append, version save/prune, and internal fanout filtering; it does not provide generic retry or recovery for database outages.

Each snapshot attempt writes to a unique immutable object key. A known stale compare-and-swap can remove its unreferenced candidate object; an ambiguous commit error retains it because PostgreSQL may have committed a row that names it. Unreferenced objects can accumulate after failures and require operational cleanup.
