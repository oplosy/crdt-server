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