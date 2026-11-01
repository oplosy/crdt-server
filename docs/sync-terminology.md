# Sync Terminology

Shared vocabulary for the CRDT server, so client code, tests, and docs match.

- **Replica** — one cooperating client or server instance producing updates.
- **Update** — an atomic set of operations exchanged between replicas.
- **State vector** — per-replica summary of which updates have been seen.
- **Convergence** — the guarantee that all replicas end up with the same content.
- **Delta** — the minimal information needed to bring one replica up to date with another.

When changing any of these behaviors, update the schema version together with this glossary.
