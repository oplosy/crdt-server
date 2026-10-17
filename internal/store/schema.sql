-- ycollab persistence schema.
--
-- The shape is the brief's: a document row carrying the latest snapshot, and an
-- append-only log of updates that arrived after it. Loading a document is the
-- snapshot followed by every remaining log row, in seq order.

-- New document incarnations need identifiers that are not reused after a row
-- is deleted and recreated. Legacy rows retain zero; callers compare tokens
-- for equality and do not rely on sequence continuity.
CREATE SEQUENCE IF NOT EXISTS document_incarnation_seq;

CREATE TABLE IF NOT EXISTS documents (
    id            UUID PRIMARY KEY,
    owner_id      UUID NOT NULL,
    -- The name the document is reached by, which is the URL path clients open.
    -- The id is derived from it by a one-way hash, so without this column a
    -- listing could return identifiers and nothing anybody could open. Empty
    -- for rows written before this column existed; see the runbook.
    name          TEXT NOT NULL DEFAULT '',
    snapshot      BYTEA,
    -- How the snapshot is encoded: 0 is the bytes as they came, 1 is deflate.
    -- A column rather than a marker byte inside the blob, because a Yjs update
    -- begins with a varUint that can be any value - there is no prefix that
    -- could not also be a legitimate document. See internal/pack.
    snapshot_codec SMALLINT NOT NULL DEFAULT 0,
    -- Where the snapshot lives. Empty means the snapshot column beside it;
    -- anything else is a key in object storage. Two columns rather than a mode
    -- flag on the server, so a database can hold both at once: turning object
    -- storage on does not migrate anything, and the rows written before it stay
    -- readable by the same query.
    snapshot_key  TEXT NOT NULL DEFAULT '',
    -- How many bytes the object holds, for the rows whose snapshot_key is set.
    -- Without it a listing would report octet_length(snapshot), which is zero
    -- for exactly those rows - so the documents that cost the most storage
    -- would be the ones reported as costing nothing. Meaningless when
    -- snapshot_key is empty: the column beside it already knows its own size.
    snapshot_bytes BIGINT NOT NULL DEFAULT 0,
    snapshot_seq  BIGINT NOT NULL DEFAULT 0,
    snapshot_generation BIGINT NOT NULL DEFAULT 0,
    incarnation   BIGINT NOT NULL DEFAULT nextval('document_incarnation_seq'),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- For databases that predate the column. Migrate runs the whole file on every
-- boot, so every statement here has to be safe to run again.
ALTER TABLE documents ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS snapshot_codec SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS snapshot_key TEXT NOT NULL DEFAULT '';
-- Rows written before this column report zero until they are compacted again,
-- which is a listing that understates a document rather than one that cannot be
-- read. Backfilling is impossible from here: only the bucket knows the size.
ALTER TABLE documents ADD COLUMN IF NOT EXISTS snapshot_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS snapshot_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS incarnation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE documents ALTER COLUMN incarnation SET DEFAULT nextval('document_incarnation_seq');

-- Listing is always "this owner's documents, by name". The owner comes first
-- because it is the equality, and multi-tenancy makes it the selective one.
CREATE INDEX IF NOT EXISTS documents_by_owner ON documents (owner_id, name, id);

CREATE TABLE IF NOT EXISTS doc_updates (
    doc_id      UUID   NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    seq         BIGINT GENERATED ALWAYS AS IDENTITY,
    payload     BYTEA  NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (doc_id, seq)
);

-- Version history: the document as it stood, at points in time.
--
-- Each row is a complete Yjs update rather than a diff from the row before it.
-- A chain would be smaller and would make reading one version a walk through
-- every version before it, with no way to drop an old one - which is the
-- operation retention is made of. What keeps the size honest instead is
-- state_vector: a version is only written when it differs from the newest one,
-- so a document nobody edited gets one row however long the timer runs.
CREATE TABLE IF NOT EXISTS doc_versions (
    doc_id        UUID   NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    id            BIGINT GENERATED ALWAYS AS IDENTITY,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The version of the document, used to skip writing one that would say
    -- nothing new, and offered to callers so they can tell two versions apart
    -- without downloading both.
    state_vector  BYTEA  NOT NULL,
    payload       BYTEA  NOT NULL,
    -- How payload is encoded; see documents.snapshot_codec.
    codec         SMALLINT NOT NULL DEFAULT 0,
    -- Where the payload lives; see documents.snapshot_key. When this is set the
    -- payload column is empty, and the row is the only thing that knows the
    -- object exists - which is why deletion drops the row first and the object
    -- second. An orphaned object costs storage; a row pointing at nothing is a
    -- version that cannot be read.
    blob_key      TEXT NOT NULL DEFAULT '',
    -- The object's size, for the rows whose blob_key is set; see
    -- documents.snapshot_bytes for why a listing cannot work it out otherwise.
    blob_bytes    BIGINT NOT NULL DEFAULT 0,
    -- Set when somebody asked for this version by hand, empty for the ones the
    -- timer took. It is what makes "before the migration" findable in a list of
    -- timestamps.
    label         TEXT   NOT NULL DEFAULT '',
    PRIMARY KEY (doc_id, id)
);

ALTER TABLE doc_versions ADD COLUMN IF NOT EXISTS codec SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE doc_versions ADD COLUMN IF NOT EXISTS blob_key TEXT NOT NULL DEFAULT '';
ALTER TABLE doc_versions ADD COLUMN IF NOT EXISTS blob_bytes BIGINT NOT NULL DEFAULT 0;

-- Listing and pruning both want the newest first for one document.
CREATE INDEX IF NOT EXISTS doc_versions_recent ON doc_versions (doc_id, id DESC);
