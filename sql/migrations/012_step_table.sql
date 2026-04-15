-- +migrate Up

CREATE TABLE

INSERT INTO "utask_sql_migrations" VALUES ('v1.22.0-migration012');

CREATE TABLE "step" (
    id BIGSERIAL PRIMARY KEY,
    resolution_id BIGINT REFERENCES "resolution"(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    state TEXT NOT NULL,
    try_count INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    last_run TIMESTAMP with time zone,
    idempotent BOOLEAN NOT NULL,

    encrypted_step_data BYTEA
);

-- +migrate Down

DROP TABLE IF EXISTS "step";

DELETE FROM "utask_sql_migrations" WHERE current_migration_applied = 'v1.22.0-migration012';