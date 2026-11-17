import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { openDatabase, closeDatabase } from "../../src/store/database.js";

const V1_SCHEMA = `
  CREATE TABLE feeds (feed_id TEXT PRIMARY KEY, base_url TEXT NOT NULL, allowed_hosts_json TEXT NOT NULL,
    minimum_items INTEGER NOT NULL, missing_threshold INTEGER NOT NULL, last_accepted_at TEXT,
    last_item_count INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
  CREATE TABLE captures (capture_id INTEGER PRIMARY KEY, feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
    fingerprint TEXT NOT NULL, captured_at TEXT NOT NULL, accepted INTEGER NOT NULL, candidate_count INTEGER NOT NULL,
    normalized_count INTEGER NOT NULL, rejected_count INTEGER NOT NULL, duplicate_count INTEGER NOT NULL, reason TEXT,
    UNIQUE(feed_id, fingerprint));
  CREATE TABLE articles (article_id INTEGER PRIMARY KEY, feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
    canonical_url TEXT NOT NULL, title TEXT NOT NULL, published_at TEXT, first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL, seen_count INTEGER NOT NULL DEFAULT 1, missing_streak INTEGER NOT NULL DEFAULT 0,
    visibility TEXT NOT NULL DEFAULT 'visible', review_state TEXT NOT NULL DEFAULT 'unreviewed',
    review_note TEXT, review_revision INTEGER NOT NULL DEFAULT 0, UNIQUE(feed_id, canonical_url));
  CREATE TABLE capture_items (capture_id INTEGER NOT NULL REFERENCES captures(capture_id) ON DELETE CASCADE,
    article_id INTEGER NOT NULL REFERENCES articles(article_id), source_index INTEGER NOT NULL, PRIMARY KEY(capture_id, article_id));
  CREATE TABLE fetch_attempts (attempt_id INTEGER PRIMARY KEY, feed_id TEXT NOT NULL, started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN ('accepted','rejected','not_modified','failed')),
    http_status INTEGER, attempt_count INTEGER NOT NULL, requested_url TEXT NOT NULL, final_url TEXT,
    capture_id INTEGER, error_code TEXT, error_message TEXT);
  CREATE TABLE review_events (event_id INTEGER PRIMARY KEY, article_id INTEGER NOT NULL, prior_state TEXT NOT NULL,
    next_state TEXT NOT NULL, note TEXT, revision INTEGER NOT NULL, changed_at TEXT NOT NULL);
  PRAGMA user_version = 1;
  INSERT INTO feeds VALUES ('f','https://s.test/i','["s.test"]',3,3,NULL,0,'t0','t0');
  INSERT INTO fetch_attempts VALUES (1,'f','t0','t1','failed',503,3,'https://s.test/i',NULL,NULL,'fetch_failed','boom');
`;

test("migrates a v1 audit table, preserving history and accepting cancelled outcomes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "news-migration-"));
  const filename = join(directory, "archive.sqlite");
  try {
    const legacy = new DatabaseSync(filename);
    legacy.exec(V1_SCHEMA);
    legacy.close();

    const database = openDatabase(filename);
    assert.equal(database.prepare("PRAGMA user_version").get().user_version, 2);
    const preserved = database.prepare("SELECT * FROM fetch_attempts WHERE attempt_id = 1").get();
    assert.equal(preserved.outcome, "failed");
    assert.equal(preserved.error_code, "fetch_failed");

    database.prepare(`INSERT INTO fetch_attempts
      (feed_id, started_at, finished_at, outcome, http_status, attempt_count, requested_url, final_url,
       capture_id, error_code, error_message)
      VALUES ('f','t2','t3','cancelled',NULL,1,'https://s.test/i',NULL,NULL,'refresh_aborted','cancelled')`).run();
    assert.equal(database.prepare("SELECT COUNT(*) AS total FROM fetch_attempts").get().total, 2);
    closeDatabase(database);

    // Reopening an already-migrated database is a no-op.
    const reopened = openDatabase(filename);
    assert.equal(reopened.prepare("PRAGMA user_version").get().user_version, 2);
    closeDatabase(reopened);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
