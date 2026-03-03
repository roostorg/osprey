-- Seed default saved queries for Divine Osprey (Nostr moderation)
-- Run: docker exec -i divine-postgres psql -U osprey -d osprey < divine/scripts/seed-saved-queries.sql
--
-- Idempotent: uses ON CONFLICT DO NOTHING.
-- Re-seed with fresh date ranges: DELETE FROM saved_queries WHERE id BETWEEN 900000000000100001 AND 900000000000100020;
--                                  DELETE FROM queries WHERE id BETWEEN 900000000000000001 AND 900000000000000020;
--                                  Then re-run this script.

-- Ensure tables exist (normally created by SQLAlchemy at runtime)
CREATE TABLE IF NOT EXISTS queries (
    id BIGINT PRIMARY KEY NOT NULL,
    parent_id BIGINT,
    executed_by TEXT NOT NULL,
    query_filter TEXT NOT NULL,
    date_range TSRANGE NOT NULL,
    top_n TEXT[] NOT NULL,
    sort_order VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS saved_queries (
    id BIGINT PRIMARY KEY NOT NULL,
    query_id BIGINT NOT NULL REFERENCES queries(id),
    name TEXT NOT NULL,
    saved_by TEXT NOT NULL
);

-- ═══════════════════════════════════════════════════════════════
-- Overview queries
-- ═══════════════════════════════════════════════════════════════

-- 1. All Events by Action — what's flowing through the pipeline
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000001, NULL, 'system@divine.video', '',
    tsrange((NOW() - INTERVAL '7 days')::timestamp, NOW()::timestamp),
    ARRAY['ActionName'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100001, 900000000000000001, 'All Events by Action', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 2. Events by Kind — Nostr event kind distribution
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000002, NULL, 'system@divine.video', '',
    tsrange((NOW() - INTERVAL '7 days')::timestamp, NOW()::timestamp),
    ARRAY['Kind'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100002, 900000000000000002, 'Events by Kind', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 3. Top Pubkeys — most active posters
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000003, NULL, 'system@divine.video', '',
    tsrange((NOW() - INTERVAL '7 days')::timestamp, NOW()::timestamp),
    ARRAY['Pubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100003, 900000000000000003, 'Top Pubkeys', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════
-- Kind-specific queries
-- ═══════════════════════════════════════════════════════════════

-- 4. Kind 1 — Text Notes by Pubkey
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000004, NULL, 'system@divine.video', 'Kind == 1',
    tsrange((NOW() - INTERVAL '7 days')::timestamp, NOW()::timestamp),
    ARRAY['Pubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100004, 900000000000000004, 'Kind 1 — Text Notes', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 5. Kind 1984 — Moderation Reports by Reason
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000005, NULL, 'system@divine.video', 'Kind == 1984',
    tsrange((NOW() - INTERVAL '30 days')::timestamp, NOW()::timestamp),
    ARRAY['ReportReason'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100005, 900000000000000005, 'Kind 1984 — Reports by Reason', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 6. Kind 1984 — Reports by Reported Pubkey
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000006, NULL, 'system@divine.video', 'Kind == 1984',
    tsrange((NOW() - INTERVAL '30 days')::timestamp, NOW()::timestamp),
    ARRAY['ReportedPubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100006, 900000000000000006, 'Kind 1984 — Most Reported Pubkeys', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════
-- Rule hit queries (moderation signals)
-- ═══════════════════════════════════════════════════════════════

-- 7. New Account Spam — flagged new accounts
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000007, NULL, 'system@divine.video', 'NewAccountSpam == 1',
    tsrange((NOW() - INTERVAL '7 days')::timestamp, NOW()::timestamp),
    ARRAY['Pubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100007, 900000000000000007, 'New Account Spam', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 8. Rapid Posting — rate-limited accounts
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000008, NULL, 'system@divine.video', 'RapidPosting == 1',
    tsrange((NOW() - INTERVAL '7 days')::timestamp, NOW()::timestamp),
    ARRAY['Pubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100008, 900000000000000008, 'Rapid Posting', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 9. Repeat Offenders — previously warned
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000009, NULL, 'system@divine.video', 'PreviouslyWarned == 1',
    tsrange((NOW() - INTERVAL '30 days')::timestamp, NOW()::timestamp),
    ARRAY['Pubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100009, 900000000000000009, 'Repeat Offenders — Previously Warned', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 10. Suspended Users — currently suspended
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000010, NULL, 'system@divine.video', 'PreviouslySuspended == 1',
    tsrange((NOW() - INTERVAL '30 days')::timestamp, NOW()::timestamp),
    ARRAY['Pubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100010, 900000000000000010, 'Suspended Users', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 11. Trusted Reporter CSAM Flags
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000011, NULL, 'system@divine.video', 'TrustedReporterCSAM == 1',
    tsrange((NOW() - INTERVAL '30 days')::timestamp, NOW()::timestamp),
    ARRAY['ReportedPubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100011, 900000000000000011, 'Trusted Reporter — CSAM', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;

-- 12. Trusted Reporter NSFW Flags
INSERT INTO queries (id, parent_id, executed_by, query_filter, date_range, top_n, sort_order)
VALUES (
    900000000000000012, NULL, 'system@divine.video', 'TrustedReporterNSFW == 1',
    tsrange((NOW() - INTERVAL '30 days')::timestamp, NOW()::timestamp),
    ARRAY['ReportedPubkey'], 'DESCENDING'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO saved_queries (id, query_id, name, saved_by)
VALUES (900000000000100012, 900000000000000012, 'Trusted Reporter — NSFW', 'system@divine.video')
ON CONFLICT (id) DO NOTHING;
