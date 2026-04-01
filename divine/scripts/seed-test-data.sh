#!/bin/bash
# Seed realistic test data for Osprey local development
#
# Prerequisites:
#   - Kind cluster with Funnelcake running (ws://localhost:4444)
#   - Osprey stack running (docker compose up -d)
#   - nak installed (~/go/bin/nak)
#
# Creates 4 test identities and publishes events that exercise every rule:
#   1. Trusted reporter CSAM/NSFW reports (auto_hide.sml)
#   2. Label routing: confirmed nudity, CSAM, AI-generated, rejected (label_routing.sml)
#   3. New account spam (new_account_spam.sml)
#   4. Rapid posting (rapid_posting.sml) -- depends on new_account_activity label from #3
#   5. Repeat offender (repeat_offender.sml) -- depends on warned label

set -e

NAK="${NAK:-$HOME/go/bin/nak}"
RELAY="${RELAY:-ws://localhost:4444}"
POSTGRES_CMD="docker compose exec -T postgres psql -U osprey -d osprey"

echo "=== Generating test identities ==="

# Generate 4 keypairs
ADMIN_SEC=$(jq -r '.secretKey' ~/code/divine-relay-test/.admin-key.json)
ADMIN_NSEC=$($NAK encode nsec "$ADMIN_SEC")
ADMIN_PUB=$($NAK encode pubkey "$ADMIN_SEC" 2>/dev/null || $NAK decode nsec "$ADMIN_NSEC" 2>/dev/null | head -1)

# Generate fresh keypairs for test actors
REPORTER_SEC=$($NAK genkey 2>/dev/null || openssl rand -hex 32)
REPORTER_NSEC=$($NAK encode nsec "$REPORTER_SEC")
REPORTER_PUB=$($NAK encode pubkey "$REPORTER_SEC" 2>/dev/null || echo "")

MODERATOR_SEC=$($NAK genkey 2>/dev/null || openssl rand -hex 32)
MODERATOR_NSEC=$($NAK encode nsec "$MODERATOR_SEC")
MODERATOR_PUB=$($NAK encode pubkey "$MODERATOR_SEC" 2>/dev/null || echo "")

SPAMMER_SEC=$($NAK genkey 2>/dev/null || openssl rand -hex 32)
SPAMMER_NSEC=$($NAK encode nsec "$SPAMMER_SEC")
SPAMMER_PUB=$($NAK encode pubkey "$SPAMMER_SEC" 2>/dev/null || echo "")

# If nak encode pubkey doesn't work, extract from event
if [ -z "$REPORTER_PUB" ]; then
    REPORTER_PUB=$($NAK event --sec "$REPORTER_NSEC" -k 0 -c '{}' 2>/dev/null | jq -r '.pubkey')
    MODERATOR_PUB=$($NAK event --sec "$MODERATOR_NSEC" -k 0 -c '{}' 2>/dev/null | jq -r '.pubkey')
    SPAMMER_PUB=$($NAK event --sec "$SPAMMER_NSEC" -k 0 -c '{}' 2>/dev/null | jq -r '.pubkey')
fi

echo "  Admin:     ${ADMIN_PUB:0:16}..."
echo "  Reporter:  ${REPORTER_PUB:0:16}..."
echo "  Moderator: ${MODERATOR_PUB:0:16}..."
echo "  Spammer:   ${SPAMMER_PUB:0:16}..."

echo ""
echo "=== Setting up entity labels in Postgres ==="

# Mark reporter as trusted_reporter
cd ~/code/osprey/divine
$POSTGRES_CMD -c "
INSERT INTO entity_labels (entity_key, labels)
VALUES (
  'Pubkey/$REPORTER_PUB',
  '{\"labels\": {\"trusted_reporter\": {\"status\": 1, \"reasons\": {\"manual\": {\"pending\": false, \"features\": {}, \"created_at\": \"2026-01-01T00:00:00+00:00\", \"description\": \"Manually designated trusted reporter\"}}}}}'::jsonb
)
ON CONFLICT (entity_key) DO UPDATE SET labels = EXCLUDED.labels;
" 2>/dev/null
echo "  Marked reporter as trusted_reporter"

# Mark admin as verified (so NewAccountSpam doesn't fire on admin events)
$POSTGRES_CMD -c "
INSERT INTO entity_labels (entity_key, labels)
VALUES (
  'Pubkey/$ADMIN_PUB',
  '{\"labels\": {\"verified\": {\"status\": 1, \"reasons\": {\"manual\": {\"pending\": false, \"features\": {}, \"created_at\": \"2026-01-01T00:00:00+00:00\", \"description\": \"Admin account\"}}}}}'::jsonb
)
ON CONFLICT (entity_key) DO UPDATE SET labels = EXCLUDED.labels;
" 2>/dev/null
echo "  Marked admin as verified"

echo ""
echo "=== Publishing test events ==="

# Helper to generate a realistic sha256 hash
sha256hash() {
    echo -n "$1" | shasum -a 256 | cut -d' ' -f1
}

VIDEO_HASH_1=$(sha256hash "test-video-nudity-001")
VIDEO_HASH_2=$(sha256hash "test-video-csam-001")
VIDEO_HASH_3=$(sha256hash "test-video-clean-001")
VIDEO_HASH_4=$(sha256hash "test-video-ai-001")

# --- 1. Verified user publishes video (should NOT trigger NewAccountSpam) ---
echo ""
echo "--- Test 1: Verified user publishes video (no rules should fire) ---"
$NAK event --sec "$ADMIN_NSEC" -k 34235 \
    -d "verified-video-001" \
    -t "x=$VIDEO_HASH_3" \
    -t "url=https://media.divine.video/clean-001.mp4" \
    -t m=video/mp4 \
    -t "title=My First Video" \
    -t "thumb=https://media.divine.video/clean-001-thumb.jpg" \
    -t "image=https://media.divine.video/clean-001-thumb.jpg" \
    "$RELAY" 2>&1 | grep -o "success\|failed.*"

VERIFIED_VIDEO_ID=$($NAK event --sec "$ADMIN_NSEC" -k 34235 \
    -d "verified-video-001" \
    -t "x=$VIDEO_HASH_3" \
    -t "url=https://media.divine.video/clean-001.mp4" \
    -t m=video/mp4 \
    -t "title=My First Video" \
    -t "thumb=https://media.divine.video/clean-001-thumb.jpg" \
    -t "image=https://media.divine.video/clean-001-thumb.jpg" \
    2>/dev/null | jq -r '.id')
echo "  Video event: ${VERIFIED_VIDEO_ID:0:16}..."

# --- 2. Spammer publishes videos (should trigger NewAccountSpam) ---
echo ""
echo "--- Test 2: New unverified account publishes video (NewAccountSpam) ---"
$NAK event --sec "$SPAMMER_NSEC" -k 34235 \
    -d "spam-video-001" \
    -t "x=$VIDEO_HASH_1" \
    -t "url=https://media.divine.video/spam-001.mp4" \
    -t m=video/mp4 \
    -t "title=Buy Crypto Now" \
    -t "thumb=https://media.divine.video/spam-001-thumb.jpg" \
    -t "image=https://media.divine.video/spam-001-thumb.jpg" \
    "$RELAY" 2>&1 | grep -o "success\|failed.*"

SPAM_VIDEO_ID=$($NAK event --sec "$SPAMMER_NSEC" -k 34235 \
    -d "spam-video-001" \
    -t "x=$VIDEO_HASH_1" \
    -t "url=https://media.divine.video/spam-001.mp4" \
    -t m=video/mp4 \
    -t "title=Buy Crypto Now" \
    -t "thumb=https://media.divine.video/spam-001-thumb.jpg" \
    -t "image=https://media.divine.video/spam-001-thumb.jpg" \
    2>/dev/null | jq -r '.id')
echo "  Spam video event: ${SPAM_VIDEO_ID:0:16}..."

# --- 3. Trusted reporter files CSAM report (should trigger auto_hide + ban) ---
# Uses divine-mobile tag format: reason in 3rd element of e/p tags
echo ""
echo "--- Test 3: Trusted reporter CSAM report (TrustedReporterCSAM -> auto_hide) ---"
$NAK event --sec "$REPORTER_NSEC" -k 1984 \
    -c 'CONTENT REPORT - NIP-56
Reason: csam
Reported via Divine for community safety' \
    -t "e=$SPAM_VIDEO_ID;illegal" \
    -t "p=$SPAMMER_PUB;illegal" \
    -t "client=diVine" \
    "$RELAY" 2>&1 | grep -o "success\|failed.*"

# --- 4. Trusted reporter files NSFW report (should trigger flag_for_review) ---
# Uses divine-mobile tag format
echo ""
echo "--- Test 4: Trusted reporter NSFW report (TrustedReporterNSFW -> flag_for_review) ---"
NSFW_VIDEO_ID=$($NAK event --sec "$SPAMMER_NSEC" -k 34235 \
    -d "nsfw-video-001" \
    -t "x=$VIDEO_HASH_1" \
    -t "url=https://media.divine.video/nsfw-001.mp4" \
    -t m=video/mp4 \
    -t "title=Spicy Content" \
    -t "thumb=https://media.divine.video/nsfw-001-thumb.jpg" \
    -t "image=https://media.divine.video/nsfw-001-thumb.jpg" \
    2>/dev/null | jq -r '.id')

$NAK event --sec "$REPORTER_NSEC" -k 1984 \
    -c 'CONTENT REPORT - NIP-56
Reason: nudity
Reported via Divine for community safety' \
    -t "e=$NSFW_VIDEO_ID;nudity" \
    -t "p=$SPAMMER_PUB;nudity" \
    -t "client=diVine" \
    "$RELAY" 2>&1 | grep -o "success\|failed.*"

# --- 5. Moderator confirms nudity via kind 1985 label ---
echo ""
echo "--- Test 5: Human moderator confirms nudity (ConfirmedNudity -> restrict) ---"
$NAK event --sec "$MODERATOR_NSEC" -k 1985 \
    -c "" \
    -t "L=content-warning" \
    -t "l=nudity;content-warning;{\"confidence\":0.95,\"source\":\"human-moderator\",\"verified\":true,\"sha256\":\"$VIDEO_HASH_1\"}" \
    -e "$NSFW_VIDEO_ID" \
    -t "x=$VIDEO_HASH_1" \
    "$RELAY" 2>&1 | grep -o "success\|failed.*"

# --- 6. Moderator confirms CSAM via kind 1985 label ---
echo ""
echo "--- Test 6: Human moderator confirms CSAM (ConfirmedCSAM -> ban) ---"
$NAK event --sec "$MODERATOR_NSEC" -k 1985 \
    -c "" \
    -t "L=content-warning" \
    -t "l=csam;content-warning;{\"confidence\":0.99,\"source\":\"human-moderator\",\"verified\":true,\"sha256\":\"$VIDEO_HASH_2\"}" \
    -e "$SPAM_VIDEO_ID" \
    -t "x=$VIDEO_HASH_2" \
    "$RELAY" 2>&1 | grep -o "success\|failed.*"

# --- 7. Moderator confirms AI-generated content ---
echo ""
echo "--- Test 7: Human moderator confirms AI-generated (ConfirmedAIGenerated -> flag_for_review) ---"
AI_VIDEO_ID=$($NAK event --sec "$SPAMMER_NSEC" -k 34235 \
    -d "ai-video-001" \
    -t "x=$VIDEO_HASH_4" \
    -t "url=https://media.divine.video/ai-001.mp4" \
    -t m=video/mp4 \
    -t "title=Definitely Real" \
    -t "thumb=https://media.divine.video/ai-001-thumb.jpg" \
    -t "image=https://media.divine.video/ai-001-thumb.jpg" \
    2>/dev/null | jq -r '.id')

$NAK event --sec "$MODERATOR_NSEC" -k 1985 \
    -c "" \
    -t "L=content-warning" \
    -t "l=ai-generated;content-warning;{\"confidence\":0.88,\"source\":\"human-moderator\",\"verified\":true,\"sha256\":\"$VIDEO_HASH_4\"}" \
    -e "$AI_VIDEO_ID" \
    -t "x=$VIDEO_HASH_4" \
    "$RELAY" 2>&1 | grep -o "success\|failed.*"

# --- 8. Moderator rejects a false positive (RejectedLabel -> approve) ---
echo ""
echo "--- Test 8: Moderator rejects false positive (RejectedLabel -> approve) ---"
$NAK event --sec "$MODERATOR_NSEC" -k 1985 \
    -c "" \
    -t "L=content-warning" \
    -t "l=nudity;content-warning;{\"confidence\":0.62,\"source\":\"human-moderator\",\"rejected\":true,\"sha256\":\"$VIDEO_HASH_3\"}" \
    -e "$VERIFIED_VIDEO_ID" \
    -t "x=$VIDEO_HASH_3" \
    "$RELAY" 2>&1 | grep -o "success\|failed.*"

echo ""
echo "=== Seed complete ==="
echo ""
echo "Published events covering:"
echo "  - NewAccountSpam (unverified pubkey publishes video)"
echo "  - TrustedReporterCSAM (trusted reporter files CSAM report)"
echo "  - TrustedReporterNSFW (trusted reporter files NSFW report)"
echo "  - ConfirmedNudity (human moderator label, kind 1985)"
echo "  - ConfirmedCSAM (human moderator label, kind 1985)"
echo "  - ConfirmedAIGenerated (human moderator label, kind 1985)"
echo "  - RejectedLabel (false positive rejection, kind 1985)"
echo "  - Verified user baseline (no rules should fire)"
echo ""
echo "Check worker logs:  docker compose logs osprey-worker --tail 50"
echo "Check ClickHouse:   curl -s 'http://localhost:8123/?user=default&password=clickhouse' --data 'SELECT __verdicts, __rule_hits, Kind, EventId FROM osprey.osprey_events FORMAT Pretty'"
echo "Check Osprey UI:    http://localhost:5002"
