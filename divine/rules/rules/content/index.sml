# Content moderation rules for Nostr events
#
# label_routing: Routes kind 1985 human-verified labels to verdicts
#                (primary path for moderation-service decisions)
# ai_classification: Routes Hive AI results to verdicts via API lookup
#                    (secondary path for video events without labels yet)
#
# Future:
# - Text content filtering (hate speech, harassment patterns)
# - Spam link detection
# - Content provenance / C2PA signal rules

Import(rules=['models/base.sml'])

Require(rule='rules/content/label_routing.sml')
Require(rule='rules/content/ai_classification.sml')
