# NIP-32 Label Event (kind 1985)
#
# Published by moderation-service for human-verified content decisions.
# Uses 'content-warning' namespace with category labels (nudity, violence,
# ai-generated, etc.) and metadata JSON containing confidence scores.
#
# Tag structure:
#   ['L', 'content-warning']              -- namespace declaration
#   ['l', '<label>', 'content-warning', '<metadata_json>']  -- label + metadata
#   ['e', '<nostr_event_id>']             -- referenced event (if known)
#   ['x', '<sha256>']                     -- content hash
#   ['r', '<cdn_url>']                    -- reference URL
#
# Metadata JSON: { confidence, verified, source, sha256, [rejected] }
#
# Rejected labels use 'not-<label>' format (e.g., 'not-nudity').

Import(rules=['models/base.sml'])

LabelNamespace: str = JsonData(
  path='$.label_namespace',
  coerce_type=True,
  required=False
)

LabelValue: str = JsonData(
  path='$.label_value',
  coerce_type=True,
  required=False
)

LabelMetadata: str = JsonData(
  path='$.label_metadata',
  coerce_type=True,
  required=False
)

LabelTargetEvent: str = JsonData(
  path='$.label_target_event',
  coerce_type=True,
  required=False
)

LabelContentHash: str = JsonData(
  path='$.label_content_hash',
  coerce_type=True,
  required=False
)

LabelConfidence: float = JsonData(
  path='$.label_confidence',
  coerce_type=True,
  required=False
)

LabelSource: str = JsonData(
  path='$.label_source',
  coerce_type=True,
  required=False
)

LabelRejected: bool = JsonData(
  path='$.label_rejected',
  coerce_type=True,
  required=False
)
