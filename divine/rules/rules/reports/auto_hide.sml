# Trusted Reporter Auto-Hide
# Automatically acts on reports from trusted reporters for CSAM and NSFW content.
#
# Report reasons are normalized by the bridge. Canonical values:
#   csam, nudity, spam, impersonation, illegal, harassment, other
#
# Mobile sends 'illegal' for CSAM (NIP-56 mapping), which the bridge
# can't distinguish from violence/copyright 'illegal'. We match both
# 'csam' (unambiguous) and 'illegal' (may over-match, but for trusted
# reporters the cost of a false auto-hide on 'illegal' is low).

Import(
  rules=[
    'models/base.sml',
    'models/nostr/kind1984_report.sml',
  ]
)

TrustedReporterCSAM = Rule(
  when_all=[
    Kind == 1984,
    HasLabel(entity=Pubkey, label='trusted_reporter'),
    ReportReason in ['csam', 'illegal'],
  ],
  description='Trusted reporter flagged CSAM or illegal content',
)

TrustedReporterNSFW = Rule(
  when_all=[
    Kind == 1984,
    HasLabel(entity=Pubkey, label='trusted_reporter'),
    ReportReason == 'nudity',
  ],
  description='Trusted reporter flagged NSFW content',
)

WhenRules(
  rules_any=[TrustedReporterCSAM],
  then=[
    BanNostrEvent(event_id=ReportedEventId, pubkey=ReportedPubkey, reason='CSAM reported by trusted reporter'),
    DeclareVerdict(verdict='auto_hide'),
  ],
)

WhenRules(
  rules_any=[TrustedReporterNSFW],
  then=[
    DeclareVerdict(verdict='flag_for_review'),
    LabelAdd(entity=ReportedPubkey, label='nsfw_flagged', expires_after=TimeDelta(days=7)),
  ],
)
