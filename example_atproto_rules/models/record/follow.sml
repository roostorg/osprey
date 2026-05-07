Import(rules=['models/base.sml'])

FollowSubject: str = JsonData(
  path='$.commit.record.subject',
  required=False,
)
