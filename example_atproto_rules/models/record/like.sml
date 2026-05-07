Import(rules=['models/base.sml'])

LikeSubjectUri: str = JsonData(
  path='$.commit.record.subject.uri',
  required=False,
)
