Import(rules=['models/base.sml'])

IsCreate = OperationKind == 'create'
IsUpdate = OperationKind == 'update'
IsDelete = OperationKind == 'delete'

Collection: str = JsonData(
  path='$.commit.collection',
  required=False,
)

Rkey: str = JsonData(
  path='$.commit.rkey',
  required=False,
)

Cid: str = JsonData(
  path='$.commit.cid',
  required=False,
)

# Some collections (like, repost) have `record.subject = {uri, cid}`; others (follow)
# have `record.subject = "did:plc:..."`. Prefer the URI when present, else fall back to
# the raw subject value (coerced to str — for follows that's the DID; for the dict shape
# the URI path resolves first so the coerced repr is never used).
SubjectUri: Optional[str] = JsonData(
  path='$.commit.record.subject.uri',
  required=False,
)

SubjectRaw: Optional[str] = JsonData(
  path='$.commit.record.subject',
  required=False,
  coerce_type=True,
)

Subject: str = ResolveOptional(optional_value=SubjectUri, default_value=SubjectRaw)
