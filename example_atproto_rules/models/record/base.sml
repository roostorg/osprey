Import(rules=['models/base.sml'])

IsCreate = OperationKind == 'create'
IsUpdate = OperationKind == 'update'
IsDelete = OperationKind == 'delete'

Collection: str = JsonData(
  path='$.commit.collection',
  required=False,
  coerce_type=True,
)

Rkey: str = JsonData(
  path='$.commit.rkey',
  required=False,
  coerce_type=True,
)

Cid: str = JsonData(
  path='$.commit.cid',
  required=False,
  coerce_type=True,
)
