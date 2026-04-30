Import(rules=['models/base.sml'])

IsCreate = OperationKind == 'create'
IsUpdate = OperationKind == 'update'
IsDelete = OperationKind == 'delete'

Collection: str = JsonData(
  path='$.operation.collection',
  required=False,
  coerce_type=True,
)

Path: str = JsonData(
  path='$.operation.path',
  required=False,
  coerce_type=True,
)

Cid: str = JsonData(
  path='$.operation.cid',
  required=False,
  coerce_type=True,
)
