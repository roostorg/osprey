Import(
  rules=[
    'models/base.sml',
  ],
)

IsCreate = OperationKind == 'create'
IsUpdate = OperationKind == 'update'
IsDelete = OperationKind == 'delete'

Collection: str = JsonData(
  path='$.operation.collection',
)

Path: str = JsonData(
  path='$.operation.path',
)

_UserIdResolved: str = ResolveOptional(optional_value=UserId)
AtUri: Entity[str] = Entity(
  type='AtUri',
  id=f'at://{_UserIdResolved}/{Path}',
)
