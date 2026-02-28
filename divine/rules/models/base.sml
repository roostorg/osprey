EventId: Entity[str] = EntityJson(
  type='EventId',
  path='$.event_id',
  coerce_type=True
)

Pubkey: Entity[str] = EntityJson(
  type='Pubkey',
  path='$.pubkey',
  coerce_type=True
)

Kind: int = JsonData(
  path='$.kind',
  coerce_type=True
)

CreatedAt: int = JsonData(
  path='$.created_at',
  coerce_type=True
)

Content: str = JsonData(
  path='$.content',
  coerce_type=True,
  required=False
)

Tags: List[str] = JsonData(
  path='$.tags',
  coerce_type=True,
  required=False
)

ActionName=GetActionName()
