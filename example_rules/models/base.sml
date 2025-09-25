UserId: Entity[str] = EntityJson(
  type='User',
  path='$.user_id',
  coerce_type=True
)

EventType: Entity[str] = EntityJson(
  type='EventType',
  path='$.event_type',
  coerce_type=True
)

ActionName=GetActionName()

