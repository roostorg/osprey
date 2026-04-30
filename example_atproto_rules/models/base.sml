Did: Entity[str] = EntityJson(
  type='Did',
  path='$.did',
  coerce_type=True
)

Collection: Entity[str] = EntityJson(
  type='Collection',
  path='$.collection',
  coerce_type=True
)

EventType: Entity[str] = EntityJson(
  type='EventType',
  path='$.event_type',
  coerce_type=True
)

PostText: Entity[str] = EntityJson(
  type='PostText',
  path='$.record.text',
  coerce_type=True
)

ActionName=GetActionName()
