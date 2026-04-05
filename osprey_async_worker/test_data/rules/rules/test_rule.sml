UserId: Entity[str] = EntityJson(
  type='User',
  path='$.user_id',
  coerce_type=True
)

EventType: str = JsonData(path='$.event_type', coerce_type=True)
ActionName = GetActionName()
MessageLength = StringLength(value=JsonData(path='$.message', coerce_type=True))

IsLongMessage = Rule(
  when_all=[
    MessageLength > 100,
  ],
  description='Message is longer than 100 characters',
)
