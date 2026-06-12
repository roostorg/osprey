# Flag actions whose message text is longer than 100 characters.
#
# Demonstrates stdlib-only UDFs the async Phase-0 engine supports:
# EntityJson, JsonData, GetActionName, StringLength.
UserId: Entity[str] = EntityJson(type='User', path='$.user_id', coerce_type=True)
EventType: str = JsonData(path='$.event_type', coerce_type=True)
ActionName = GetActionName()

MessageText: str = JsonData(path='$.message', coerce_type=True)
MessageLength = StringLength(s=MessageText)

LongMessage = Rule(
  when_all=[
    MessageLength > 100,
  ],
  description='Message text is longer than 100 characters',
)
