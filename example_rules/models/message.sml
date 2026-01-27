MessageText: Entity[str] = EntityJson(
  type='MessageText',
  path='$.message.text',
  coerce_type=True
)

RecipientId: Entity[str] = EntityJson(
  type='User',
  path='$.message.recipient_id',
  coerce_type=True
)
