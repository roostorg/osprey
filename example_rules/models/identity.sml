PreviousHandle: Entity[str] = EntityJson(
  type='Handle',
  path='$.identity.previous_handle',
  coerce_type=True
)

NewHandle: Entity[str] = EntityJson(
  type='Handle',
  path='$.identity.new_handle',
  coerce_type=True
)

DisplayName: Entity[str] = EntityJson(
  type='DisplayName',
  path='$.identity.display_name',
  coerce_type=True
)

ChangeType: Entity[str] = EntityJson(
  type='ChangeType',
  path='$.identity.change_type',
  coerce_type=True
)
