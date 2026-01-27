AccountCreatedAt: Entity[str] = EntityJson(
  type='Timestamp',
  path='$.account.created_at',
  coerce_type=True
)

PostCount: Entity[int] = EntityJson(
  type='Count',
  path='$.account.post_count',
  coerce_type=True
)

FollowerCount: Entity[int] = EntityJson(
  type='Count',
  path='$.account.follower_count',
  coerce_type=True
)
