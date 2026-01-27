IpAddress: Entity[str] = EntityJson(
  type='IpAddress',
  path='$.ip_address',
  coerce_type=True
)

UserAgent: Entity[str] = EntityJson(
  type='UserAgent',
  path='$.user_agent',
  coerce_type=True
)

IpNetworkValue: Entity[str] = EntityJson(
  type='IpNetwork',
  path='$.ip_address',
  coerce_type=True
)
