ActionName = GetActionName()

UserId: Entity[str] = EntityJson(
  type='UserId',
  path='$.did',
  required=False,
)

# The DID as a plain string, for enrichment UDFs that resolve it to a profile.
Did: str = JsonData(
  path='$.did',
  required=False,
)

# JetStream carries only the DID, so resolve the handle and display name via the
# Bluesky public API (cached per DID). Absent when the account can't be resolved
# (e.g. the API rate-limits at full firehose volume).
Handle: str = AtprotoHandle(did=Did)

DisplayName: str = AtprotoDisplayName(did=Did)

OperationKind: Optional[str] = JsonData(
  path='$.commit.operation',
  required=False,
)

IsOperation = OperationKind != None

Second: int = 1
Minute: int = Second * 60
Hour: int = Minute * 60
Day: int = Hour * 24
Week: int = Day * 7
