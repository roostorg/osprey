import dataclasses
from typing import Optional

from osprey.engine.utils.types import add_slots


@add_slots
# Can unfreeze if we ever need to modify instances of this.
@dataclasses.dataclass(frozen=True)
class AccessAuditLog:
    """
    Stores the access log in scylladb.
    """

    id: int  # snowflake
    requester_email: Optional[str]
    request_method: str
    request_path: str
    request_headers: str
    request_body: bytes
    response_status_code: int
    response_headers: str
    response_body: bytes

    def persist(self) -> None:
        pass
        # we should move this store to postgres, or some other plugin.
        # row = osprey_bigtable.table('audit_log').row(self.id.to_bytes(8, 'big'))
        # timestamp = datetime.datetime.utcnow()
        # for k in dataclasses.fields(self):
        #     v = getattr(self, k.name)
        #     row.set_cell('audit_log', k.name.encode(), v.encode() if hasattr(v, 'encode') else v, timestamp=timestamp)
        # osprey_bigtable.table('audit_log').mutate_rows([row])
