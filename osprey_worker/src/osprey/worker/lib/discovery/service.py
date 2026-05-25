from __future__ import annotations

from typing import Any

import simplejson as json


class Service:
    def __init__(
        self,
        name: str,
        port: int,
        address: str = 'localhost',
        metadata: dict[str, Any] | None = None,
        ports: dict[str, int] | None = None,
        ip: str | None = None,
        id_override: str | None = None,
        draining: bool = False,
    ):
        # type (...) -> None
        self.name = name
        self.address = address
        self.port = port
        self.ports: dict[str, int] = ports or {}
        self.metadata = metadata or {}
        self.ip = ip
        self.id_override = id_override
        self.draining = draining

    def __str__(self) -> str:
        return f'{self.name}@{self.id}'

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self}>'

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other) -> bool:
        return (self.name, self.address, self.port, self.ports, self.ip, self.id_override) == (
            other.name,
            other.address,
            other.port,
            other.ports,
            other.ip,
            other.id_override,
        )

    @property
    def id(self) -> str:
        if self.id_override:
            return f'{self.id_override}:{self.port}'
        return f'{self.address}:{self.port}'

    @property
    def connection_key(self) -> tuple[str, str | None]:
        """A key for distinguishing this service based on the host it's connecting to. It
        contains both hostname and IP in order to prevent confusion if another host picked
        up the same IP as an old host.
        """
        return (self.address, self.ip)

    @property
    def connection_address(self) -> str:
        return self.ip or self.address

    @classmethod
    def deserialize(cls, value: str) -> Service:
        service = json.loads(value)
        return Service(
            name=service['name'],
            address=service['address'],
            port=service['port'],
            ports=service.get('ports') or {},
            metadata=service.get('metadata') or {},
            ip=service.get('ip'),
            id_override=service.get('id_override'),
            draining=service.get('draining', False),
        )

    def serialize(self) -> str:
        return json.dumps(
            {
                'name': self.name,
                'address': self.address,
                'port': self.port,
                'metadata': self.metadata,
                'ports': self.ports,
                'ip': self.ip,
                'id_override': self.id_override,
                'draining': self.draining,
            }
        )

    def merge(self, other: Service) -> None:
        self.name = other.name
        self.address = other.address
        self.port = other.port
        self.metadata = other.metadata
        self.ports = other.ports
        self.ip = other.ip
        self.id_override = other.id_override
        self.draining = other.draining

    @property
    def grpc_port(self) -> int:
        """Returns the port that should be used for GRPC connections to this service."""
        # GRPC port is either defined or implicitly at service.port + 1.
        return self.ports.get('grpc', self.port + 1)
