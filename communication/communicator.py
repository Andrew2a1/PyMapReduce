import json
import socket
from typing import Any

import attr

DEFAULT_TIMEOUT = 5


@attr.s(auto_attribs=True)
class Communicator:
    self_host: str
    self_port: int
    target_host: str
    target_port: int
    timeout: int = DEFAULT_TIMEOUT

    def __eq__(self, other) -> bool:
        if type(other) is Communicator:
            return (
                self.target_host == other.target_host
                and self.target_port == other.target_port
            )
        return super().__eq__(other)

    def __hash__(self) -> int:
        return hash(f"{self.target_host}:{self.target_port}")

    def __str__(self) -> str:
        return f"{self.target_host}:{self.target_port}"

    def communicate(
        self, command_name: str, command_data: dict[str, Any] = dict()
    ) -> None:
        with socket.create_connection(
            (self.target_host, self.target_port), self.timeout
        ) as connection:
            conn_data = {
                "host": self.self_host,
                "port": self.self_port,
                "name": command_name,
                "data": command_data,
            }
            connection.sendall(json.dumps(conn_data).encode())
            response = connection.recv(512)

        if response != b"ACCEPT":
            raise ConnectionError(
                f"Cannot connect to master at: host={self.target_host} port={self.target_port}"
            )

    @property
    def target_string(self) -> str:
        return f"host={self.target_host}, port={self.target_port}"

    @property
    def self_string(self) -> str:
        return f"host={self.self_host}, port={self.self_port}"
