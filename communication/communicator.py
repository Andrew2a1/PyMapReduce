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

    def communicate(self, command_name: str, command_data: dict[str, Any]) -> None:
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
        return f"host={self.target_host}, port={self.self_port}"

    @property
    def self_string(self) -> str:
        return f"host={self.self_host}, port={self.self_port}"
