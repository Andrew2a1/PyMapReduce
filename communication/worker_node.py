import threading
from typing import Any, Optional

from command_validator import CommandValidator
from master import Master
from node import Node

exit_flag: Optional[threading.Event] = None


class WorkerNode(Node):
    def __init__(self, host: str, port: int) -> None:
        super().__init__(host, port)
        self.master: Optional[Master] = None

    def validate_command(self, command: dict[str, Any]) -> bool:
        return CommandValidator.validate_worker(command)

    def set_master(self, host: str, port: int):
        if not self.server_created.is_set() or self.server is None:
            raise Exception("Cannot set master while self server was not created")

        server_host, server_port = self.server.socket.getsockname()
        self.master = Master(server_host, server_port, host, port)

    def connect_master(self):
        self.master.connect()

    def disconnect_master(self):
        self.master.disconnect()


def worker_thread():
    global exit_flag

    worker = WorkerNode("localhost", 0)
    exit_flag = worker.exit_flag

    worker.run_server()
    worker.wait_server_initialized()

    worker.set_master(host="localhost", port=9999)
    worker.connect_master()

    worker.main_loop()

    worker.disconnect_master()
    worker.shutdown_server()
    worker.wait_finished()


if __name__ == "__main__":
    worker = threading.Thread(target=worker_thread)
    worker.start()

    while True:
        user_input = input(">> ")

        if user_input == "q":
            exit_flag.set()  # type: ignore
            break

    worker.join()
