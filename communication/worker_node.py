import threading
from typing import Any, Optional

from command_validator import CommandValidator
from master import Master
from node import Node


class WorkerNode(Node):
    def __init__(self, host: str, port: int) -> None:
        super().__init__(host, port)
        self.master: Optional[Master] = None

    def validate_command(self, command: dict[str, Any]) -> bool:
        return CommandValidator.validate_worker(command)

    def set_master(self, host: str, port: int):
        if not self.server_created.is_set() or self.server is None:
            raise Exception("Cannot set master when self server was not created")

        server_host, server_port = self.server.socket.getsockname()
        self.master = Master(server_host, server_port, host, port)

    def connect_master(self):
        self.master.connect()

    def disconnect_master(self):
        self.master.disconnect()


def run_worker_thread(worker: WorkerNode):
    worker.run_server()
    worker.wait_server_initialized()

    worker.set_master(host="localhost", port=9999)
    worker.connect_master()

    worker.main_loop()

    worker.disconnect_master()
    worker.shutdown_server()
    worker.wait_finished()


if __name__ == "__main__":
    worker = WorkerNode("localhost", 0)
    worker_thread = threading.Thread(target=run_worker_thread, args=[worker])
    worker_thread.start()

    while True:
        user_input = input(">> ")

        if user_input == "q":
            worker.exit_flag.set()
            break

    worker_thread.join()
