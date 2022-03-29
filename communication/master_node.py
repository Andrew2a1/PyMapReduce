import threading
from typing import Any, Optional

from command_validator import CommandValidator
from node import Node
from worker import Worker

exit_flag: Optional[threading.Event] = None


class MasterNode(Node):
    def __init__(self, host: str, port: int) -> None:
        super().__init__(host, port)
        self.workers: list[Worker] = []

    def validate_command(self, command: dict[str, Any]) -> bool:
        return CommandValidator.validate_master(command)


def master_thread():
    global exit_flag

    master = MasterNode(host="localhost", port=9999)
    exit_flag = master.exit_flag

    master.run_server()
    master.main_loop()

    master.shutdown_server()
    master.wait_finished()


if __name__ == "__main__":
    master = threading.Thread(target=master_thread)
    master.start()

    while True:
        user_input = input(">> ")

        if user_input == "q":
            exit_flag.set()  # type: ignore
            break

    master.join()
