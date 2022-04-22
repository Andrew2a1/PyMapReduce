import threading
from typing import Any, TextIO

import loguru

from command_validator import CommandValidator
from event_handler import event_handler
from loguru import logger
from node import Node
from rich import print
from worker import Worker


class MasterNode(Node):
    def __init__(self, host: str, port: int) -> None:
        super().__init__(host, port)
        self.workers: list[Worker] = []

    def validate_command(self, command: dict[str, Any]) -> bool:
        return CommandValidator.validate_master(command)

    @event_handler
    def connect(self, command: dict[str, Any]):
        worker = Worker(self.host, self.port, command["host"], command["port"])
        self.workers.append(worker)
        logger.info(f"Connected with: {worker.target_string}")

    @event_handler
    def disconnect(self, command: dict[str, Any]):
        host, port = command["host"], command["port"]
        for worker in self.workers:
            if worker.target_host == host and worker.target_port == port:
                self.workers.remove(worker)
                logger.info(f"Removed: {worker.target_string}")
                break

    @event_handler
    def task_done(self, command: dict[str, Any]):
        host, port = command["host"], command["port"]
        for worker in self.workers:
            if worker.target_host == host and worker.target_port == port:
                worker.task_done.set()

    def map_reduce(self, map_func: str, input_file: str):
        working_nodes: list[Worker] = []

        for worker in self.workers:
            worker.task_done.clear()
            if worker.map(map_func, input_file) is True:
                working_nodes.append(worker)
            else:
                worker.task_done.set()

        for worker in working_nodes:
            worker.task_done.wait(10)

        logger.debug(f"Map done.")


def run_master_thread(master: MasterNode):
    master.run_server()
    master.main_loop()
    master.shutdown_server()
    master.wait_finished()


if __name__ == "__main__":
    master = MasterNode(host="localhost", port=9999)
    master_thread = threading.Thread(target=run_master_thread, args=[master])
    master_thread.start()

    while True:
        user_input = input(">> ")

        if user_input == "q":
            master.exit_flag.set()
            break

        if user_input == "w":
            print(master.workers)

        if user_input == "map":
            function = input("function: ")
            file = input("file: ")
            master.map_reduce(function, file)

    master_thread.join()
