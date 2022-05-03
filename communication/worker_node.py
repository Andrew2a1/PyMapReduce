import json
import os
import threading
from typing import Any, Optional

from command_validator import CommandValidator
from event_handler import event_handler
from loguru import logger
from master import Master
from node import Node

from mapreduce.map_task import MapTask
from mapreduce.reduce_task import ReduceTask


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

    @event_handler
    def ping(self, command: dict[str, Any]):
        pass

    @event_handler
    def map(self, command: dict[str, Any]):
        if self.master is None:
            raise AttributeError("Master not set.")

        function_str: str = command["data"]["map_function"]
        input_file: str = command["data"]["filename"]
        output_filename = (
            f"{os.path.dirname(input_file)}/{command['name']}_{hash(self)}.txt"
        )

        task = MapTask()
        task.load_function(function_str)

        with open(input_file, "r") as file:
            task.execute(input_file, file.read())

        task.store_results(output_filename)

        logger.debug(f"Map results stored to file: {output_filename}")
        self.master.task_done(output_filename)

    @event_handler
    def reduce(self, command: dict[str, Any]):
        if self.master is None:
            raise AttributeError("Master not set.")

        function_str: str = command["data"]["reduce_function"]
        input_file: str = command["data"]["filename"]
        output_filename = (
            f"{os.path.dirname(input_file)}/{command['name']}_{hash(self)}.txt"
        )

        task = ReduceTask()
        task.load_function(function_str)

        with open(input_file, "r") as file:
            data = json.load(file)
            for key, values in data.items():
                task.execute(key, values)

        task.store_results(output_filename)

        logger.debug(f"Map results stored to file: {output_filename}")
        self.master.task_done(output_filename)


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

    try:
        while True:
            user_input = input(">> ")

            if user_input == "q":
                worker.exit_flag.set()
                break
    except KeyboardInterrupt:
        worker.exit_flag.set()

    worker_thread.join()
