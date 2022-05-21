import threading
from typing import Any

from command_validator import CommandValidator
from event_handler import event_handler
from loguru import logger
from node import Node
from rich import print
from worker import Worker

from mapreduce.function_loader import FunctionLoader
from mapreduce.map_reduce import MapReduce


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
                worker.put_last_result(command["data"])
                worker.task_done.set()

    def map_reduce(self, input_file: str, map_fn_file: str, reduce_fn_file: str):
        with open(map_fn_file, "r") as map_file:
            map_function = FunctionLoader.from_file(map_file)

        with open(reduce_fn_file, "r") as reduce_file:
            reduce_function = FunctionLoader.from_file(reduce_file)

        mr = MapReduce(input_file, map_function, reduce_function)
        mr.run(self.workers)


def run_master_thread(master: MasterNode):
    master.run_server()
    master.main_loop()
    master.shutdown_server()
    master.wait_finished()


if __name__ == "__main__":
    master = MasterNode(host="localhost", port=9999)
    master_thread = threading.Thread(target=run_master_thread, args=[master])
    master_thread.start()

    try:
        while True:
            user_input = input(">> ")

            if user_input == "q":
                master.exit_flag.set()
                break

            elif user_input == "w":
                print(master.workers)

            elif user_input == "map_reduce":
                map_function_file = input("map function file: ")
                reduce_function_file = input("reduce function file: ")
                file = input("data file: ")
                master.map_reduce(file, map_function_file, reduce_function_file)

            elif user_input == "mr":
                map_function_file = "./files/map.py"
                reduce_function_file = "./files/reduce.py"
                file = "./files/words.txt"
                master.map_reduce(file, map_function_file, reduce_function_file)

    except KeyboardInterrupt:
        master.exit_flag.set()

    master_thread.join()
