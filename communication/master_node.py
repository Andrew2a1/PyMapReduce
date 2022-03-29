import json
import socketserver
import threading
from queue import Queue
from typing import Any, Optional

from command_validator import CommandValidator
from loguru import logger
from worker import Worker

command_semaphore = threading.Semaphore(0)
exit_flag = threading.Event()
commands: Queue[dict] = Queue()


class MasterServer(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.data = self.request.recv(1024).strip()
        self.store_command(data)
        self.request.sendall(b"ACCEPT")

    def store_command(self, rawdata: str):
        try:
            command = json.loads(rawdata)
            commands.put(command)
            command_semaphore.release()
            logger.info(f"Received data: {command}")
        except ValueError:
            logger.error(f"Received invalid data: {rawdata}")


class MasterNode:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

        self.server_thread: Optional[threading.Thread] = None
        self.server: Optional[socketserver.TCPServer] = None

        self.workers: list[Worker] = []

    def main_loop(self):
        while not exit_flag.is_set():
            command_semaphore.acquire(timeout=2)
            if commands.empty():
                continue

            command = commands.get()
            is_valid = CommandValidator.validate_master(command)

            if not is_valid:
                logger.error(f"Invalid command: {command}")
                continue

            self.__run_command(command)

    def __run_command(self, command: dict[str, Any]):
        cmd_name: str = command["name"]

        try:
            method_handle = getattr(self, f"{cmd_name}")
            method_handle(command)
        except AttributeError:
            logger.error(f"Command {cmd_name} not implemented")

    def run_server(self):
        logger.info(f"Starting master server at: {self.host} {self.port}")
        self.server_thread = threading.Thread(target=self.__run_server_thread)
        self.server_thread.start()

    def __run_server_thread(self):
        logger.info("Running master server...")
        with socketserver.TCPServer((self.host, self.port), MasterServer) as server:
            self.server = server
            server.serve_forever()

        logger.info("Server has been shut down")

    def shutdown_server(self):
        self.server.shutdown()

    def wait_finished(self):
        self.server_thread.join()


def master_thread():
    master = MasterNode(host="localhost", port=9999)
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
            exit_flag.set()
            break

    master.join()
