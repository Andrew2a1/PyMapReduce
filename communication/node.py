import json
import socketserver
import threading
from queue import Queue
from typing import Any, Callable, Optional

from loguru import logger


class Node:
    handlers: dict[str, Callable] = {}

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

        self.server_thread: Optional[threading.Thread] = None
        self.server: Optional[socketserver.TCPServer] = None

        self.server_created = threading.Event()
        self.command_semaphore = threading.Semaphore(0)
        self.exit_flag = threading.Event()

        self.commands: Queue[dict] = Queue()

    def main_loop(self):
        while not self.exit_flag.is_set():
            self.command_semaphore.acquire(timeout=2)
            if self.commands.empty():
                continue

            command = self.commands.get()
            is_valid = self.validate_command(command)

            if not is_valid:
                logger.error(f"Invalid command: {command}")
                continue

            self.__run_command(command)

    def validate_command(self, command: dict[str, Any]) -> bool:
        raise NotImplementedError()

    def __run_command(self, command: dict[str, Any]):
        cmd_name: str = str(command["name"])

        try:
            Node.handlers[cmd_name](self, command)
        except KeyError:
            logger.error(f"Command '{cmd_name}' not implemented")

    def run_server(self):
        logger.info(f"Starting server at: host={self.host}, port={self.port}")
        self.server_thread = threading.Thread(target=self.__run_server_thread)
        self.server_thread.start()

    def __run_server_thread(self):
        class NodeServer(socketserver.BaseRequestHandler):
            def handle(s):
                s.data = s.request.recv(1024).strip()
                s.store_command(s.data)
                s.request.sendall(b"ACCEPT")

            def store_command(s, rawdata: str):
                try:
                    command = json.loads(rawdata)
                    self.commands.put(command)
                    self.command_semaphore.release()
                    logger.debug(f"Received data: {command}")
                except ValueError:
                    logger.error(f"Received invalid data: {rawdata}")

        with socketserver.TCPServer((self.host, self.port), NodeServer) as server:
            logger.info("Server started")
            self.server = server
            self.server_created.set()
            server.serve_forever()

        logger.info("Server has been shut down")

    def shutdown_server(self):
        self.server.shutdown()

    def wait_finished(self):
        self.server_thread.join()

    def wait_server_initialized(self):
        self.server_created.wait()
