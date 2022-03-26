import json
import socket
import socketserver
import threading
from queue import Queue

from command_validator import CommandValidator
from connection_error import ConnectionError
from loguru import logger

command_semaphore = threading.Semaphore(0)
exit_flag = threading.Event()
commands = Queue()


class WorkerServer(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.data = self.request.recv(1024).strip()
        self.store_command(data)
        self.request.sendall(b"ACCEPT")

    def store_command(self, rawdata):
        try:
            command = json.loads(rawdata)
            commands.put(command)
            command_semaphore.release()
            logger.info(f"Received command: {command}")
        except ValueError:
            logger.error(f"Received invalid data: {rawdata}")


class Worker:
    def __init__(self, host, port=0) -> None:
        self.host = host
        self.port = port

        self.server_thread = None
        self.server = None

        self.server_created = threading.Event()

        self.master_host = None
        self.master_port = None

    def set_master(self, host, port):
        self.master_host = host
        self.master_port = port

    def send_to_master(self, command_name, command_data=dict()):
        with socket.create_connection((self.master_host, self.master_port), 5) as connection:
            server_host, server_port = self.server.socket.getsockname()
            conn_data = {
                "host": server_host,
                "port": server_port,
                "name": command_name,
                "data": command_data,
            }
            connection.sendall(json.dumps(conn_data).encode())
            response = connection.recv(512)

        if response != b"ACCEPT":
            raise ConnectionError(
                f"Cannot connect to master at: host={self.master_host} port={self.master_port}"
            )

        return True

    def connect_master(self):
        self.send_to_master("connect")
        logger.info(f"Connection to master at: host={self.master_host} port={self.master_port} accepted")

    def disconnect_master(self):
        try:
            self.send_to_master("disconnect")
        except ConnectionError:
            logger.error(f"Cannot disconnect from master at: host={self.master_host} port={self.master_port}")
        else:
            logger.info(f"Succesfully disconnected from master at: host={self.master_host} port={self.master_port}")

    def main_loop(self):
        while not exit_flag.is_set():
            command_semaphore.acquire(timeout=2)
            if commands.empty():
                continue

            command = commands.get()
            is_valid = CommandValidator.validate_worker(command)

            if not is_valid:
                logger.error(f"Received invalid command: {command}")
                continue

            logger.info(f"Received command: {command}")

    def run_server(self):
        logger.info(f"Starting Worker server at: {self.host} {self.port}")
        self.server_thread = threading.Thread(target=self.__run_server_thread)
        self.server_thread.start()

    def wait_server_initialized(self):
        self.server_created.wait()

    def __run_server_thread(self):
        logger.info("Running worker server...")
        with socketserver.TCPServer((self.host, self.port), WorkerServer) as server:
            self.server = server
            self.server_created.set()
            server.serve_forever()

        logger.info("Server has been shut down")

    def shutdown_server(self):
        self.server.shutdown()

    def wait_finished(self):
        self.server_thread.join()


def worker_thread():
    worker = Worker("localhost")
    worker.set_master(host="localhost", port=9999)

    worker.run_server()
    worker.wait_server_initialized()
    worker.connect_master()

    worker.main_loop()

    worker.disconnect_master()
    worker.shutdown_server()
    worker.wait_finished()


if __name__ == "__main__":
    worker_thread = threading.Thread(target=worker_thread)
    worker_thread.start()

    while True:
        user_input = input(">> ")

        if user_input == "q":
            exit_flag.set()
            break

    worker_thread.join()
