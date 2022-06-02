import threading
from queue import Queue
from typing import Any, Callable, Optional

from communicator import DEFAULT_TIMEOUT, Communicator


class Worker(Communicator):
    def __init__(
        self, self_host: str, self_port: int, target_host: str, target_port: int
    ):
        self.self_host = self_host
        self.self_port = self_port
        self.target_host = target_host
        self.target_port = target_port

        self.timeout: int = DEFAULT_TIMEOUT
        self.task_done: threading.Event = threading.Event()
        self.task_done_callback: Optional[Callable] = None
        self.last_results: Queue[dict[str, Any]] = Queue()

    def set_task_done(self):
        self.task_done.set()

        if self.task_done_callback:
            self.task_done_callback()

    def put_last_result(self, data: dict[str, Any]):
        self.last_results.put(data)

    def get_last_result(self) -> dict[str, Any]:
        return self.last_results.get()

    def ping(self) -> bool:
        return self.communicate("ping")

    def terminate(self) -> bool:
        return self.communicate("terminate")

    def map(self, map_function: str, filename: str) -> bool:
        return self.communicate(
            "map", {"map_function": map_function, "filename": filename}
        )

    def reduce(self, reduce_function: str, filename: str):
        return self.communicate(
            "reduce", {"reduce_function": reduce_function, "filename": filename}
        )
