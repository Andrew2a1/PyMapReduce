import threading
from typing import Any

import attr
from communicator import DEFAULT_TIMEOUT, Communicator


@attr.s(auto_attribs=True)
class Worker(Communicator):
    self_host: str
    self_port: int
    target_host: str
    target_port: int
    timeout: int = DEFAULT_TIMEOUT
    task_done: threading.Event = threading.Event()
    last_result: dict[str, Any] = {}
    last_result_lock: threading.Lock = threading.Lock()

    def set_last_result(self, data: dict[str, Any]):
        self.last_result_lock.acquire()
        self.last_result = data
        self.last_result_lock.release()

    def get_last_result(self) -> dict[str, Any]:
        self.last_result_lock.acquire()
        result = self.last_result
        self.last_result_lock.release()
        return result

    def ping(self) -> bool:
        return self.communicate("ping")

    def terminate(self):
        return self.communicate("terminate")

    def map(self, map_function: str, filename: str):
        return self.communicate(
            "map", {"map_function": map_function, "filename": filename}
        )
