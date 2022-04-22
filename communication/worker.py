import threading

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

    def ping(self) -> bool:
        return self.communicate("ping")

    def terminate(self):
        return self.communicate("terminate")

    def map(self, map_function: str, filename: str):
        return self.communicate("map", {"map_function": map_function, "filename": filename})
