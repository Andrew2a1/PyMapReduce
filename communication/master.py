import attr
from communicator import DEFAULT_TIMEOUT, Communicator
from loguru import logger


@attr.s(auto_attribs=True)
class Master(Communicator):
    self_host: str
    self_port: int
    target_host: str
    target_port: int
    timeout: int = DEFAULT_TIMEOUT

    def connect(self):
        return self.communicate("connect")

    def disconnect(self):
        return self.communicate("disconnect")

    def task_done(self, name: str):
        return self.communicate("task_done", {"name": name})
