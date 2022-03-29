import attr
from communicator import DEFAULT_TIMEOUT, Communicator
from loguru import logger


@attr.s(auto_attribs=True)
class Worker(Communicator):
    self_host: str
    self_port: int
    target_host: str
    target_port: int
    timeout: int = DEFAULT_TIMEOUT

    def ping(self) -> bool:
        try:
            logger.info(f"Sending PING to {self.target_string}")
            self.communicate("ping", {})
        except ConnectionError:
            logger.error(f"Rejected PING to {self.target_string}")
            return False

        return True

    def terminate(self):
        try:
            logger.info(f"Sending TERMINATE to {self.target_string}")
            self.communicate("terminate", {})
        except ConnectionError:
            logger.error(f"Rejected TERMINATE to {self.target_string}")
            return False

        return True
