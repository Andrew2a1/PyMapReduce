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
        try:
            logger.info(f"Sending CONNECT to {self.target_string}")
            self.communicate("connect")
        except (ConnectionError, ConnectionRefusedError):
            logger.error(f"Rejected CONNECT to {self.target_string}")
            return False

        return True

    def disconnect(self):
        try:
            logger.info(f"Sending DISCONNECT to {self.target_string}")
            self.communicate("disconnect")
        except (ConnectionError, ConnectionRefusedError):
            logger.error(f"Rejected DISCONNECT to {self.target_string}")
            return False

        return True
