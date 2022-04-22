from functools import partial  # NOQA
from typing import Callable


class MapTask:
    def __init__(self, map_func: str):
        self.call: Callable = lambda k, v: None
        self.results: list[tuple[str, str]] = []

        map_func = map_func.replace("emit", "self.emit")
        header = ["def map_function(self, key, value):"]
        contents = [" " * 4 + line.rstrip() for line in map_func.strip().splitlines()]
        footer = ["self.call = partial(map_function, self)"]

        exec("\n".join(header + contents + footer))

    def emit(self, key: str, value: str):
        self.results.append((key, value))
