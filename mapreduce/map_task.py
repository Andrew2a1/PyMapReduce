import json
from functools import partial  # NOQA
from typing import Callable


class MapTask:
    """
    From Google map reduce implementation:

    map(key: str, value: str) -> list[tuple[str, str]]:
        // key: document name
        // value: document contents
        ...
        emit(some_key, some_value);
    """

    def __init__(self):
        self.call: Callable = lambda k, v: None
        self.results: list[tuple[str, str]] = []

    def execute(self, filename: str, contents: str):
        self.call(filename, contents)

    def load_function(self, map_func: str):
        map_func = map_func.replace("emit", "self.emit")
        header = ["def map_function(self, key, value):"]
        contents = [" " * 4 + line.rstrip() for line in map_func.strip().splitlines()]
        footer = ["self.call = partial(map_function, self)"]
        exec("\n".join(header + contents + footer))

    def emit(self, key: str, value: str):
        self.results.append((key, value))

    def store_results(self, output_name: str):
        with open(output_name, "w") as output:
            json.dump(self.results, output)
