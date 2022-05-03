import json
from functools import partial  # NOQA
from typing import Callable


class ReduceTask:
    """
    From Google map reduce implementation:

    reduce(key: str, values: list[str]) -> list[str]:
        ...
        emit(some_value);
    """

    def __init__(self):
        self.call: Callable = lambda k, v: None
        self.results: dict[str, list[str]] = {}
        self.key = ""

    def execute(self, key: str, values: list[str]):
        self.key = key
        self.call(key, values)

    def load_function(self, reduce_func: str):
        reduce_func = reduce_func.replace("emit", "self.emit")
        header = ["def reduce_function(self, key, values):"]
        contents = [
            " " * 4 + line.rstrip() for line in reduce_func.strip().splitlines()
        ]
        footer = ["self.call = partial(reduce_function, self)"]
        exec("\n".join(header + contents + footer))

    def emit(self, value: str):
        if self.key in self.results:
            self.results[self.key].append(value)
        else:
            self.results[self.key] = [value]

    def store_results(self, output_name: str):
        with open(output_name, "w") as output:
            json.dump(self.results, output)
