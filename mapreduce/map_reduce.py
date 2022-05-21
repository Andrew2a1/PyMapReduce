import itertools
import json
import math
import os

import attr
from loguru import logger

from communication.worker import Worker


@attr.s(auto_attribs=True)
class MapReduce:
    input_file: str
    map_function: str
    reduce_function: str
    chunk_size: int = 8 * 1024

    def run(self, workers: list[Worker]):
        map_results = self.run_map(workers)
        shuffle_results_filename = self.shuffle(map_results)
        self.run_reduce(shuffle_results_filename, workers)

    def run_map(self, workers: list[Worker]) -> list[str]:
        chunk_files = self.__split_file(self.input_file, self.chunk_size)

        results_files = []
        sent_chunks = 0

        while sent_chunks < len(chunk_files):
            working_nodes: list[Worker] = []

            for worker in workers:
                worker.task_done.clear()
                if worker.map(self.map_function, chunk_files[sent_chunks]) is True:
                    working_nodes.append(worker)
                    sent_chunks += 1

            for worker in working_nodes:
                is_done = worker.task_done.wait(10)

                if is_done is False:
                    logger.error(f"Timeout waiting for worker: {worker}")
                    workers.remove(worker)
                    continue

                result = worker.get_last_result()
                logger.debug(f"Result: {result}")
                results_files.append(result["output"])

        logger.info(f"Map done")
        return results_files

    def __split_file(self, filename: str, chunk_size: int) -> list[str]:
        chunk_files: list[str] = []
        file_size = os.path.getsize(filename)
        split_count = math.ceil(file_size / chunk_size)

        with open(filename, "r") as file:
            for i in range(split_count):
                data = file.read(chunk_size)
                chunk_filename = f"{os.path.dirname(filename)}/chunk_{i}_{os.path.basename(filename)}"
                chunk_files.append(chunk_filename)
                with open(chunk_filename, "w") as chunk_file:
                    chunk_file.write(data)

        return chunk_files

    def shuffle(self, map_results_files: list[str]) -> str:
        shuffle_results: dict[str, list[str]] = {}
        for file in map_results_files:
            with open(file, "r") as input:
                data = json.load(input)

            for k, v in data:
                if k in shuffle_results:
                    shuffle_results[k].append(v)
                else:
                    shuffle_results[k] = [v]

        shuffle_results_filename = (
            f"{os.path.dirname(map_results_files[0])}/shuffle_results.txt"
        )
        with open(shuffle_results_filename, "w") as shuffle_results_file:
            json.dump(shuffle_results, shuffle_results_file)

        return shuffle_results_filename

    def run_reduce(
        self, shuffle_results_filename: str, workers: list[Worker]
    ) -> list[str]:
        working_nodes: list[Worker] = []
        chunk_files = iter(self.__split_shuffle(shuffle_results_filename, len(workers)))

        for worker in workers:
            worker.task_done.clear()

        for worker in workers:
            if worker.reduce(self.reduce_function, next(chunk_files)) is True:
                working_nodes.append(worker)
            else:
                worker.task_done.set()

        results_files = []
        for worker in working_nodes:
            is_done = worker.task_done.wait(10)

            if is_done is False:
                logger.error(f"Timeout waiting for worker: {worker}")
                workers.remove(worker)
                continue

            result = worker.get_last_result()
            logger.debug(f"Result: {result}")
            results_files.append(result["output"])

        logger.info(f"Reduce done")
        return results_files

    def __split_shuffle(self, filename: str, split_count: int) -> list[str]:
        chunk_files: list[str] = []

        with open(filename, "r") as file:
            contents = json.load(file)

        items = len(contents)
        chunk_size = items // split_count

        for i in range(split_count):
            chunk_filename = (
                f"{os.path.dirname(filename)}/chunk_{i}_{os.path.basename(filename)}"
            )
            data = itertools.islice(
                contents.items(), i * chunk_size, (i + 1) * chunk_size
            )
            with open(chunk_filename, "w") as chunk_file:
                json.dump(dict(data), chunk_file)
            chunk_files.append(chunk_filename)

        return chunk_files
