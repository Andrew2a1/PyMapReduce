import itertools
import json
import math
import os
from queue import Empty, Queue
from threading import Thread
from typing import Callable

import attr
from loguru import logger

from communication.worker import Worker


@attr.s(auto_attribs=True)
class MapReduce:
    input_file: str
    map_function: str
    reduce_function: str
    chunk_size: int = 8 * 1024
    max_reduce_workers: int = 4

    def run(self, workers: list[Worker]):
        map_results = self.run_map(workers)
        logger.info(f"Map done")

        shuffle_results_filename = self.shuffle(map_results)
        logger.info(f"Shuffle done")

        self.run_reduce(shuffle_results_filename, workers)
        logger.info(f"Reduce done")

    def run_map(self, workers: list[Worker]) -> list[str]:
        chunk_files = self.__split_file(self.input_file, self.chunk_size)
        results: list[str] = self.__run_stage("map", workers, chunk_files)
        return results

    def run_reduce(
        self, shuffle_results_filename: str, workers: list[Worker]
    ) -> list[str]:
        chunk_files = self.__split_shuffle(
            shuffle_results_filename, min(len(workers), self.max_reduce_workers)
        )
        results: list[str] = self.__run_stage("reduce", workers, chunk_files)
        return results

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

    def __run_stage(
        self, stage: str, workers: list[Worker], chunk_files: list[str]
    ) -> list[str]:
        if stage not in ["map", "reduce"]:
            raise RuntimeError("Invalid stage: " + stage)

        tasks: Queue[str] = Queue()
        results_files: Queue[str] = Queue()

        for file in chunk_files:
            tasks.put_nowait(file)

        worker_watchers: list[Thread] = []
        for worker in workers:
            stage_function: str = getattr(self, f"{stage}_function")
            worker_function = lambda worker, task: getattr(worker, stage)(
                stage_function, task
            )
            watcher = Thread(
                target=self.__process_tasks,
                args=[worker, worker_function, tasks, results_files],
            )
            worker_watchers.append(watcher)
            watcher.start()

        for watcher in worker_watchers:
            watcher.join()

        results: list[str] = []
        while not results_files.empty():
            results.append(results_files.get_nowait())

        return results

    def __process_tasks(
        self,
        worker: Worker,
        worker_function: Callable,
        tasks: Queue[str],
        results: Queue[str],
    ):
        fails = 0
        while not tasks.empty() and fails < 3:
            worker.task_done.clear()
            try:
                task = tasks.get(timeout=2)
            except Empty:
                return

            if worker_function(worker, task) is False:
                tasks.put(task)
                fails += 1
                continue

            is_done = worker.task_done.wait(worker.timeout)
            if is_done is False:
                tasks.put(task)
                fails += 1
                logger.error(f"Timeout waiting for worker: {worker}")
                continue

            result = worker.get_last_result()
            logger.debug(f"Result: {result}")
            results.put(result["output"])

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
