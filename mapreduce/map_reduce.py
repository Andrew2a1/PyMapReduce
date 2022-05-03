import json

import attr
from loguru import logger

from communication.worker import Worker


@attr.s(auto_attribs=True)
class MapReduce:
    input_file: str
    map_function: str
    reduce_function: str

    def run(self, workers: list[Worker]):
        results = self.run_map(workers)
        shuffled = self.shuffle(results)

        shuffle_results_filename = "files/shuffle_results.txt"
        with open(shuffle_results_filename, "w") as shuffle_results:
            json.dump(shuffled, shuffle_results)

        self.run_reduce(shuffle_results_filename, workers)

    def run_map(self, workers: list[Worker]) -> list[str]:
        working_nodes: list[Worker] = []

        for worker in workers:
            worker.task_done.clear()
            if worker.map(self.map_function, self.input_file) is True:
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

        logger.info(f"Map done")
        return results_files

    def shuffle(self, map_results_files: list[str]) -> dict[str, list[str]]:
        shuffle_results: dict[str, list[str]] = {}
        for file in map_results_files:
            with open(file, "r") as input:
                data = json.load(input)

            for k, v in data:
                if k in shuffle_results:
                    shuffle_results[k].append(v)
                else:
                    shuffle_results[k] = [v]

        return shuffle_results

    def run_reduce(
        self, shuffle_results_filename: str, workers: list[Worker]
    ) -> list[str]:
        working_nodes: list[Worker] = []

        for worker in workers:
            worker.task_done.clear()
            if worker.reduce(self.reduce_function, shuffle_results_filename) is True:
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
