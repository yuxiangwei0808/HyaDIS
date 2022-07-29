import time
from threading import Thread

import ray
from ray.state import state

from ..utils import get_logger


class NodeResource:

    def __init__(self, node_id, node_address, num_gpus):
        self.node_id = node_id
        self.address = node_address
        self.num_gpus = num_gpus
        self.allocated_to = list()

    def alloc(self, rank):
        success = False
        if len(self.allocated_to) < self.num_gpus:
            self.allocated_to.append(rank)
            success = True
        return success

    def num_free_gpus(self):
        return int(self.num_gpus - len(self.allocated_to)) \
            if len(self.allocated_to) < self.num_gpus else 0


class ScalingAgent:

    def __init__(
        self,
        worker_group,
        autoscale: bool = True,
        interval: float = 1.,
    ):
        self.state = state.global_state_accessor
        self.worker_group = worker_group
        self.autoscale = autoscale
        self.interval = interval
        self.keep_running = False
        self.thread = Thread(target=self._listen_to_resource_change, daemon=True)

    def start(self):
        self.keep_running = True
        self.thread.start()
        get_logger().info("Scaling agent started.")

    def resize(self, num_workers: int):
        get_logger().info(f"Resizing workers: {self.worker_group.num_workers} --> {num_workers}")
        available_resources = dict()
        # TODO: we should fetch available resources of the current task from scheduler
        for node in ray.nodes():
            if node['Alive']:
                available_resources[node['NodeID']] = NodeResource(
                    node['NodeID'],
                    node['NodeManagerAddress'],
                    node['Resources']['GPU'],
                )

        total_workers = 0
        workers_to_release = list()
        for worker in self.worker_group.worker_metadata:
            node = available_resources.get(worker.node_id, None)
            if node is None or (not node.alloc(worker.rank)):
                workers_to_release.append(worker.rank)
            else:
                total_workers += 1
                if total_workers >= num_workers:
                    break

        num_new_workers = num_workers - total_workers
        self.worker_group.update_workers(num_new_workers, workers_to_release)

    def shutdown(self):
        self.keep_running = False
        get_logger().info("Scaling agent shutdown.")

    def _check_placement(self):
        available_resources = dict()
        # TODO: we should fetch available resources of the current task from scheduler
        for node in ray.nodes():
            if node['Alive']:
                available_resources[node['NodeID']] = NodeResource(
                    node['NodeID'],
                    node['NodeManagerAddress'],
                    node['Resources']['GPU'],
                )

        workers_to_release = list()
        for worker in self.worker_group.worker_metadata:
            node = available_resources.get(worker.node_id, None)
            if node is None or (not node.alloc(worker.rank)):
                workers_to_release.append(worker.rank)

        num_new_workers = 0
        for _, node in available_resources.items():
            num_new_workers += node.num_free_gpus()

        return num_new_workers, workers_to_release

    def _listen_to_resource_change(self):
        while self.keep_running:
            time.sleep(self.interval)
            if self.autoscale:
                num_new_workers, workers_to_release = self._check_placement()
                if num_new_workers > 0 or len(workers_to_release) > 0:
                    get_logger().info(
                        f"Changing resource allocation (+{num_new_workers}, -{len(workers_to_release)})...")
                    self.worker_group.update_workers(num_new_workers, workers_to_release)
                    get_logger().info("Resource allocation updated.")
