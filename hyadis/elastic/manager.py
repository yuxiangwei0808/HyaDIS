from ray.worker import global_worker

from ..utils import get_logger
from .agent import ScalingAgent
from .worker_group import WorkerGroupManager


class JobManager:

    def __init__(
        self,
        num_workers=1,
        num_cpus_per_worker: int = 1,
        use_gpu: bool = False,
        autoscale: bool = True,
        batch_size: int = 1
    ):
        self.job_id = global_worker.current_job_id
        self.num_workers = num_workers
        self.num_cpus_per_worker = num_cpus_per_worker
        self.use_gpu = use_gpu
        self.autoscale = autoscale
        self.batch_size = batch_size
        self.worker_group = None
        self.scaling_agent = None

    def start(self):
        self.worker_group = WorkerGroupManager(self.num_workers, self.num_cpus_per_worker, self.use_gpu, self.batch_size)
        self.scaling_agent = ScalingAgent(self.worker_group, self.autoscale)
        self.scaling_agent.start()
        get_logger().info(f"Job manager started: {self.job_id}")

    def size(self):
        return self.worker_group.num_workers

    def resize(self, num_workers: int, optimizer):
        self.scaling_agent.resize(num_workers, optimizer)

    def set_epoch(self, epoch: int):
        return self.worker_group.set_epoch(epoch)

    def set_batch_size(self, batch_size: int):
        return self.worker_group.set_batch_size(batch_size)

    def shutdown(self):
        if self.scaling_agent is not None:
            self.scaling_agent.shutdown()
        if self.worker_group is not None:
            self.worker_group.shutdown()
        get_logger().info(f"Job manager ({self.job_id}) shutdown.")

    def register_initialization(self, fn):
        return self.worker_group.register_initialization(fn)

    def call_initialization(self, *args, **kwargs):
        return self.worker_group.call_initialization(*args, **kwargs)

    def register_train_step(self, fn):
        return self.worker_group.register_train_step(fn)

    def call_train_step(self, *args, **kwargs):
        return self.worker_group.call_train_step(*args, **kwargs)

    def get_module(self, key: str):
        return self.worker_group.get_module(key)
