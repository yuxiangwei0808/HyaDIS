import ray
from ray.worker import global_worker

from ..utils import get_logger
from .manager import JobManager

_register_pool = dict(
    register_initialization=None,
    register_train_step=None,
)


class ElasticRunner:

    def __init__(
        self,
        num_workers: int = 1,
        num_cpus_per_worker: int = 1,
        use_gpu: bool = False,
        autoscale: bool = True,
        batch_size: int = 1,
    ):
        self.job_id = global_worker.current_job_id
        get_logger().info(f"Starting runner ({self.job_id}) ...")
        manager_actor = ray.remote(num_cpus=0)(JobManager)
        self.job_manager = manager_actor.remote(
            num_workers=num_workers,
            num_cpus_per_worker=num_cpus_per_worker,
            use_gpu=use_gpu,
            autoscale=autoscale,
            batch_size=batch_size,
        )

        ray.get(self.job_manager.start.remote())

        get_logger().info(f"Runner ({self.job_id}) started.")

        # register worker functions
        for key, fn in _register_pool.items():
            getattr(self, key)(fn)

    def __getattr__(self, key: str):
        return ray.get(self.job_manager.get_module.remote(key))

    def size(self):
        return ray.get(self.job_manager.size.remote())

    def resize(self, num_workers: int, optimizer):
        ray.get(self.job_manager.resize.remote(num_workers, optimizer))

    def shutdown(self):
        ray.get(self.job_manager.shutdown.remote())
        get_logger().info(f"Runner ({self.job_id}) shutdown.")

    # TODO: we need to find out how to squeeze up these register and call functions.
    def set_epoch(self, epoch: int):
        ray.get(self.job_manager.set_epoch.remote(epoch))

    def register_initialization(self, fn):
        if fn is not None:
            ray.get(self.job_manager.register_initialization.remote(fn))

    def call_initialization(self, *args, **kwargs):
        ray.get(self.job_manager.call_initialization.remote(*args, **kwargs))

    def register_train_step(self, fn):
        if fn is not None:
            ray.get(self.job_manager.register_train_step.remote(fn))

    def call_train_step(self, *args, **kwargs):
        return ray.get(self.job_manager.call_train_step.remote(*args, **kwargs))


_runner = None


def init(
    num_workers: int = 1,
    num_cpus_per_worker: int = 1,
    use_gpu: bool = False,
    autoscale: bool = True,
):
    global _runner
    _runner = ElasticRunner(num_workers, num_cpus_per_worker, use_gpu, autoscale)
    return _runner


def call_initialization(*args, **kwargs):
    assert _runner is not None, "Elastic runner is not initialized. Please call hyadis.elastic.init first."
    return _runner.call_initialization(*args, **kwargs)


def initialization(fn):
    if _runner is None:
        _register_pool['register_initialization'] = fn
    else:
        _runner.register_initialization(fn)
    return call_initialization


def call_train_step(*args, **kwargs):
    assert _runner is not None, "Elastic runner is not initialized. Please call hyadis.elastic.init first."
    return _runner.call_train_step(*args, **kwargs)


def train_step(fn):
    if _runner is None:
        _register_pool['register_train_step'] = fn
    else:
        _runner.register_initialization(fn)
    return call_train_step
