import socket
import time
from dataclasses import dataclass
from enum import Enum
from functools import partial
from threading import Lock
from typing import Any, Callable, List, Optional, Union
import ray
import torch
import torch.distributed as dist
from torch.nn import Module
from torch.utils.data import DataLoader

from ..utils import get_ip_address, get_logger, get_unused_port
from .parallel import ElasticDistributedDataLoader, ElasticDistributedDataParallel
from .state import State


class WorkerStatus(Enum):
    START = 0
    INITIALIZING = 1
    INITIALIZED = 2
    SYNCHRONIZING = 3
    SYNCHRONIZED = 4
    READY = 5
    RUNNING = 6
    COMPLETE = 7


@dataclass
class WorkerMetadata:
    node_id: str
    node_ip: str
    hostname: str
    gpu_id: Optional[int]
    rank: Optional[int] = None
    status: Optional[WorkerStatus] = WorkerStatus.START
    mutex: Optional[Lock] = None


class Worker:

    def __init__(self):
        self.process_group = None
        self.named_modules = dict()
        self.named_values = dict()
        self.state = State()
        self.epoch = 0
        self.dataloader = None
        self.data_iter = None
        self.init_fn = None
        self.base_train_step_fn = None
        self.train_step_fn = None
        device = "cpu" if not torch.cuda.is_available() else f"cuda:{self.gpu_id}"
        get_logger().info(f"Worker started on {self.node_ip} - {device}.")

    def get_metadata(self):
        return WorkerMetadata(
            node_id=self.node_id,
            node_ip=self.node_ip,
            hostname=self.hostname,
            gpu_id=self.gpu_id,
        )

    def get_module(self, key: str):
        if key in self.named_modules:
            return self.named_modules[key]
        elif key in self.named_values:
            value = self.named_values[key]
            if isinstance(value, torch.Tensor):
                value = value.detach().cpu()
            return value
        else:
            if key.startswith("reduced_"):
                key = key[8:]
                if key in self.named_values:
                    value = self._reduce_return_value(self.named_values[key])
                    if isinstance(value, torch.Tensor):
                        value = value.detach().cpu()
                    return value
            raise AttributeError(key)

    def set_epoch(self, epoch: int):
        self.epoch = epoch
        self.dataloader.set_epoch(self.epoch)

    def register_initialization(self, fn):
        if self.init_fn is None and fn is not None:
            self.init_fn = fn

    def _wrap_train_func(self):
        if self.base_train_step_fn is not None:
            kwargs = {
                name: module
                for name, module in self.named_modules.items()
                if not isinstance(module, (DataLoader, ElasticDistributedDataLoader))
            }
            self.train_step_fn = partial(self.base_train_step_fn, **kwargs)

    def call_initialization(self, *args, **kwargs):
        assert self.init_fn is not None and callable(self.init_fn)
        initialized_modules = self.init_fn(*args, **kwargs)
        if not isinstance(initialized_modules, dict):
            raise RuntimeError("Initialization function does not return a dictionary, which helps the runner use " +
                               "these key-value pairs to set its attributes.")

        self.named_modules = dict()
        for name, module in initialized_modules.items():
            self.named_modules[name] = module
            if State.is_loadable(module):
                self.state[name] = module
            if isinstance(module, DataLoader):
                raise RuntimeError("Please use hyadis.elastic.data.ElasticDistributedDataloader intead of " +
                                   "torch.utils.data.DataLoader.")
            elif isinstance(module, ElasticDistributedDataLoader):
                self.dataloader = module
                self.dataloader.set_epoch(self.epoch)
            elif isinstance(module, Module):
                module.to(self.device)

        self._wrap_train_func()

    def register_train_step(self, fn):
        if self.base_train_step_fn is None and fn is not None:
            self.base_train_step_fn = fn
        self._wrap_train_func()

    def _reduce_return_value(self, val: Any):
        if isinstance(val, (int, float, torch.Tensor)):
            reduced_val = val
            if not isinstance(reduced_val, torch.Tensor):
                reduced_val = torch.tensor(reduced_val)
            reduced_val = reduced_val.to(self.device)
            dist.all_reduce(reduced_val)
            reduced_val /= self.world_size
            if isinstance(val, torch.Tensor):
                return reduced_val
            else:
                return reduced_val.item()
        else:
            return None

    def call_train_step(self, *args, **kwargs):
        if self.dataloader is not None:
            if self.data_iter is None:
                self.data_iter = iter(self.dataloader)
            try:
                batch = next(self.data_iter)
            except StopIteration:
                self.data_iter = iter(self.dataloader)
                return True
        else:
            batch = None

        assert self.train_step_fn is not None and callable(self.train_step_fn)
        if batch is not None:
            return_values = self.train_step_fn(batch, *args, **kwargs)
        else:
            return_values = self.train_step_fn(*args, **kwargs)
        if return_values is not None:
            if isinstance(return_values, dict):
                for name, value in return_values.items():
                    self.named_values[name] = value
            else:
                raise RuntimeError("Train function does not return a dictionary, which helps the runner use " +
                                   "these key-value pairs to set its attributes.")
        return False

    def _resize(self):
        for module in self.named_modules.values():
            if isinstance(module, ElasticDistributedDataLoader):
                module.resize()
            elif isinstance(module, ElasticDistributedDataParallel):
                module.resize(self.process_group)

    def setup_dist(self, world_size: int, rank: int, init_method: str, backend: str = 'nccl'):
        if self.process_group is not None:
            dist.destroy_process_group(group=self.process_group)
            self.process_group = None

        dist.init_process_group(backend=backend, init_method=init_method, world_size=world_size, rank=rank)
        self.process_group = dist.distributed_c10d._get_default_group()
        self._resize()

    def synchronize(self):
        state_dict = self.state.state_dict()
        dist.broadcast_object_list([state_dict], src=0, device=self.device)
        self.state.load_state_dict(state_dict)

    @property
    def node_id(self):
        return ray.get_runtime_context().node_id.hex()

    @property
    def node_ip(self):
        return ray.util.get_node_ip_address()

    @property
    def hostname(self):
        return socket.gethostname()

    @property
    def gpu_id(self):
        return ray.get_gpu_ids()[0]

    @property
    def device(self):
        if torch.cuda.is_available():
            return torch.device("cuda", torch.cuda.current_device())
        else:
            return torch.device("cpu")

    @property
    def world_size(self):
        if self.process_group is not None:
            return dist.get_world_size(self.process_group)
        else:
            return 1

    @property
    def rank(self):
        if self.process_group is not None:
            return dist.get_rank(self.process_group)
        else:
            return 0


class WorkerGroupManager:

    def __init__(
        self,
        num_workers=1,
        num_cpus_per_worker: int = 1,
        use_gpu: bool = False,
        wait_for_resource_interval: float = 0.1,
    ) -> None:

        self.num_workers = num_workers
        self.num_cpus_per_worker = num_cpus_per_worker
        self.num_gpus_per_worker = 1 if use_gpu else 0
        self.use_gpu = use_gpu
        self.wait_for_resource_interval = wait_for_resource_interval
        self.workers = list()
        self.worker_metadata = list()
        self.init_fn = None
        self.init_args = None
        self.init_kwargs = None
        self.train_step_fn = None
        self.init_invoked = False
        self.worker_actor_cls = ray.remote(
            num_cpus=self.num_cpus_per_worker,
            num_gpus=self.num_gpus_per_worker,
        )(Worker)
        self.resource_update_lock = Lock()

        with self.resource_update_lock:
            self._add_new_workers(self.num_workers)
        get_logger().info(f"Worker group manager started: {self.num_workers} worker(s).")

    def get_module(self, key: str):
        assert len(self.workers) > 0, "No worker is running."
        return ray.get([w.get_module.remote(key) for w in self.workers])[0]

    def update_workers(self, num_new_workers: int, workers_to_release: List[int]):
        with self.resource_update_lock:
            if num_new_workers > 0:
                self._add_new_workers(num_new_workers)
            if len(workers_to_release) > 0:
                self._release_workers(workers_to_release)

    def _add_new_workers(self, num_workers: int):
        get_logger().info("Creating worker(s) ...")
        rank_offset = len(self.workers)
        new_workers = [self.worker_actor_cls.remote() for _ in range(num_workers)]
        new_worker_metadata = ray.get([w.get_metadata.remote() for w in new_workers])
        for i, metadata in enumerate(new_worker_metadata):
            metadata.rank = rank_offset + i
            metadata.mutex = Lock()

        # initialize new workers if other workers are already initialized
        futures = list()
        if self.init_fn is not None:
            futures.extend([w.register_initialization.remote(self.init_fn) for w in new_workers])
        if self.train_step_fn is not None:
            futures.extend([w.register_train_step.remote(self.train_step_fn) for w in new_workers])
        ray.get(futures)
        if self.init_invoked:
            self.call_initialization(*self.init_args,
                                     workers=new_workers,
                                     worker_metadata=new_worker_metadata,
                                     **self.init_kwargs)

        self.workers.extend(new_workers)
        self.worker_metadata.extend(new_worker_metadata)
        self.num_workers = len(self.workers)

        get_logger().info("Synchronizing worker(s) ...")
        # synchronize all workers
        self._synchronize()
        get_logger().info(f"{num_workers} worker(s) created.")

    def _release_workers(self, worker_list: List[int]):
        new_workers = list()
        new_worker_metadata = list()
        for i in range(len(self.workers)):
            if i not in worker_list:
                new_workers.append(self.workers[i])
                self.worker_metadata[i].rank = len(new_worker_metadata)
                new_worker_metadata.append(self.worker_metadata[i])
            else:
                self.worker_metadata[i].rank = -1
                self._update_status(self.worker_metadata[i], WorkerStatus.COMPLETE)
                get_logger().info(
                    f"Worker {i} ({self.worker_metadata[i].node_ip} - {self.worker_metadata[i].gpu_id}) released.")

        self.workers = new_workers
        self.worker_metadata = new_worker_metadata
        self.num_workers = len(self.workers)

        self._synchronize()

    def _wait_for_resource(self):
        while len(self.workers) == 0:
            time.sleep(self.wait_for_resource_interval)

    def _update_status(self, workers: Union[WorkerMetadata, List[WorkerMetadata]], status: WorkerStatus):

        def _update(worker):
            with worker.mutex:
                worker.status = status

        if isinstance(workers, list):
            for w in workers:
                _update(w)
        elif isinstance(workers, WorkerMetadata):
            _update(workers)

    def _validate_status(self, workers: Union[WorkerMetadata, List[WorkerMetadata]], status: List[WorkerStatus]):

        def _validate(worker):
            if worker.status not in status:
                raise RuntimeError(f"Calling initialization function on worker that is {worker.status}," +
                                   f" expected {status}.")

        if isinstance(workers, list):
            for w in workers:
                _validate(w)
        elif isinstance(workers, WorkerMetadata):
            _validate(workers)

    def _find_status(self, workers: Union[WorkerMetadata, List[WorkerMetadata]], status: List[WorkerStatus], mode=any):

        def _find(worker):
            return worker.status in status

        if isinstance(workers, list):
            return mode([_find(w) for w in workers])
        elif isinstance(workers, WorkerMetadata):
            return _find(workers)

    def _wait_for_status(self,
                         workers: Union[WorkerMetadata, List[WorkerMetadata]],
                         status: List[WorkerStatus],
                         mode=all):
        ready = False
        while not ready:
            ready = self._find_status(workers, status, mode=mode)

    def _synchronize(self):
        self._wait_for_status(self.worker_metadata, [WorkerStatus.START, WorkerStatus.INITIALIZED, WorkerStatus.READY])
        # if some workers are INITIALIZED while some others are READY, we synchronize state
        do_broadcast = self._find_status(self.worker_metadata, [WorkerStatus.READY]) and \
            self._find_status(self.worker_metadata, [WorkerStatus.INITIALIZED])
        done_status = WorkerStatus.SYNCHRONIZED \
            if self._find_status(self.worker_metadata, [WorkerStatus.START], mode=all) else WorkerStatus.READY
        self._update_status(self.worker_metadata, WorkerStatus.SYNCHRONIZING)

        # setup distributed connection
        addr = get_ip_address()
        port = get_unused_port()
        init_method = f"tcp://{addr}:{port}"
        world_size = len(self.workers)
        backend = 'nccl' if self.use_gpu else 'gloo'
        ray.get([w.setup_dist.remote(world_size, i, init_method, backend) for i, w in enumerate(self.workers)])

        # synchonize state
        if do_broadcast:
            ray.get([w.synchronize.remote() for w in self.workers])

        self._update_status(self.worker_metadata, done_status)

    def shutdown(self, patience_s: float = 5):
        """graceful shutdown, adopted from ray.train.worker_group.WorkerGroup.shutdown
        """
        num_workers = self.num_workers
        if patience_s <= 0:
            for worker in self.workers:
                ray.kill(worker.actor)
        else:
            done_refs = [w.__ray_terminate__.remote() for w in self.workers]
            # Wait for actors to die gracefully.
            done, not_done = ray.wait(done_refs, timeout=patience_s)
            if not_done:
                for worker in self.workers:
                    ray.kill(worker)

        self.workers = []
        get_logger().info(f"Shut down {num_workers} worker(s).")
        get_logger().info("Worker group manager shutdown.")

    def set_epoch(self, epoch: int):
        ray.get([w.set_epoch.remote(epoch) for w in self.workers])

    def register_initialization(self, fn: Callable):
        self.init_fn = fn
        ray.get([w.register_initialization.remote(fn) for w in self.workers])

    def call_initialization(self, *args, workers=None, worker_metadata=None, **kwargs):
        if workers is None:
            assert worker_metadata is None
            workers = self.workers
            worker_metadata = self.worker_metadata
        self._validate_status(worker_metadata, [WorkerStatus.START, WorkerStatus.SYNCHRONIZED])
        if not self.init_invoked:
            self.init_args = args
            self.init_kwargs = kwargs
        done_status = WorkerStatus.READY \
            if self._find_status(worker_metadata, [WorkerStatus.SYNCHRONIZED]) else WorkerStatus.INITIALIZED
        self._update_status(worker_metadata, WorkerStatus.INITIALIZING)
        ray.get([w.call_initialization.remote(*args, **kwargs) for w in workers])
        self._update_status(worker_metadata, done_status)
        self.init_invoked = True

    def register_train_step(self, fn: Callable):
        self.train_step_fn = fn
        ray.get([w.register_train_step.remote(fn) for w in self.workers])

    def call_train_step(self, *args, workers=None, worker_metadata=None, **kwargs):
        if workers is None:
            assert worker_metadata is None
            workers = self.workers
            worker_metadata = self.worker_metadata
        self._validate_status(worker_metadata, [WorkerStatus.READY])
        self._update_status(worker_metadata, WorkerStatus.RUNNING)
        stop_iteration = any(ray.get([w.call_train_step.remote(*args, **kwargs) for w in workers]))
        self._update_status(worker_metadata, WorkerStatus.READY)
        return stop_iteration
