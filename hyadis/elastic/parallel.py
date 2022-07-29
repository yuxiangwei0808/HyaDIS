import math
from typing import OrderedDict

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler
import torch.optim as optim


class ElasticDistributedSampler(DistributedSampler):

    def __init__(self, dataset, shuffle=True, seed=0, drop_last=False):
        self._idx = 0
        super().__init__(dataset, self.world_num_replicas, self.world_rank, shuffle, seed, drop_last)

    def state_dict(self):
        return dict(epoch=self.epoch, idx=self.idx)

    def load_state_dict(self, state_dict: OrderedDict):
        if "epoch" in state_dict:
            self.set_epoch(state_dict["epoch"])
        if "idx" in state_dict:
            self._idx = state_dict["idx"]

    @property
    def world_num_replicas(self):
        return dist.get_world_size() if dist.is_initialized() else 1

    @property
    def world_rank(self):
        return dist.get_rank() if dist.is_initialized() else 0

    @property
    def idx(self):
        return self._idx

    def __iter__(self):
        """ Adopted from torch.utils.data.distributed.DistributedSampler.__iter__
        """
        if self.shuffle:
            g = torch.Generator()
            g.manual_seed(self.seed + self.epoch)
            indices = torch.randperm(len(self.dataset), generator=g).tolist()
        else:
            indices = list(range(len(self.dataset)))

        if self.drop_last and (len(indices) - self.idx) % self.num_replicas != 0:
            num_samples = math.ceil((len(indices) - self.idx - self.num_replicas) / self.num_replicas)
        else:
            num_samples = math.ceil((len(indices) - self.idx) / self.num_replicas)
        total_size = num_samples * self.num_replicas

        if not self.drop_last:
            padding_size = total_size + self.idx - len(indices)
            if padding_size <= len(indices):
                indices += indices[:padding_size]
            else:
                indices += (indices * math.ceil(padding_size / len(indices)))[:padding_size]
        else:
            indices = indices[:self.idx + total_size]
        assert len(indices) == self.idx + total_size

        indices = indices[self.idx + self.rank:self.idx + total_size:self.num_replicas]
        assert len(indices) == num_samples

        return iter(indices)

    def __next__(self):
        try:
            self._idx += 1
            return super().__next__()
        except StopIteration as e:
            self._idx = 0
            raise e


class ElasticDistributedDataLoader(object):

    def __init__(
        self,
        dataset,
        batch_size=1,
        shuffle=True,
        seed=0,
        drop_last=False,
        **kwargs,
    ):
        self.dataset = dataset
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.seed = seed
        self.drop_last = drop_last
        self.kwargs = kwargs

        self.sampler = None
        self.dataloader = None
        self._build()

    def _build(self):
        self.sampler = ElasticDistributedSampler(self.dataset,
                                                 shuffle=self.shuffle,
                                                 seed=self.seed,
                                                 drop_last=self.drop_last)

        self.dataloader = DataLoader(
            self.dataset,
            batch_size=self.batch_size,
            sampler=self.sampler,
            drop_last=self.drop_last,
            **self.kwargs,
        )

    def __iter__(self):
        return iter(self.dataloader)

    def resize(self):
        state = self.state_dict()
        self._build()
        self.load_state_dict(state)

    def state_dict(self):
        return self.sampler.state_dict()

    def load_state_dict(self, state_dict: OrderedDict):
        self.sampler.load_state_dict(state_dict)

    def set_epoch(self, epoch: int):
        self.sampler.set_epoch(epoch)


class ElasticDistributedDataParallel(torch.nn.Module):

    def __init__(self, module, process_group=None, **kwargs):
        super().__init__()
        self.base_module = module
        self.ddp_kwargs = kwargs
        self.module = None
        self._build(process_group)

    def _build(self, process_group=None):
        if dist.is_initialized() and dist.get_world_size(process_group) > 1:
            self.module = DistributedDataParallel(self.base_module, process_group=process_group, **self.ddp_kwargs)
        else:
            self.module = self.base_module

    def resize(self, process_group=None):
        self._build(process_group=process_group)

    def forward(self, *args, **kwargs):
        return self.module(*args, **kwargs)


class ElasticOptimizer:
    """Set the accumulation duration according to the current number of worker versus the max number of worker.
    Ceil is adopted currently if the change is not integer multiple"""
    def __init__(
            self,
            optimizer: optim.optimizer,
                 ):
        self.max_workers = 1
        self.optimizer = optimizer
        self.accumulation_iter = 1
        self.accumulated_iter = 1

    def accumulate(self, new_num_workers):
        if new_num_workers > self.max_workers:
            self.max_workers = new_num_workers
        else:
            self.accumulation_iter = math.ceil(self.max_workers / new_num_workers)

    def step(self):
        if self.accumulated_iter == self.accumulation_iter:
            self.optimizer.step()
            self.accumulated_iter = 1
        else:
            self.accumulated_iter += 1

    def set_workers(self, num_workers):
        """calls at the initialization stage"""
        self.max_workers = num_workers
