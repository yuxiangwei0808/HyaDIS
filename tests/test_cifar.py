import argparse
import time

import hyadis
import ray
import torch
from hyadis.elastic.parallel import ElasticDistributedDataLoader, ElasticDistributedDataParallel
from hyadis.utils import get_logger
from torchvision import datasets
from torchvision.models import resnet18
from torchvision.transforms import transforms


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_workers', type=int)
    parser.add_argument('--data_path', type=str)
    parser.add_argument('--batch_size', type=int)
    return parser.parse_args()


@hyadis.elastic.initialization
def test_init(path, batch_size):
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5]),
        transforms.Resize((224, 224))
    ])

    training_data = datasets.CIFAR10(
        root=path,
        train=True,
        download=True,
        transform=transform,
    )

    model = resnet18()
    model.to(torch.cuda.current_device())
    model = ElasticDistributedDataParallel(model)
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.1)
    dataloader = ElasticDistributedDataLoader(dataset=training_data,
                                              batch_size=batch_size,
                                              shuffle=True,
                                              drop_last=False)

    return dict(model=model, data=dataloader, optim=optimizer, criterion=criterion)


@hyadis.elastic.train_step
def test_train(data, model=None, optim=None, criterion=None):
    model.train()
    x, y = data
    x, y = x.to(torch.cuda.current_device()), y.to(torch.cuda.current_device())
    optim.zero_grad()
    o = model(x)
    loss = criterion(o, y)
    loss.backward()
    optim.step()
    return dict(loss=loss)


def train(epoch, runner, batch_size):
    logger = get_logger()
    runner.set_epoch(epoch)
    epoch_end = False
    num_samples = 0
    start_time = time.time()
    while not epoch_end:
        epoch_end = test_train()
        num_samples += batch_size * runner.size()

    end_time = time.time()
    logger.info(f"[Epoch {epoch}] Loss = {runner.reduced_loss.item():.3f} | " +
                f"Throughput = {num_samples/(end_time - start_time):.3f}")


def main():
    args = get_args()
    print(f"Job (size={args.num_workers}) start")
    job_start = time.time()
    ray.init(address='auto')
    runner = hyadis.elastic.init(args.num_workers, use_gpu=True, autoscale=False)
    print(f"Job (size={args.num_workers}) ready")

    batch_size = args.batch_size

    test_init(args.data_path, batch_size)

    runner.resize(4)
    for epoch in range(3):
        train(epoch, runner, batch_size)

    job_end = time.time()
    print(f"Job (size={runner.size()}) complete: used time = {job_end - job_start:.3f}")

    runner.shutdown()


if __name__ == "__main__":
    main()
