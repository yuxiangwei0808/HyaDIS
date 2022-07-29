import time

import hyadis
import ray
import torch
from hyadis.utils import get_logger


@hyadis.elastic.initialization
def test_init():
    model = torch.nn.Linear(8, 8).to(torch.cuda.current_device())
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
    return dict(model=model, criterion=criterion, optim=optimizer)


@hyadis.elastic.train_step
def test_train(model=None, criterion=None, optim=None):
    x = torch.randn(4, 8).to(torch.cuda.current_device())
    y = torch.randint(8, (4,)).to(torch.cuda.current_device())
    o = model(x)
    loss = criterion(o, y)
    loss.backward()
    optim.step()
    return dict(loss=loss)


ray.init()
runner = hyadis.elastic.init(use_gpu=True, autoscale=False)

runner.resize(4)

logger = get_logger()

test_init()
test_train()

logger.info(f"Loss = {runner.reduced_loss.item()}")

# runner.resize(2)

# time.sleep(60)

runner.shutdown()
