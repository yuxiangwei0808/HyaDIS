import atexit
from typing import Optional

import ray

from .core import disable_hyadis_node, enable_hyadis_node
from .utils import get_logger


def enable_hyadis():
    enable_hyadis_node()


def disable_hyadis():
    disable_hyadis_node()


is_initialized = False


def init(address: Optional[str] = None, **kwargs):
    logger = get_logger()

    global is_initialized

    assert not is_initialized, "HyaDIS already initialized"
    assert not ray.is_initialized(), "Ray already initialized. Please do not initialize ray before HyaDIS."

    enable_hyadis()

    ray.init(address, **kwargs)

    is_initialized = True

    logger.info("HyaDIS is initialized.")


def shutdown(_exiting_interpreter: bool = False):
    logger = get_logger()

    global is_initialized

    if ray.is_initialized():
        ray.shutdown(_exiting_interpreter)

    if is_initialized:
        disable_hyadis()
        logger.info("HyaDIS is shut down.")


atexit.register(shutdown, True)
