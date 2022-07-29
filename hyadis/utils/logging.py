import logging
import os
from rich.logging import RichHandler

_default_logger = None


def init_logger():
    global _default_logger
    _default_logger = logging.getLogger("hyadis")

    level = logging.INFO

    _default_logger.setLevel(level)

    handler = RichHandler(level=level, show_path=False)
    formatter = logging.Formatter("%(message)s", datefmt="[%Y/%m/%d %H:%M:%S.%f]")
    handler.setFormatter(formatter)
    _default_logger.addHandler(handler)


def write_log_to_file(file):
    global _default_logger
    assert _default_logger is not None, "Logger is not initialized."

    path = os.path.dirname(file)
    os.makedirs(path, exist_ok=True)

    file_handler = logging.FileHandler(file, mode="a")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(name)s - %(levelname)s: %(message)s", datefmt="[%Y/%m/%d %H:%M:%S.%f]")
    file_handler.setFormatter(formatter)

    _default_logger.addHandler(file_handler)


def get_logger():
    return _default_logger


init_logger()
