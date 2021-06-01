import time
import logging
from pathlib import Path
from rich.logging import RichHandler
from typing import Callable
from functools import wraps


# logging paths
LOGS_DIR = Path(__file__).parent.parent / "data/logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)


# setting for stdout and file logging
logger = logging.getLogger(__name__)
stdout_handler = RichHandler()
file_handler = logging.FileHandler(f"{LOGS_DIR}/debug.log")

# logging levels
logger.setLevel(logging.DEBUG)
stdout_handler.setLevel(logging.INFO)
file_handler.setLevel(logging.DEBUG)

# the formatters look
fmt_stdout = "%(message)s"
fmt_file = (
    "[%(levelname)s] %(asctime)s | %(filename)s:%(funcName)s:%(lineno)d | %(message)s"
)

stdout_formatter = logging.Formatter(fmt_stdout)
file_formatter = logging.Formatter(fmt_file)

# set formatters
stdout_handler.setFormatter(stdout_formatter)
file_handler.setFormatter(file_formatter)

# add handlers
logger.addHandler(stdout_handler)
logger.addHandler(file_handler)


# pipeline logger
def log_pipeline(function: Callable) -> Callable:
    @wraps(function)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = function(*args, **kwargs)
        time_taken = time.perf_counter() - start_time
        logger.debug(
            f"{function.__name__} completed | shape = {result.shape:} | time {time_taken:.3f}s"
        )
        return result

    return wrapper
