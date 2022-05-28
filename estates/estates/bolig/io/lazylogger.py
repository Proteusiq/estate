import logging
import sys
import time
from tempfile import gettempdir
from typing import Callable
from functools import wraps
from pathlib import Path


# logging paths
TEMP_DIR = gettempdir()
LOGS_DIR = Path(TEMP_DIR)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


# setting for stdout and file logging
logger = logging.getLogger(__name__)
stdout_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler(f"{LOGS_DIR}/bolig.log")

# logging levels
logger.setLevel(logging.INFO)
stdout_handler.setLevel(logging.INFO)
file_handler.setLevel(logging.ERROR)

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


def catch(f: Callable) -> Callable:
    @wraps(f)
    def wrap(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            return result
        except Exception as e:
            logger.error(e)
            raise e

    return wrap


def pipeline(function: Callable) -> Callable:
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


logger.catch = catch  # type:ignore
logger.pipelines = pipeline  # type:ignore
