import time
from contextlib import contextmanager
from loguru import logger


@contextmanager
def log_time():
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Execution time: {execution_time:.4f} seconds")
