from contextlib import contextmanager
from time import time

from loguru import logger


@contextmanager
def log_time():
    """Log the time taken for a block of code to execute."""
    start_time = time()
    try:
        yield
    finally:
        end_time = time()
        elapsed_time = end_time - start_time
        logger.info(f"Time taken: {elapsed_time:.2f} seconds")
