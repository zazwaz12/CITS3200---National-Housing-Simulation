from contextlib import contextmanager
from loguru import logger
from time import time


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
