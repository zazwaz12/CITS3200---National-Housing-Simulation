from typing import Any, Callable

from pathos.multiprocessing import ProcessingPool as Pool


def compute_in_parallel(*jobs: Callable[[], Any]) -> tuple[Any, ...]:
    """
    Compute a list of jobs in parallel using multiple processes.

    Parameters
    ----------
    jobs : List[Callable]
        A list of jobs to compute in parallel. Each function
        must be a function with no arguments.
    """
    with Pool() as pool:
        results = pool.map(lambda job: job(), jobs)
    return tuple(results)
