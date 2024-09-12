from typing import Callable, Sequence
from pathos.multiprocessing import ProcessingPool as Pool  # type: ignore


def parallel_map[
    T, R
](data_lst: list[T], func: Callable[[T], R], num_cores: int) -> Sequence[R]:
    """
    Map `func` to each element of `data_lst` in parallel using `num_cores` cores.
    """
    with Pool(num_cores) as pool:
        all_results = pool.map(func, data_lst)  # type: ignore
    return all_results  # type: ignore
