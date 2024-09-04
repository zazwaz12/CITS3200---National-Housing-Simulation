"""
This type stub file was generated by pyright.
"""

from pathos.abstract_launcher import AbstractWorkerPool

"""
This module contains map and pipe interfaces to python's threading module.

Pipe methods provided:
    pipe        - blocking communication pipe             [returns: value]
    apipe       - asynchronous communication pipe         [returns: object]

Map methods provided:
    map         - blocking and ordered worker pool        [returns: list]
    imap        - non-blocking and ordered worker pool    [returns: iterator]
    uimap       - non-blocking and unordered worker pool  [returns: iterator]
    amap        - asynchronous worker pool                [returns: object]


Usage
=====

A typical call to a pathos threading map will roughly follow this example:

    >>> # instantiate and configure the worker pool
    >>> from pathos.threading import ThreadPool
    >>> pool = ThreadPool(nodes=4)
    >>>
    >>> # do a blocking map on the chosen function
    >>> print(pool.map(pow, [1,2,3,4], [5,6,7,8]))
    >>>
    >>> # do a non-blocking map, then extract the results from the iterator
    >>> results = pool.imap(pow, [1,2,3,4], [5,6,7,8])
    >>> print("...")
    >>> print(list(results))
    >>>
    >>> # do an asynchronous map, then get the results
    >>> results = pool.amap(pow, [1,2,3,4], [5,6,7,8])
    >>> while not results.ready():
    ...     time.sleep(5); print(".", end=' ')
    ...
    >>> print(results.get())
    >>>
    >>> # do one item at a time, using a pipe
    >>> print(pool.pipe(pow, 1, 5))
    >>> print(pool.pipe(pow, 2, 6))
    >>>
    >>> # do one item at a time, using an asynchronous pipe
    >>> result1 = pool.apipe(pow, 1, 5)
    >>> result2 = pool.apipe(pow, 2, 6)
    >>> print(result1.get())
    >>> print(result2.get())


Notes
=====

This worker pool leverages the python's multiprocessing.dummy module, and thus
has many of the limitations associated with that module. The function f and
the sequences in args must be serializable. The maps in this worker pool
have full functionality whether run from a script or in the python
interpreter, and work reliably for both imported and interactively-defined
functions. Unlike python's multiprocessing.dummy module, pathos.threading maps
can directly utilize functions that require multiple arguments.

"""
__all__ = ["ThreadPool", "_ThreadPool"]
_ThreadPool__STATE = ...

class ThreadPool(AbstractWorkerPool):
    """
    Mapper that leverages python's threading.
    """

    def __init__(self, *args, **kwds) -> None:
        """\nNOTE: if number of nodes is not given, will autodetect processors.
        \nNOTE: additional keyword input is optional, with:
            id          - identifier for the pool
            initializer - function that takes no input, called when node is spawned
            initargs    - tuple of args for initializers that have args
        """
        ...
    if AbstractWorkerPool.__init__.__doc__:
        ...
    clear = ...
    def map(self, f, *args, **kwds): ...
    def imap(self, f, *args, **kwds):  # -> IMapIterator | Generator[Any, None, None]:
        ...

    def uimap(
        self, f, *args, **kwds
    ):  # -> IMapUnorderedIterator | Generator[Any, None, None]:
        ...

    def amap(self, f, *args, **kwds):  # -> MapResult:
        ...

    def pipe(self, f, *args, **kwds): ...
    def apipe(self, f, *args, **kwds):  # -> ApplyResult:
        ...

    def __repr__(self):  # -> str:
        ...

    def restart(self, force=...):  # -> ThreadPool:
        "restart a closed pool"
        ...

    def close(self):  # -> None:
        "close the pool to any new jobs"
        ...

    def terminate(self):  # -> None:
        "a more abrupt close"
        ...

    def join(self):  # -> None:
        "cleanup the closed worker processes"
        ...
    nthreads = ...
    nodes = ...
    __state__ = ...
