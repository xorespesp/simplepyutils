import multiprocessing
import os
import os.path as osp
import threading
import traceback
from typing import Optional, Callable
import multiprocessing.pool
from contextlib import AbstractContextManager
import multiprocessing.dummy


class ThrottledPool(AbstractContextManager):
    """A multiprocessing pool that blocks on task submission if there are many pending tasks.

    This is a wrapper around :py:class:`multiprocessing.Pool <multiprocessing.pool.Pool>`,
    extended with a :py:class:`threading.Semaphore <threading.Semaphore>` to limit the number of
    tasks that can be pending (i.e., submitted but not yet processed) at any given time.

    This can be useful in throttling the task producer thread and avoiding too many tasks piling up
    in the queue and eating up too much RAM.

    Args:
        n_processes: the number of processes to use (defaults to the number of cores)
        task_buffer_size: the maximum number of tasks to be processed at once (defaults to 2
            * n_processes)
        initializer: a function to be called in each process at the beginning of its lifetime
    """

    def __init__(
        self,
        n_processes: Optional[int] = None,
        task_buffer_size: Optional[int] = None,
        initializer=None,
        use_threads=False,
    ):

        if n_processes is None:
            n_processes = len(os.sched_getaffinity(0))
        if task_buffer_size is None:
            task_buffer_size = 2 * n_processes
        if use_threads:
            self.pool = multiprocessing.pool.ThreadPool(processes=n_processes, initializer=initializer)
        else:
            self.pool = multiprocessing.Pool(processes=n_processes, initializer=initializer)
        self.task_semaphore = threading.Semaphore(task_buffer_size)

    def apply_async(
        self, f: Callable, args: tuple, kwargs: dict = None, callback: Optional[Callable] = None
    ) -> multiprocessing.pool.AsyncResult:
        """Submit a task to the pool. Blocks if there are already `task_buffer_size` tasks under
        processing.

        Args:
            f: the function to be called
            args: a tuple of arguments to be passed to the function
            kwargs: a dictionary of keyword arguments to be passed to the function
            callback: function to be called when the task is completed

        Returns:
            An object as returned by :py:meth:`multiprocessing.Pool.apply_async() \
            <multiprocessing.pool.Pool.apply_async>` yeah :py:class:`multiprocessing.Pool <multiprocessing.pool.Pool>`
        """
        self.task_semaphore.acquire()

        def on_task_completion(result):
            if callback is not None:
                callback(result)
            self.task_semaphore.release()

        if kwargs is None:
            kwargs = {}

        return self.pool.apply_async(safe_fun, args=(f, args, kwargs), callback=on_task_completion)

    def close(self):
        """Close the pool, i.e. prevent any new tasks from being submitted."""
        self.pool.close()

    def join(self):
        """Wait for all tasks to complete."""
        self.pool.join()

    def finish(self):
        self.pool.close()
        self.pool.join()
        self.pool.terminate()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager.

        Close the pool, wait for all tasks to complete, then terminate the pool.
        """
        self.finish()


class DummyPool(AbstractContextManager):
    """A dummy replacement for BoundedPool, for testing purposes."""

    def __init__(self, n_processes=None, task_buffer_size=None):
        pass

    def apply_async(self, f, args, kwargs=None, callback=None):
        if kwargs is None:
            kwargs = {}
        result = f(*args, **kwargs)
        if callback is not None:
            callback(result)

    def close(self):
        pass

    def join(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def safe_fun(f, args, kwargs):
    try:
        return f(*args, **kwargs)
    except BaseException:
        traceback.print_exc()
        raise
