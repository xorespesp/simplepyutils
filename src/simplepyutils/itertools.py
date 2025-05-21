import itertools
import queue
import threading
from collections.abc import Iterable, Iterator
from typing import Any, Optional
import numpy as np


def roundrobin(iterables: list[Iterable[Any]], sizes: Iterable[int]):
    """Yields elements from the given iterables in a round-robin fashion.

    Args:
        iterables: a list of iterables
        sizes: integers, the number of elements to take from each iterable in each round

    Returns:
        elements from the given iterables in a round-robin fashion
    """
    iterators = [iter(iterable) for iterable in iterables]
    for iterator, size in zip(itertools.cycle(iterators), itertools.cycle(sizes)):
        for _ in range(size):
            try:
                yield next(iterator)
            except StopIteration:
                return


def iterate_repeatedly(
        seq: Iterable[Any],
        shuffle_before_each_epoch: bool = False,
        rng: Optional[np.random.RandomState] = None,
):
    """Iterates over and yields the elements of `iterable` over and over.

    Args:
        seq: sequence to iterate over
        shuffle_before_each_epoch: if True, the elements are put in a list and shuffled before
            every pass over the data, including the first.
        rng: random number generator to use for shuffling. If None, a new one is created.

    Returns:
        elements of `iterable` in a repeated fashion
    """

    if rng is None:
        rng = np.random.RandomState()

    # create a (shallow) copy so shuffling only applies to the copy.
    seq = list(seq)
    rng.shuffle(seq)
    yield from seq

    while True:
        if shuffle_before_each_epoch:
            rng.shuffle(seq)
        yield from seq


def roundrobin_iterate_repeatedly(
        seqs: Iterable[Iterable[Any]],
        roundrobin_sizes: Iterable[int],
        shuffle_before_each_epoch: bool = False,
        rng: Optional[np.random.RandomState] = None,
):
    """Iterates over the given sequences in a round-robin fashion, yielding elements from each,
    and repeating the sequences indefinitely.

    Args:
        seqs: sequences to iterate over
        roundrobin_sizes: integers, the number of elements to take from each sequence in
            each round
        shuffle_before_each_epoch: if True, the elements are put in a list and shuffled before
            every pass over the data, including the first.
        rng: random number generator to use for shuffling. If None, a new one is created.

    Returns:
        iterable over the elements of the sequences in a round-robin fashion.
    """
    iters = [iterate_repeatedly(seq, shuffle_before_each_epoch, util.new_rng(rng)) for seq in seqs]
    return roundrobin(iters, roundrobin_sizes)


def nested_spy_first(iterable: Iterable[Any], levels: int = 1):
    it = iter(iterable)
    head = next(it)

    if levels == 1:
        return head, itertools.chain([head], it)

    deep_head, new_head = nested_spy_first(head, levels=levels - 1)
    return deep_head, itertools.chain([new_head], it)


def prefetch(seq: Iterable[Any], buffer_size: int) -> Iterable[Any]:
    """Prefetch elements in a separate thread.

    Args:
        seq: sequence to prefetch elements from
        buffer_size: number of elements to prefetch at a time
    """
    q = queue.Queue(buffer_size)
    end_of_sequence_marker = object()

    def producer():
        for elem in seq:
            q.put(elem)
        q.put(end_of_sequence_marker)

    producer_thread = threading.Thread(target=producer)
    producer_thread.start()

    try:
        while (elem := q.get()) is not end_of_sequence_marker:
            yield elem
    finally:
        producer_thread.join()


def repeat_n(iterable, n):
    for item in iterable:
        for _ in range(n):
            yield item


def filter_by_index(iterable, indices, enumerate=False):
    """
    Selects and yields elements from an iterable at specified indices.

    Args:
        iterable (Iterable): The input sequence or iterable to select elements from.
        indices (Iterable[int]): An iterable of sorted (non-decreasing) indices
            specifying which elements to yield.
        enumerate (bool): If True, yields a tuple of the index and the element.

    Yields:
        Any: The elements from `iterable` corresponding to the specified `indices`,
        in the same order as they appear in `indices`.
        If `enumerate` is True, yields a tuple of the index and the element.

    Raises:
        StopIteration: If an index exceeds the length of the iterable.

    Notes:
        - The `indices` must be in sorted (non-decreasing) order.
        - The function iterates through `iterable` sequentially and skips over elements
          until it reaches each specified index.
        - If an index is out of range, `StopIteration` will be implicitly raised.
        - The function assumes `iterable` is finite and can be iterated sequentially.

    Examples:
        >>> list(select_by_index("abcdef", [0, 2, 4]))
        ['a', 'c', 'e']

        >>> list(select_by_index([10, 20, 30, 40, 50], [1, 3]))
        [20, 40]

        >>> list(select_by_index(range(10, 100, 10), [0, 5, 7]))
        [10, 60, 80]
    """
    item_iter = iter(iterable)
    index_iter = iter(indices)
    i_next = 0
    for i_wanted in index_iter:
        while i_next < i_wanted:
            next(item_iter)
            i_next += 1
        if enumerate:
            yield i_next, next(item_iter)
        else:
            yield next(item_iter)
        i_next += 1


class RecallableIterable(Iterable):
    """Allows a dynamic iterable to be iterated multiple times by caching its outputs.

    Args:
        iterable (Iterable): The input iterable to make recallable.

    Examples:
        >>> def dynamic_iterable():
        ...     for i in range(3):
        ...         yield i
        ...
        >>> recallable_iter = RecallableIterable(dynamic_iterable())
        >>> for i in recallable_iter:
        ...     print(i)
        ...     break
        0
        >>> for i in recallable_iter:
        ...     print(i)
        0
        1
        2
        3
    """

    def __init__(self, iterable: Iterable):
        self.cached_items = []
        self.inner_iterator = iter(iterable)

    def _advance(self):
        result = next(self.inner_iterator)
        self.cached_items.append(result)
        return result

    def __iter__(self):
        return self._Iterator(self)

    class _Iterator:
        def __init__(self, recall_iter: 'RecallableIterable'):
            self.recallable_iter = recall_iter
            self.next_index = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.next_index < len(self.recallable_iter.cached_items):
                result = self.recallable_iter.cached_items[self.next_index]
                self.next_index += 1
                return result
            else:
                self.next_index += 1
                return self.recallable_iter._advance()


class SlicableForwardSlice:
    def __init__(self, start=None, stop=None, step=None):
        self.start = start if start is not None else 0
        self.stop = stop
        self.step = step if step is not None else 1

    def __getitem__(self, slc):
        if not isinstance(slc, slice):
            raise TypeError("Only slice objects are supported.")

        slc_step = slc.step if slc.step is not None else 1
        slc_start = slc.start if slc.start is not None else 0

        if slc_start < 0 or (slc.stop is not None and slc.stop < 0) or slc_step < 0:
            raise ValueError(
                "Negative values are not supported for slicing a SlicableForwardSlice."
            )

        new_start = self.start + slc_start * self.step

        if slc.stop is not None:
            new_stop = self.start + slc.stop * self.step
            if self.stop is not None:
                new_stop = min(new_stop, self.stop)
        else:
            new_stop = self.stop

        new_step = self.step * slc_step
        return SlicableForwardSlice(new_start, new_stop, new_step)

    def to_slice(self):
        """Convert back to a standard slice object."""
        return slice(self.start, self.stop, self.step)

    def apply(self, iterable):
        """Apply the stored slice to an iterable."""
        if self.start == 0 and self.stop is None and self.step == 1:
            return iterable
        return itertools.islice(iterable, self.start, self.stop, self.step)

    def __repr__(self):
        return f"SlicableForwardSlice({self.start}, {self.stop}, {self.step})"
