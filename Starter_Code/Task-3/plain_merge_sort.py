"""
Plain merge sort algorithm 
"""
import heapq
from typing import List

def merge(sublists: List[list]) -> list:
    """
    Merge sorted sublists into a single sorted list.

    :param sublists: List of sorted lists
    :return: Merged result
    """
    result = []
    sublists = [sublist for sublist in sublists if len(sublist)> 0]
    heap = [(sublist[0], i, 0) for i, sublist in enumerate(sublists)]
    heapq.heapify(heap)
    while len(heap):
        val, i, list_ind = heapq.heappop(heap)
        result.append(val)
        if list_ind+1 < len(sublists[i]):
            heapq.heappush(heap, (sublists[i][list_ind+1], i, list_ind+1))
    return result

def plain_merge_sort(collection: list, npartitions: int = 4) -> list:
    """
    Sorts a list using the merge sort algorithm. Breaks the list into multiple partitions.

    :param collection: A mutable ordered collection with comparable items.
    :return: The same collection ordered in ascending order.

    Time Complexity: O(n log n)

    Examples:
    >>> merge_sort([0, 5, 3, 2, 2])
    [0, 2, 2, 3, 5]
    >>> merge_sort([])
    []
    >>> merge_sort([-2, -5, -45])
    [-45, -5, -2]

    Modified from: https://github.com/TheAlgorithms/Python/
    """

    if len(collection) < npartitions:
        return sorted(collection)
    breaks = [i*len(collection)//npartitions for i in range(npartitions)]
    breaks.append(len(collection))
    sublists = [collection[breaks[i]:breaks[i+1]] for i in range(len(breaks)-1)]
    sorted_sublists = [plain_merge_sort(sublist, npartitions=2) for sublist in sublists] # just use 2 partitions in recursive calls
    return merge(sorted_sublists)