#!/usr/bin/env python
# coding: utf-8

# In[7]:


"""
Plain merge sort algorithm 
"""
import heapq
from typing import List

def merge0(sublists: List[list]) -> list:
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
    return merge0(sorted_sublists)


# In[8]:


import heapq
from typing import List
import ray
from ray import ObjectRef
# from plain_merge_sort import plain_merge_sort, merge  
import time
import numpy as np

# Initialize Ray.
num_workers = 4
ray.init(num_cpus=num_workers, ignore_reinit_error=True)


# In[9]:


def merge(sublists: List[list]) -> list:
    """
    Merge sorted sublists into a single sorted list.

    :param sublists: List of sorted lists
    :return: Merged result
    """
    ## YOU CAN MODIFY THIS WITH RAY
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

@ray.remote
def custom_sort_sublist(collection_ref: ObjectRef, start: int, end: int):
   
    # Returns a sorted sublist and also we are directly passing the object reference without passing the actual ray.get value
    return plain_merge_sort(collection_ref[start:end]) 


def merge_sort_ray(collection_ref: ObjectRef, length: int, npartitions: int = 4) -> list:
    """
    Merge sort with ray.
    """
    ## DO NOT MODIFY: START    
    breaks = [i*length//npartitions for i in range(npartitions)]
    breaks.append(length)
    # Keep track of partition end points
    sublist_end_points = [(breaks[i], breaks[i+1]) for i in range(len(breaks)-1)]
    ## DO NOT MODIFY: END
    
    # Sort each sublist in parallel by passing the object reference and the indices.
    sublist_sorted_refrence = [custom_sort_sublist.remote(collection_ref, start, end) for start, end in sublist_end_points]
    # Wait for all sorting tasks to complete and retrieve the results
    sublists_sorted_results = ray.get(sublist_sorted_refrence)
    
    # Merge the sorted sublists
    return merge(sublists_sorted_results)


# In[11]:


if __name__ == "__main__":
    # We will be testing your code for a list of size 10M. Feel free to edit this for debugging. 
#     list1 = list(np.random.randint(low=0, high=1000, size=10_000_000))
    list1 = list(np.random.randint(low=0, high=1000, size=10_000_000))
    list2 = [c for c in list1] # make a copy
    length = len(list2)
    list2_ref = ray.put(list2) # insert into the driver's object store
    
    start1 = time.time()
    list1 = plain_merge_sort(list1, npartitions=num_workers)
    end1 = time.time()
    time_baseline = end1 - start1
    print("Plain sorting:", time_baseline)

    start2 = time.time()
    list2 = merge_sort_ray(collection_ref=list2_ref, length=length, npartitions=num_workers)
    end2 = time.time()
    time_ray = end2 - start2
    print("Ray sorting:", time_ray)

    print("Speedup: ", time_baseline/ time_ray)
    ## You can uncomment and verify that this holds
    assert sorted(list1) == list2, "Sorted lists are not equal"
    


# In[ ]:





# In[ ]:




