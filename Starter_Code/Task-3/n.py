import heapq
from typing import List
import ray
from ray import ObjectRef
from plain_merge_sort import plain_merge_sort
import time
import numpy as np

## RAY INIT. DO NOT MODIFY
num_workers = 4
ray.init(num_cpus=num_workers)
## END OF INIT

## Feel free to add your own functions here for usage with Ray
@ray.remote
def sort_sublist(sublist):
    """Sort a sublist. This function is intended to be executed as a Ray task."""
    return sorted(sublist)


def merge(sorted_sublists: List[List[int]]) -> List[int]:
    """Merge sorted sublists into a single sorted list using a heap."""
    merged_list = []
    heap = []

    # Initialize the heap
    for i, sublist in enumerate(sorted_sublists):
        if sublist:
            heapq.heappush(heap, (sublist[0], i, 0))

    # Merge the sublists
    while heap:
        val, list_idx, element_idx = heapq.heappop(heap)
        merged_list.append(val)
        if element_idx + 1 < len(sorted_sublists[list_idx]):
            next_element = sorted_sublists[list_idx][element_idx + 1]
            heapq.heappush(heap, (next_element, list_idx, element_idx + 1))

    return merged_list

def merge_sort_ray(collection_ref: ObjectRef, length: int, npartitions: int = 4) -> list:
    """
    Merge sort with ray
    """
    ## DO NOT MODIFY: START    
    breaks = [i*length//npartitions for i in range(npartitions)]
    breaks.append(length)
    # Keep track of partition end points
    sublist_end_points = [(breaks[i], breaks[i+1]) for i in range(len(breaks)-1)]
    ## DO NOT MODIFY: END

  # Sort sublists in parallel
    sorted_sublist_refs = [sort_sublist.remote(ray.get(collection_ref)[start:end]) for start, end in sublist_end_points]
    
    # Retrieve the sorted sublists
    sorted_sublists = ray.get(sorted_sublist_refs)
    
    # Merge the sorted sublists using a heap
    final_sorted_list = merge(sorted_sublists)
    return final_sorted_list

    
    ## PLEASE COMPLETE THIS ##
    # sorted_sublists = 
    ## END ##
    # Pass your list of sorted sublists to `merge`
    return merge(sorted_sublists)

if __name__ == "__main__":
    # We will be testing your code for a list of size 10M. Feel free to edit this for debugging. 
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
    # assert sorted(list1) == list2, "Sorted lists are not equal"
    




