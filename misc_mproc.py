#########################################################################################
##
##  This module contains helper functions related multiprocessing.
##
## Author: Lucas Viani
## Date  : 28.11.2019
##
import ray
import numpy  as np
import pandas as pd
import multiprocessing as mp

NPROC = mp.cpu_count()-1 if mp.cpu_count() > 1 else 1

def apply_mt (func, df, axis):
    return df.apply(func, axis=axis)


def pd_multiply_mt (df, vals, axis=0):
    return df.mul(vals, axis=axis)


def parallelize_apply (data, func, nproc=NPROC, axis=0, axis_concat=None):
    '''Parallelize a func over the data provided.

    Example:
    def parallelize_on_rows(data, func, num_of_processes=8):
        return parallelize_split(data, partial(apply_mt, func), nproc=3, axis=1)
        
    Parameters
    ----------
    data : Numpy Array, Pandas DataFrame, etc
        The data to apply the function over.
    func : function
        The function to be executed.
    nrpoc : int 
        The number of processes to use.
    axis : int
        The axis to apply the function. 0 applies along the rows, 1 along the columns.
    axis_concat : int or str
        The axis value used in the pd.concat used to merge the results. If None, it is set as 'axis'.
        It may be used when splitting the calculation by columns (axis=1), but it is desired to concatenate by index (axis=1).
    '''
    # Handling the default values
    axis_concat = axis if axis_concat is None else axis_concat
    # Loading the shared object in Ray
    data_objId = ray.put(data)
    # Splitting the ROWs or COLUMNs
    if axis == 0: entry_split = np.array_split([c for c in range(0, data.shape[axis])], nproc)
    else:         entry_split = np.array_split(data.columns, nproc)
    # Executing the calculation
    futures     = [_func_ray.remote(entries, data_objId, func, axis) for entries in entry_split]    
    lst_res     = ray.get(futures)
    data        = pd.concat(lst_res, axis=axis_concat)

    # # Multiprocessing
    # # Splitting the data
    # data_split = np.array_split(data, nproc, axis=axis)
    # # Applying the function
    # pool = mp.Pool(nproc)
    # # Rebuilding the data
    # data = pd.concat(pool.map(func, data_split), axis=axis)
    # pool.close()
    # pool.join ()
    return data


@ray.remote
def _func_ray (lst_entries, data, func, axis):
    '''Remote function to be used with ray.

    Example:
    def parallelize_on_rows(data, func, num_of_processes=8):
        return parallelize_split(data, partial(apply_mt, func), nproc=3, axis=1)
        
    Parameters
    ----------
    data : Numpy Array, Pandas DataFrame, etc
        The data to apply the function over.
    '''
    # Rebuilding the data
    if axis == 0: return func(data.iloc[lst_entries])
    else:         return func(data[lst_entries])


def parallelize_calc (lst_split, func, nproc=NPROC):
    '''Parallelize a func over the data provided.

    Example:
    def parallelize_on_rows(data, func, num_of_processes=8):
        return parallelize_split(data, partial(apply_mt, func), nproc=3, axis=1)
        
    Parameters
    ----------
    lst_split : list
        List of items to pass to the function
    func : function
        The function to be executed.
    nrpoc : int 
        The number of processes to use.
    '''
    # Applying the function
    pool = mp.Pool(nproc)
    # Rebuilding the data
    data = pool.map(func, lst_split)
    pool.close()
    pool.join ()
    return data


def parallelize_calc_ray (lst_split, func, obj_to_share=None, obj_shared=None, func_args=None):
    '''Parallelize a func over the data provided.

    Example:
    def parallelize_on_rows(data, func, num_of_processes=8):
        return parallelize_split(data, partial(apply_mt, func), nproc=3, axis=1)
        
    Parameters
    ----------
    lst_split : list
        List of items to pass to the function
    func : function
        The function to be executed.
    obj_share : dict
        Dictionary of objects to be loaded as shared in Ray
    func_args : dict
        Dictionary with the extra function's parameters
    '''
    # Validating the default arguments...................................................
    obj_to_share = {} if obj_to_share is None else obj_to_share
    obj_shared   = {} if obj_shared   is None else obj_shared  
    func_args    = {} if func_args    is None else func_args   

    # Setting the shared objects.........................................................
    ray_shared = {}
    for lbl, obj in obj_to_share.items():
        ray_shared[lbl] = ray.put(obj)    

    # Calling the function remote........................................................    
    futures = [func.remote(block, **ray_shared, **obj_shared, **func_args) for block in lst_split]
    return ray.get(futures)