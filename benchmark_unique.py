import numpy as np
import numba
import timeit

def benchmark(func):
    return min(timeit.Timer(lambda: func).repeat(10))

@numba.jit(nopython=True)
def _unique(arr):
    arr_l = len(arr)
    flags = np.zeros(arr_l, dtype=np.bool_)
    arr = np.sort(arr)

    flags[0] = True
    tmp = arr[0]
    for i in np.arange(1, arr_l):
        if arr[i] == tmp:
            flags[i] = False
        else:
            flags[i] = True
            tmp = arr[i]

    arr_o = np.zeros(np.sum(flags), dtype=np.int32)
    ix = 0
    for i in np.arange(arr_l):
        if flags[i]:
            arr_o[ix] = arr[i]
            ix += 1
    return arr_o

def main():
    arr = np.random.randint(5, size=400000)

    print("Numpy.unique:\t %.5f seconds, best of 10" % benchmark(np.unique(arr)))
    print("jit_unique:\t %.5f seconds, best of 10" % benchmark(_unique(arr)))

if __name__ == '__main__':
    main()