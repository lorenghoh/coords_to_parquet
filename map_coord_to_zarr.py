import glob
import numpy as np

# Zarr has pretty weak support outside simple slicing, so
# we will need to load the dataset into a numpy array to 
# begin with
def main():
    arr = np.random.rand(5, 5, 5)

    z = np.random.randint(5, size=10)
    y = np.random.randint(5, size=10)
    x = np.random.randint(5, size=10)

    print(arr) # Full array

    # Just taking the values for each zyx tuple
    ix = np.array((z, y, x))
    print(arr[tuple(ix)]) # Selected elements

    # If you need to keep the original dimension
    b_map = np.zeros_like(arr, dtype=bool)
    b_map[tuple(ix)] = True
    arr[~b_map] = 0
    print(arr) # Full masked array

if __name__ == '__main__':
    main()