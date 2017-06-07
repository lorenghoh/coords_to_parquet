import glob
import numpy as np

# Zarr has pretty weak support outside simple slicing, so
# we will need to load the dataset into a numpy array to 
# begin with

# June 6 update: support for periodic boundary condition
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

    # For clouds that cross the (periodic) boundaries
    arr = np.zeros((11, 11), dtype=bool)

    # Diamond-shaped cloud going across y-axis
    x = np.array([1, 2, 3, 4, 5, 2, 3, 4, 3, 3, 2, 3, 4])
    y = np.array([0, 0, 0, 0, 0, 1, 1, 1, 2, 9, 10, 10, 10])
    arr[y, x] = True # Target array
    print(arr)

    x_axis, y_axis = arr.shape
    if (np.max(x) - np.min(x)) > x_axis // 2:
        
        # Shift target array
        x_off = x_axis - np.min(x[(x > x_axis // 2)])
        arr_r = np.roll(arr, x_off, axis=1)

        # Shift x-coordinates
        x_r = x + x_off
        x_r[x_r > x_axis - 1] = x_r[x_r > x_axis - 1] - x_axis - 1
    if (np.max(y) - np.min(y)) > y_axis // 2:
        # Shift target array
        y_off = y_axis - np.min(y[(y > y_axis // 2)])
        arr_r = np.roll(arr, y_off, axis=0)
        
        # Shift y-coordinates
        y_r = y + y_off
        y_r[y_r > y_axis - 1] = y_r[y_r > y_axis - 1] - y_axis - 1

    print(arr_r)

if __name__ == '__main__':
    main()