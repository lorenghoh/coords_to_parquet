import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pdir

def index_to_zyx(index, nz, ny, nx):
    z = np.floor_divide(index, (ny * nx))
    xy = np.mod(index, (ny * nx))
    y = np.floor_divide(xy, nx)
    x = np.mod(xy, nx)
    return (z, y, x)

the_file='/home/phil/repos/pdf_to_mean_field/clouds_00000057.pq'
parquet_file=pq.ParquetFile(the_file)
print(parquet_file.metadata)
print(parquet_file.schema)
the_table=parquet_file.read_row_group(0)
print(pdir(the_table))
index_vals=the_table.column(3).to_pandas().to_xarray()
#
#  add x, y, z columns to pq file
#
