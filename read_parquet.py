import glob, json

import numpy as np
import numba as nb
import pandas as pd
import dask.dataframe as dd
import pyarrow.parquet as pq

from get_model_config import get_model_config

case_name, model_config = get_model_config('BOMEX')

nz = model_config['config']['nz']
ny = model_config['config']['ny']
nx = model_config['config']['nx']

# @nb.jit(nopython=True)
def index_to_zyx(index):
    z = index // (ny * nx)
    xy = index % (ny * nx)
    y = xy // nx
    x = xy % nx
    return pd.DataFrame({'z':z, 'y':y, 'x':x})

def read_parquet():
    # cols = ['cid', 'type', 'coord']
    cols = ['cloud_id', 'type', 'coord']
    input_file = '/scratchSSD/phil/tracking/clouds_00000057.pq'
    df = dd.read_parquet(input_file, columns=cols)
    df = df.merge(df.coord.map_partitions(index_to_zyx))

    print(df.compute())

if __name__ == '__main__':
    read_parquet()
