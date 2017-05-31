import glob, json, dask

import numpy as np
import numba as nb
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd
import pyarrow.parquet as pq

from get_model_config import get_model_config

case_name, model_config = get_model_config('BOMEX')

nz = model_config['config']['nz']
ny = model_config['config']['ny']
nx = model_config['config']['nx']

def index_to_zyx(index):
    z = index // (ny * nx)
    xy = index % (ny * nx)
    y = xy // nx
    x = xy % nx
    return pd.DataFrame({'z':z, 'y':y, 'x':x})

def find_longest_lived_clouds():
    cols = ['cloud_id', 'type']
    pq_list = sorted(glob.glob('/scratchSSD/phil/tracking/clouds_*.pq'))

    df_lifetime = pd.DataFrame(0, index=[], columns=['tau'])
    for time, pq_file in enumerate(pq_list):
        df = dd.read_parquet(pq_file, index=None, columns=cols)

        uids = np.unique(df.cloud_id)
        for uid in uids:
            try:
                df_lifetime.loc[uid] += 1
            except:
                df_lifetime.loc[uid] = 1

    print(df_lifetime.sort_values('tau', ascending=False).head(n=15))

def pick_clouds():
    # cids = np.array([21026, 18295, 21270, 20998, 20542])
    cid = 21026 # The longest-living cloud in BOMEX
    cols = ['cloud_id', 'type', 'coord']
    pq_list = sorted(glob.glob('/scratchSSD/phil/tracking/clouds_*.pq'))
    out_path = '/scratchSSD/loh/tracking/'

    for time, pq_file in enumerate(pq_list):
        if time < 144: continue
        df = dd.read_parquet(pq_file, index=None, columns=cols) 
        
        if cid in df.cloud_id.compute():
            rec = (df.coord.loc[df.cloud_id == cid].map_partitions(index_to_zyx))
            rec = df.loc[df.cloud_id == cid].merge(rec)

            try:
                tab = pa.Table.from_pandas(rec.compute())
                pq.write_table(tab, '/scratchSSD/loh/tracking/clouds_%08d.pq' % time)
            except:
                pass
        else:
            print('cid not found at timestep:', time)


if __name__ == '__main__':
    # find_longest_lived_clouds()
    pick_clouds()
