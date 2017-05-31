import os, glob, ujson

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

def create_output_dir(o_path):
    try:
        os.makedirs(o_path, exist_ok=True)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def pick_clouds():
    cols = ['cloud_id', 'type', 'coord']
    pq_list = sorted(glob.glob('/scratchSSD/phil/tracking/clouds_*.pq'))
    out_path = '/scratchSSD/loh/tracking/'

    c_dict = ujson.load(open('unique_clouds.json'))
    for cid in c_dict:
        print('Writing Parquet files for cid:', cid)
        
        # Ensure output directory exists
        create_output_dir('/scratchSSD/loh/tracking/%s' % cid)

        start_t = c_dict[cid][0]
        final_t = c_dict[cid][-1] + 1

        for time in c_dict[cid]:
            pq_file = pq_list[time]

            df = dd.read_parquet(pq_file, index=None, columns=cols)
            if len(df.loc[df.cloud_id == int(cid)]) < 1: continue

            rec = (df.coord.loc[df.cloud_id == int(cid)].map_partitions(index_to_zyx))
            rec = df.loc[df.cloud_id == int(cid)].merge(rec)

            try:
                tab = pa.Table.from_pandas(rec.compute())
                pq.write_table(tab, '/scratchSSD/loh/tracking/%s/clouds_%08d.pq' % (cid, time))
            except:
                pass

if __name__ == '__main__':
    # find_longest_lived_clouds()
    pick_clouds()
