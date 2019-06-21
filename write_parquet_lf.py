import glob, h5py, ujson

import dask.dataframe as dd

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

from joblib import Parallel, delayed
from tqdm import tqdm

keys = {
        "condensed": 0,
        "core": 1,
        "plume": 2,
        }

with open('config.json') as json_dict:
    dirs = ujson.load(json_dict)

def write_parquet_lf(time, item):
    def visit_items(item):
        cid = item.name.split('/')[-1] # cid

        rec = []
        for key in keys:
            for coord in list(item[key][...]):
                rec.append((cid, keys[key], coord))
        return rec

    with h5py.File(item, 'r', driver='core') as h5_file:
        cids = list(h5_file.keys())[1:] # Ignore noise
        
        result = []
        for cid in tqdm(cids):
            result.extend(visit_items(h5_file[cid]))
    
    cols = ['cid', 'type', 'loc']
    df = pd.DataFrame(result, columns=cols)
    loc = dirs['output_dir']
    pq.write_table(
        pa.Table.from_pandas(df), 
        f'{loc}/clouds_{time:08d}.pq',
        use_dictionary=True
    )

if __name__ == '__main__':
    loc = dirs['input_dir']
    filelist = sorted(glob.glob(f'{loc}/clouds_*.h5'))

    for time, item in enumerate(tqdm(filelist)):
        write_parquet_lf(time, item)
