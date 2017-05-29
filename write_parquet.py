import glob, h5py

import dask.dataframe as dd

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

keys = {
        "condensed": 0,
        "condensed_edge": 1,
        "condensed_env": 2,
        "condensed_shell": 3,
        "core": 4,
        "core_edge": 5,
        "core_env": 6,
        "core_shell": 7,
        "plume": 8,
        }

def write_parquet():
    _i = int

    def append_items(type, obj):
        for index in obj:
            rec['cid'].append(_i(obj.name.split('/')[1]))
            rec['type'].append(keys[type])
            rec['coord'].append(index)

    rec = {'cid': [], 'type': [], 'coord': []}
    filelist = sorted(glob.glob('/newtera/loh/data/BOMEX/tracking/clouds_*.h5'))
    for time, file in enumerate(filelist):
        with h5py.File(file) as h5_file:
            for cid in h5_file.keys():
                if _i(cid) == -1: continue # Ignore noise
                h5_file[cid].visititems(append_items)
        break

    df = pd.DataFrame.from_dict(rec)
    # pq.write_table(pa.Table.from_pandas(df), 'clouds.pq')

if __name__ == '__main__':
    write_parquet()
