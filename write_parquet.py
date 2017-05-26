import h5py, glob, dask

import xarray as xr
import pandas as pd

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

timestep=re.compile('.*_(\d+)\.h5')
# the_file=glob.glob('*h5')[0]
the_file='/newtera/loh/data/BOMEX/tracking/clouds_00000057.h5'
the_time=timestep.match(the_file).group(1)
the_time=int(the_time)
print(the_time)
keep_recs=[]
with h5py.File(the_file) as h5file:
    for cloud_id in h5file.keys():
        for the_type in h5file[cloud_id]:
            type_num=keys[the_type]
            for coord in h5file[cloud_id][the_type]:
                keep_recs.append((int(cloud_id),type_num,the_time,coord))
df=pd.DataFrame.from_records(keep_recs,columns=['cloud_id','type','time_step','coord'])
table = pa.Table.from_pandas(df)
pq.write_table(table, 'clouds_00000057.pq')
