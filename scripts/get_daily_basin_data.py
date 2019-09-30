from streamflow_data_retrival import get_all_streamflow_data_for_huc2
import datetime
from utils import hucs

for huc in hucs:
    get_all_streamflow_data_for_huc2(huc, "E:\\data\\streamflow_data\\"
                                     f"discharge_data_{huc}_daily",
                                     num_sites_per_chunk=10, product='dv',
                                     time_scale='D')
    with open(f'../data/daily_discharge/indicator_{huc}.txt', 'w') as f:
        f.write(f'successfully pulled daily discharge data for {huc} \n')
        f.write(str(datetime.datetime.now()))

