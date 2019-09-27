from get_all_streamflow_data import get_all_streamflow_data_for_huc2
import datetime

hucs = [f'{h:02}' for h in range(1, 19)]
for huc in hucs:
    get_all_streamflow_data_for_huc2(huc, "E:\\data\\streamflow_data\\"
                                     f"discharge_data_{huc}_daily",
                                     num_sites_per_chunk=10, product='dv',
                                     time_scale='D')
    with open(f'../data/daily_discharge/indicator_{huc}', 'w') as f:
        f.write(f'successfully pulled daily discharge data for {huc} \n')
        f.write(str(datetime.datetime.now()))

