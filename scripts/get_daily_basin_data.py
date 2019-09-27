from get_all_streamflow_data import get_all_streamflow_data_for_huc2
get_all_streamflow_data_for_huc2('02', "E:\\data\\streamflow_data\\"
                                 "discharge_data_02_daily",
                                 num_sites_per_chunk=10, product='dv',
                                 time_scale='D')
