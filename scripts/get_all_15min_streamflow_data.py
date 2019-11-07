import s3fs
from streamflow_data_retrival import get_all_streamflow_data
from utils import write_indicator_file


my_bucket = 'ds-drb-data/'
file_name = f'{my_bucket}15min_discharge'

sites_file = '../data/tables/all_streamflow_sites_CONUS_iv.csv'
get_all_streamflow_data(file_name, sites_file, num_sites_per_chunk=1,
                        time_scale='15T', output_format='zarr',
                        start_date='1970-01-01', end_date='2019-03-01',
                        num_site_chunks_write=60, s3=True
                        )

write_indicator_file(get_all_streamflow_data,
                     file_name='../data/indicators/15min_discharge.txt')
