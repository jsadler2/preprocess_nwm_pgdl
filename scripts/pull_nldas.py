from pydap.client import open_url
from pydap.cas.urs import setup_session
import pandas as pd
import xarray as xr
import time
from utils import get_indices_done_zarr, divide_chunks

username = 'jsadler'
password = 'rmX9YgFpxetSoyuxo1Dv'
all_variables = ['apcpsfc', 'cape180_0mb', 'convfracsfc', 'dlwrfsfc',
                 'dswrfsfc', 'pevapsfc', 'pressfc', 'spfh2m', 'tmp2m',
                 'ugrd10m', 'vgrd10m']

minimum_date = "1979-01-01 13:00"


def contruct_nldas_url(start_long=0, end_long=463, start_lat=0, end_lat=223,
                       start_time=0, end_time=357168, variables=all_variables):
    """
    max longitudinal n is 464
    max latitudinal n is 224
    """
    base_url = 'https://hydro1.sci.gsfc.nasa.gov/dods/NLDAS_FORA0125_H.002?'

    lon_range = f'[{start_long}:{end_long}]'
    lons = f'lon{lon_range}'

    lat_range = f'[{start_lat}:{end_lat}]'
    lats = f'lat{lat_range}'
    
    time_range = f'[{start_time}:{end_time}]'
    # time_range = f'[0:{end_time}]'
    times = f'time{time_range}'

    variables_with_coords = [f'{v}{time_range}{lat_range}{lon_range}'
                             for v in variables]
    variables = ",".join(variables_with_coords)
    url = f'{base_url}{lons},{lats},{times},{variables}'
    return url


def max_num_dates_done(zarr_store):
    try:
        ds = xr.open_zarr(zarr_store)
    except ValueError:
        return None
    t = ds['time']
    return len(t)


def get_total_time_steps(end_date):
    date_range = pd.date_range(start=minimum_date, end=end_date, freq='H')
    return len(date_range)


def get_undone_range(zarr_store, time_pull_size, end_date):
    num_total_dt_steps = get_total_time_steps(end_date)
    max_date_num = max_num_dates_done(zarr_store)
    if max_date_num:
        start = max_date_num
    else:
        start = 1
    return range(start, num_total_dt_steps, time_pull_size)


def nldas_to_zarr(zarr_store, end_date="2019-01-01", time_pull_size=10):
    undone_range = get_undone_range(zarr_store, time_pull_size, end_date)

    j = 0
    for i in undone_range:
        if j < 3:
            start_date_num = i - 1
            end_date_num = i + time_pull_size - 1
            # start_date_num = 11
            # end_date_num = 15
            dataset_url = contruct_nldas_url(end_time=end_date_num,
                                             start_time=start_date_num,
                                             variables=[all_variables[0]])
            print (dataset_url)
            session = setup_session(username, password, check_url=dataset_url)

            start_request_time = time.time()
            store = xr.backends.PydapDataStore.open(dataset_url,
                                                    session=session)
            ds = xr.open_dataset(store)
            ds.to_zarr(zarr_store, mode='a', append_dim='time')
            ds.close()
            end_request_time = time.time()

            print("time elapsed", (end_request_time - start_request_time))
            j += 1


if __name__ == '__main__':
    nldas_to_zarr('test_append_nldas')


