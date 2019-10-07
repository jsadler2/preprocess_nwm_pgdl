from pydap.client import open_url
from pydap.cas.urs import setup_session
import pandas as pd
import xarray as xr
import time
from utils import get_indices_done_zarr

username = 'jsadler'
password = 'rmX9YgFpxetSoyuxo1Dv'
all_variables = ['apcpsfc', 'cape180_0mb', 'convfracsfc', 'dlwrfsfc',
                 'dswrfsfc', 'pevapsfc', 'pressfc', 'spfh2m', 'tmp2m',
                 'ugrd10m', 'vgrd10m']

minimum_date = "1979-01-01 13:00"

def contruct_nldas_url(start_long=1, end_long=463, start_lat=1,
                       end_lat=223, start_time=1, end_time=357168,
                       variables=all_variables):
    """
    max longitudinal n is 464
    max latitudinal n is 224
    """
    base_url = 'https://hydro1.sci.gsfc.nasa.gov/dods/NLDAS_FORA0125_H.002?'

    lon_range = f'[0:{start_long}:{end_long}]' 
    lons = f'lon{lon_range}'

    lat_range = f'[0:{start_lat}:{end_lat}]'
    lats = f'lat{lat_range}'
    
    time_range = f'[0:{start_time}:{end_time}]'
    times = f'time{time_range}' 

    variables_with_coords = [f'{v}{time_range}{lat_range}{lon_range}'
                             for v in variables]
    variables = ",".join(variables_with_coords)
    url = f'{base_url}{lons},{lats},{times},{variables}'
    return url


def max_num_dates_done(zarr_store):
    ds = xr.open_zarr(zarr_store)
    t = ds['time']
    return len(t)


def get_total_time_steps(end_date ='2019-01-01'):
    date_range = pd.date_range(start=minimum_date, end_date, freq='H')
    return(len(date_range))

def nldas_to_zarr(zarr_store, end_date, time_pull_size=960):
    num_total_dt_steps = range(get_total_time_steps)
    max_date_num = max_num_dates_done(zarr_store)


    dataset_url = contruct_nldas_url(end_time=960)
    session = setup_session(username, password, check_url=dataset_url)

    start_time = time.time()
    store = xr.backends.PydapDataStore.open(dataset_url, session=session)
    ds = xr.open_dataset(store).chunk({'lat':224, 'lon': 464, 'time': 481})
    ds.to_zarr('test_nldas', mode='w')
    end_time = time.time()

    print("time elapsed", (end_time - start_time))


if __name__ == '__main__':
    nldas_to_zarr()


