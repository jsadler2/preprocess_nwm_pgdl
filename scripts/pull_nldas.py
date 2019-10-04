from pydap.client import open_url
from pydap.cas.urs import setup_session
import xarray as xr
import time

username = 'jsadler'
password = 'rmX9YgFpxetSoyuxo1Dv'
all_variables = ['apcpsfc', 'cape180_0mb', 'convfracsfc', 'dlwrfsfc',
                 'dswrfsfc', 'pevapsfc', 'pressfc', 'spfh2m', 'tmp2m',
                 'ugrd10m', 'vgrd10m']


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


def nldas_to_zarr():
    dataset_url = contruct_nldas_url(end_time=960)
    print(dataset_url)
    session = setup_session(username, password, check_url=dataset_url)

    start_time = time.time()
    store = xr.backends.PydapDataStore.open(dataset_url, session=session)
    ds = xr.open_dataset(store).chunk({'lat':56, 'lon': 116, 'time': 480})
    ds.to_zarr('test_nldas', mode='w')
    end_time = time.time()

    print("time elapsed", (end_time - start_time))


if __name__ == '__main__':
    nldas_to_zarr()


