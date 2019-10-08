from pydap.client import open_url
from pydap.cas.urs import setup_session
import pandas as pd
import xarray as xr
import time
from utils import get_indices_done_zarr, divide_chunks

all_variables = ['apcpsfc', 'cape180_0mb', 'convfracsfc', 'dlwrfsfc',
                 'dswrfsfc', 'pevapsfc', 'pressfc', 'spfh2m', 'tmp2m',
                 'ugrd10m', 'vgrd10m']

minimum_date = "1979-01-01 13:00"
base_url = 'https://hydro1.sci.gsfc.nasa.gov/dods/NLDAS_FORA0125_H.002?'


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


def nldas_to_zarr(zarr_store, urs_user, urs_pass, end_date="2019-01-01",
                  time_pull_size=10):
    session = setup_session(urs_pass, urs_user, check_url=base_url)

    start_request_time = time.time()
    store = xr.backends.PydapDataStore.open(base_url, session=session)
    ds = xr.open_dataset(store).chunk({'lat': 224, 'lon': 464,
                                       'time': 480})

    undone_range = get_undone_range(zarr_store, time_pull_size, end_date)
    j = 0
    for i in undone_range:
        if j < 3:
            start_date_num = i - 1
            end_date_num = i + time_pull_size - 1
            time_slice = slice(start_date_num, end_date_num, 1)
            ds_sliced = ds.isel(time=time_slice)
            ds_sliced.to_zarr(zarr_store, mode='a', append_dim='time')
            end_request_time = time.time()
            print("time elapsed", (end_request_time - start_request_time))
            j += 1


def get_urs_pass_user(netrc_file):
    """

    :param netrc_file: this is a path to a file that contains the urs username
    and password in the .netrc format
    :return: [tuple] (user_id, password)
    """
    with open(netrc_file, 'r') as f:
        text = f.read()
        words = text.split()

    # find urs.earthdata.nasa.gov
    url = 'urs.earthdata.nasa.gov'
    url_loc = words.index(url)
    user_name_loc = words[url_loc:].index('login') + 2
    user_name = words[user_name_loc]
    pword_loc = words[url_loc:].index('password') + 2
    pword = words[pword_loc]
    return user_name, pword


if __name__ == '__main__':
    netrc_file = 'C:\\Users\\jsadler\\.netrc'
    username, password = get_urs_pass_user(netrc_file)
    nldas_to_zarr('test_append_nldas', password, username, time_pull_size=10)


