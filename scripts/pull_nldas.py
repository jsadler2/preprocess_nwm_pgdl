from pydap.client import open_url
from pydap.cas.urs import setup_session
import pandas as pd
import xarray as xr
import time

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
                  time_pull_size=959, lat_chunk=224, lon_chunk=464,
                  time_chunk=480):
    """
    pull data from nldas and put into a zarr store for of CONUS
    :param zarr_store: [str] the path to the zarr store to which the data will
    be written
    :param urs_user: [str] the urs username
    :param urs_pass: [str] the urs password
    :param end_date: [str] date until which the data should be pulled
    :param time_pull_size: [int] the number of dates that should be pulled and
    written to zarr at a time
    :param lat_chunk: [int] the zarr chunk size for the lat dimension
    :param lon_chunk: [int] the zarr chunk size for the lon dimension
    :param time_chunk: [int] the zarr chunk size for the time dimension
    :return: None
    """
    session = setup_session(urs_pass, urs_user, check_url=base_url)

    store = xr.backends.PydapDataStore.open(base_url, session=session)
    chunks = {'lat': lat_chunk, 'lon': lon_chunk, 'time': time_chunk}
    ds = xr.open_dataset(store).chunk(chunks)

    undone_range = get_undone_range(zarr_store, time_pull_size, end_date)
    for i in undone_range:
        start_date_num = i - 1
        end_date_num = i + time_pull_size - 1
        time_slice = slice(start_date_num, end_date_num, 1)
        start_request_time = time.time()
        print(f"getting data for time {start_date_num} to {end_date_num}",
              flush=True)
        ds_sliced = ds.isel(time=time_slice)
        ds_sliced.to_zarr(zarr_store, mode='a', append_dim='time')
        end_request_time = time.time()
        print("time elapsed", (end_request_time - start_request_time),
              flush=True)


def get_urs_pass_user(netrc_file):
    """
    retrieve the urs password and username from a .netrc file
    :param netrc_file: this is a path to a file that contains the urs username
    and password in the .netrc format
    :return: [tuple] (user_name, password)
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
    netrc = 'C:\\Users\\jsadler\\.netrc'
    username, password = get_urs_pass_user(netrc)
    zarr_data_store = f'nldas'
    nldas_to_zarr(zarr_data_store, password, username, time_pull_size=479)


