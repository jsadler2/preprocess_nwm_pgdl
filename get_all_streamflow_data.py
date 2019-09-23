import pandas as pd
import xarray as xr
import numpy as np
import json
import requests
import datetime


def get_sites_for_huc2(huc2):
    df = pd.read_csv('all_streamflow_sites_CONUS.csv', dtype={'huc': str, 'code':str})
    df_for_huc2 = df[df['huc'].str.startswith(huc2)]
    sites_for_huc2 = df_for_huc2['code']
    return sites_for_huc2.to_list()

def get_all_streamflow_data_for_huc2(huc2, num_chunks=10, start_date_all="1900-01-01", end_date_all='2019-09-05'):
    date_chunks = pd.date_range(start_date_all, end_date_all, periods=num_chunks)
    huc_combined = []
    for i in range(len(date_chunks) - 1):
        start_date = date_chunks[i].date()
        end_date = date_chunks[i + 1].date()
        streamflow_data_chunk = get_streamflow_data(huc2, start_date, end_date)
        huc_combined.append(streamflow_data_chunk)
    all_huc_df = pd.concat(huc_combined, axis=0)
    all_huc_df = all_huc_df.replace(-999999, np.nan)
    return all_huc_df


def get_streamflow_data(huc2, start_date, end_date):
    response = call_nwis_service(huc2, start_date, end_date)
    data = json.loads(response.text)
    streamflow_df = nwis_json_to_df(data)
    return streamflow_df


def call_nwis_service(huc2, start_date, end_date):
    baseurl = "http://waterservices.usgs.gov/nwis/iv/?format=json&huc={}&startDT={}&endDT={}&parameterCd=00060&siteStatus=all"
    url = baseurl.format(huc2, start_date, end_date)
    request_start_time = datetime.datetime.now()
    print(f"starting request for huc {huc2} at {request_start_time}, for period {start_date} to {end_date}")
    r = requests.get(url)
    request_end_time = datetime.datetime.now()
    request_time = request_end_time - request_start_time
    print(f"took {request_time} to get data for huc {huc2}")
    return r


def nwis_json_to_df(json_data):
    df_collection = []
    time_series = json_data['value']['timeSeries']
    for ts in time_series:
        site_code = ts['sourceInfo']['siteCode'][0]['value']
        print('processing the data for site ', site_code)
        # this is where the actual data is
        ts_data = ts['values'][0]['value']
        if ts_data:
            ts_df = pd.DataFrame(ts_data)
            ts_df.index = pd.to_datetime(ts_df['dateTime'], utc=True)
            del ts_df['dateTime']
            del ts_df['qualifiers']
            # rename the column from 'value' to the site_code
            ts_df = ts_df.rename(columns={'value': site_code})
            ts_df[site_code] = pd.to_numeric(ts_df[site_code])
            ts_df = ts_df.resample('15T').mean()
            df_collection.append(ts_df)
    if df_collection:
        return pd.concat(df_collection)
    else:
        return None


hucs = [f'{h:02}' for h in range(1, 19)]
i = 0
times = []
data = []
# for huc in hucs:
        # xr = all_huc_df.to_xarray()
        # xr.to_zarr(f'{huc}_streamflow')

sites = get_sites_for_huc2('01')
# zarr_file = "E:\\data\\streamflow_data\\zarr_huc1"
# all_huc_df = get_all_streamflow_data_for_huc2(hucs[0], 2, "1990-01-07", "1990-01-08")
# d_array = xr.DataArray(all_huc_df.values, coords=[('datetime', all_huc_df.index), ('site_code', all_huc_df.columns)])
# ds = xr.Dataset({'streamflow': d_array})
# ds.to_zarr(zarr_file)
#
# all_huc_df2 = get_all_streamflow_data_for_huc2(hucs[0], 2, "1990-01-09", "1990-01-11")
# d_array2 = xr.DataArray(all_huc_df2.values, coords=[('datetime', all_huc_df2.index), ('site_code', all_huc_df2.columns)])
# ds2 = xr.Dataset({'streamflow': d_array2})
# ds2.to_zarr(zarr_file, mode='a', append_dim='datetime')
# ds2 = xr.Dataset({'streamflow':all_huc_df2.values}, coords={'datetime': all_huc_df2.index,
#                                                            'site_code':all_huc_df2.columns})
# print ('jjk')
