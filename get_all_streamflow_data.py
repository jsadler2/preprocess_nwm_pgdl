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
    """
    gets all streamflow data for all time for a given huc2. All of the stations
    in the HUC2 are taken at once, but the calls are chunked by date

    TODO: chunk by station too
    """
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
    streamflow_df = nwis_json_to_df(data, start_date, end_date)
    return streamflow_df


def call_nwis_service(huc2, start_date, end_date):
    """
    gets the data for all sites in a huc2 from a start data to an end date
    """
    baseurl = "http://waterservices.usgs.gov/nwis/iv/?format=json&huc={}&startDT={}&endDT={}&parameterCd=00060&siteStatus=all"
    url = baseurl.format(huc2, start_date, end_date)
    request_start_time = datetime.datetime.now()
    print(f"starting request for huc {huc2} at {request_start_time}, for period {start_date} to {end_date}")
    r = requests.get(url)
    request_end_time = datetime.datetime.now()
    request_time = request_end_time - request_start_time
    print(f"took {request_time} to get data for huc {huc2}")
    return r


def format_dates(datetime_txt):
    # convert datetime
    datetime_ser = pd.to_datetime(datetime_txt, utc=True)
    # remove  the time zone info since we are now in utc
    datetime_ser = datetime_ser.dt.tz_localize(None)
    return datetime_ser


def resample_reindex(df, start_date, end_date, time_scale):
    # resample to get mean at correct time scale 
    df_resamp = df.resample(time_scale).mean()

    # get new index
    date_index = pd.date_range(start=start_date, end=end_date,
                                  freq=time_scale)
    # make so the index goes from start to end regardless of actual data
    # presence
    df_reindexed = df_resamp.reindex(date_index)
    return df_reindexed


def delete_non_approved_data(df):
    """
    disregard the data that do not have the "approved" tag in the qualifier
    column
    :param df: dataframe with qualifiers
    :return: dataframe with just the values that are approved
    """
    # first I have to get the actual qualifiers. originally, these are lists
    # in a column in the df (e.g., [A, [91]]
    # todo: what does the number mean (i.e., [91])
    qualifier_df = pd.DataFrame(df['qualifiers'].to_list(),
                                columns=['qualifier_code', 'number'],
                                index=df.index)
    approved_indices = (qualifier_df['qualifier_code'] == 'A')
    approved_df = df[approved_indices]
    return approved_df


def format_df(ts_df, site_code, start_date, end_date, time_scale,
              only_approved=True):
    """
    format unformatted dataframe. this includes setting a datetime index,
    resampling, reindexing to the start and end date,
    renaming the column to the site code, removing the qualifier column and
    optionally screening out any data points that are not approved
    :param ts_df: (dataframe) unformatted time series dataframe from nwis json
    data
    :param site_code: (str) the site_code of the site (taken from json data)
    :param start_date: (str) start date of call
    :param end_date: (str) end date of call
    :param time_scale: (str) time scale in which you want to resample and at
    which your new index will be. should be a code (i.e., 'H' for hourly)
    :param only_approved: (bool) whether or not to screen out non-approved data
    points
    :return: formatted dataframe
    """
    # convert datetime
    ts_df['dateTime'] = format_dates(ts_df['dateTime'])
    ts_df.set_index('dateTime', inplace=True)

    # get rid of any points that were not approved
    ts_df = delete_non_approved_data(ts_df)
    # delete qualifiers column 
    del ts_df['qualifiers']
    # rename the column from 'value' to the site_code
    ts_df = ts_df.rename(columns={'value': site_code})
    # make the values numeric
    ts_df[site_code] = pd.to_numeric(ts_df[site_code])

    ts_df = resample_reindex(ts_df, start_date, end_date, time_scale)

    return ts_df


def nwis_json_to_df(json_data, start_date, end_date, time_scale='H'):
    """
    combine time series in json produced by nwis web from multiple sites into
    one pandas df. the df is also resampled to a time scale and reindexed so
    the dataframes are from the start date to the end date regardless of
    whether there is data available or not
    """
    df_collection = []
    time_series = json_data['value']['timeSeries']
    for ts in time_series:
        site_code = ts['sourceInfo']['siteCode'][0]['value']
        print('processing the data for site ', site_code)
        # this is where the actual data is
        ts_data = ts['values'][0]['value']
        if ts_data:
            ts_df = pd.DataFrame(ts_data)
            ts_df_formatted = format_df(ts_df, site_code, start_date, end_date, time_scale)
            df_collection.append(ts_df_formatted)
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
