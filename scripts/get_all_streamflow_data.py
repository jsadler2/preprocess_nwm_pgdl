import os
import pandas as pd
import xarray as xr
import numpy as np
import json
import requests
import datetime


def get_sites_for_huc2(huc2):
    df = pd.read_csv('../data/all_streamflow_sites_CONUS.csv',
                     dtype={'huc': str, 'code': str})
    df_for_huc2 = df[df['huc'].str.startswith(huc2)]
    sites_for_huc2 = df_for_huc2['code']
    return sites_for_huc2.to_list()


def divide_chunks(l, n): 
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 
  

def get_all_streamflow_data_for_huc2(huc2, output_zarr, num_sites_per_chunk=5,
                                     start_date_all="1970-01-01",
                                     end_date_all='2019-01-01', product='iv',
                                     time_scale='H'):
    """
    gets all streamflow data for a date range for a given huc2. Calls are 
    chunked by station
    """
    site_codes_in_huc2 = get_sites_for_huc2(huc2)
    not_done_sites = get_sites_not_done(output_zarr, site_codes_in_huc2)
    site_codes_chunked = divide_chunks(not_done_sites, num_sites_per_chunk)

    # loop through site_code_chunks
    for site_chunk in site_codes_chunked:
        streamflow_df_sites = None
        try:
            streamflow_df_sites = get_streamflow_data(site_chunk,
                                                      start_date_all,
                                                      end_date_all,
                                                      product,
                                                      time_scale)
        except json.decoder.JSONDecodeError:
            continue
        if streamflow_df_sites is not None:
            append_to_zarr(streamflow_df_sites, output_zarr)


def get_sites_not_done(output_zarr, all_site_codes):
    # check if zarr dataset exists
    if os.path.exists(output_zarr):
        # read zarr
        ds = xr.open_zarr(output_zarr)
        site_codes = ds['site_code']
        is_done = np.isin(all_site_codes, site_codes)
        all_array = np.array(all_site_codes)
        return all_array[~is_done]
    else:
        return 0


def convert_df_to_dataset(streamflow_df):
    data_array = xr.DataArray(streamflow_df.values,
                              [('datetime', streamflow_df.index),
                               ('site_code', streamflow_df.columns)])
    data_set = xr.Dataset({'streamflow': data_array})
    return data_set


def append_to_zarr(streamflow_df, output_zarr):
    ds = convert_df_to_dataset(streamflow_df)
    ds.to_zarr(output_zarr, append_dim='site_code', mode='a')


def get_streamflow_data(sites, start_date, end_date, product, time_scale):
    response = call_nwis_service(sites, start_date, end_date, product)
    data = json.loads(response.text)
    streamflow_df = nwis_json_to_df(data, start_date, end_date,
                                    time_scale)
    return streamflow_df


def call_nwis_service(sites, start_date, end_date, product):
    """
    gets the data for a list of sites from a start date to an end date
    """
    base_url = "http://waterservices.usgs.gov/nwis/{}/?format=json&sites={}&" \
               "startDT={}&endDT={}&parameterCd=00060&siteStatus=all"
    url = base_url.format(product, ",".join(sites), start_date, end_date)
    request_start_time = datetime.datetime.now()
    print(f"starting request for sites {sites} at {request_start_time}, "
          f"for period {start_date} to {end_date}", flush=True)
    r = requests.get(url)
    request_end_time = datetime.datetime.now()
    request_time = request_end_time - request_start_time
    print(f"took {request_time} to get data for huc {sites}", flush=True)
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
    qualifiers_list = df['qualifiers'].to_list()
    qualifiers = [q[0] for q in qualifiers_list]
    # check qualifier's list
    if qualifiers[0] not in ['A', 'P']:
        print("we have a weird qualifier. it is ", qualifiers[0])
    qualifier_ser = pd.Series(qualifiers, index=df.index)
    approved_indices = (qualifier_ser == 'A')
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

    if only_approved:
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
        print('processing the data for site ', site_code, flush=True)
        # this is where the actual data is
        ts_data = ts['values'][0]['value']
        if ts_data:
            ts_df = pd.DataFrame(ts_data)
            ts_df_formatted = format_df(ts_df, site_code, start_date, end_date,
                                        time_scale)
            df_collection.append(ts_df_formatted)
    if df_collection:
        df_combined = pd.concat(df_collection, axis=1)
        df_combined = df_combined.replace(-999999, np.nan)
        return df_combined
    else:
        return None

