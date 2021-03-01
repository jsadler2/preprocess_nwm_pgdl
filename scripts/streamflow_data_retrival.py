import datetime
import json

import numpy as np
import pandas as pd
import requests
import xarray as xr

from utils import divide_chunks, get_indices_not_done, \
    get_site_codes, append_to_csv_column_wise, load_s3_zarr_store,\
    convert_df_to_dataset


def get_all_streamflow_data(output_file, sites_file, huc2=None,
                            num_sites_per_chunk=5, start_date="1970-01-01",
                            end_date='2019-01-01', time_scale='H',
                            output_format='zarr', num_site_chunks_write=6,
                            s3=False):
    """
    gets all streamflow data for a date range for a given huc2. Calls are
    chunked by station

    :param output_file: [str] path to the csv file or zarr store where the data
    will be stored
    :param sites_file: [str] path to file that contains the nwis site
    information
    :param huc2: [str] zero-padded huc 2 (e.g., "02")
    :param num_sites_per_chunk: [int] the number of sites that will be pulled
    at in each web service call
    :param start_date: [str] the start date of when you want the data for
    (e.g., "1980-01-01")
    :param end_date: [str] the end date of when you want the data for
    (e.g., "1990-01-01")
    :param time_scale: [str] Pandas like time string for the time scale at which
    the data will be aggregated (e.g., 'H' for hour or 'D' for daily)
    :param output_format: [str] the format of the output file. 'csv' or 'zarr'
    :param num_site_chunks_write:
    :param S3:
    :return: None
    """
    product = get_product_from_time_scale(time_scale)
    site_codes = get_site_codes(sites_file, huc2)

    not_done_sites = get_indices_not_done(output_file, site_codes, 'site_code',
                                          output_format, is_column=False,
                                          s3=s3)
    site_codes_chunked = divide_chunks(not_done_sites, num_sites_per_chunk)

    # loop through site_code_chunks
    chunk_dfs = []
    i = 0
    for site_chunk in site_codes_chunked:
        last_chunk = False
        if site_chunk[-1] == not_done_sites[-1]:
            last_chunk = True
        streamflow_df_sites = None
        # catch if there is a problem on the server retrieving the data
        try:
            streamflow_df_sites = get_streamflow_data(site_chunk,
                                                      start_date,
                                                      end_date,
                                                      product,
                                                      time_scale)
        except json.decoder.JSONDecodeError:
            continue
        if streamflow_df_sites is not None:
            chunk_dfs.append(streamflow_df_sites)
            # add the number of stations for which we got data
            i += streamflow_df_sites.shape[1]

            if not i % (num_site_chunks_write * num_sites_per_chunk) or \
                    last_chunk:
                print('writing out', flush=True)
                write_out_chunks(chunk_dfs, output_file, output_format)
                chunk_dfs = []


def write_out_chunks(chunks_dfs, out_file, out_format):
    all_chunks_df = pd.concat(chunks_dfs, axis=1)

    # write the data out to the output file
    if out_format == 'zarr':
        zarr_store = load_s3_zarr_store(out_file)
        append_to_zarr(all_chunks_df, zarr_store)
    elif out_format == 'csv':
        append_to_csv_column_wise(all_chunks_df, out_file)
    else:
        raise ValueError("output_format should be 'csv' or 'zarr'")


def get_product_from_time_scale(time_scale):
    """
    get the the USGS nwis product that is appropriate for the time scale
    :param time_scale: str - Pandas like time string for the time scale at which
    the data will be aggregated (e.g., 'H' for hour or 'D' for daily)
    :return:
    """
    iv_scales = ['15T', 'T', 'H']
    dv_scale = ['D']
    if time_scale in iv_scales:
        return 'iv'
    elif time_scale in dv_scale:
        return 'dv'
    else:
        raise ValueError("time scale must be '15T', 'T', 'H', or 'D'")


def append_to_zarr(streamflow_df, output_zarr):
    # chunks
    time_chunk = len(streamflow_df.index)
    site_code_chunk = len(streamflow_df.columns)
    ds = convert_df_to_dataset(streamflow_df, 'site_code', 'datetime',
                               'streamflow', {'datetime': time_chunk,
                                              'site_code': site_code_chunk})
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
    r = None
    while not r:
        try:
            r = requests.get(url)
        except:
            print('there was some problem. trying again', flush=True)
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
