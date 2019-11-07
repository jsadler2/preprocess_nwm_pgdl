"""
This module contains utility functions for getting data for hucs that may be
used in multiple, more specific applications
"""
import datetime
import json
import os
import s3fs
import boto3
import botocore

import numpy as np
import pandas as pd
import requests
import xarray as xr

base_nldi_url = 'https://labs.waterdata.usgs.gov/api/nldi'
hucs = [f'{h:02}' for h in range(1, 19)]


def get_site_codes(sites_file, huc2=None):
    """
    get the site codes for a given huc2, if provided. if a huc2 is not provided,
    get site codes for all huc2's
    :param sites_file: [str] path to file where the site information is stored.
    this file can be generated by running the get_all_streamflow_sites method
    in the get_all_streamflow_sites file
    :param huc2: [str] (optional) the huc2 for which you want the nwis site
    codes
    :return:
    """
    site_df = pd.read_csv(sites_file, dtype={'huc_cd': str, 'site_no': str})
    site_codes = site_df['site_no']
    if huc2:
        site_codes_for_huc2 = site_codes[site_df['huc_cd'].str.startswith(huc2)]
        return site_codes_for_huc2.to_list()
    else:
        return site_codes.to_list()


def generate_nldi_url(category, identifier, service=None):
    """

    :param category: "comid" or "nwis"
    :param identifier: comid or site_code
    :param service: "tot" or "local"
    :return:url
    """
    if category == 'nwis':
        category = 'nwissite'
        identifier = 'USGS-' + identifier
    url = '{}/linked-data/{}/{}'
    url = url.format(base_nldi_url, category, identifier)
    if service:
        url += f'/{service}'
    return url


def json_from_nldi_request(url):
    r = requests.get(url)
    json_content = json.loads(r.content)
    return json_content


def get_nldi_huc_data(huc):
    url = base_nldi_url + "huc12pp/{}/navigate/UT/nwissite".format(huc)
    return json_from_nldi_request(url)


def get_sites_in_basin(huc):
    data = get_nldi_huc_data(huc)
    sites = data['features']
    site_nums = []
    for site in sites:
        site_id = site['properties']['identifier']
        # is returned as USGS-[site_num]
        id_num = site_id.split('-')[1]
        site_nums.append(id_num)
    return site_nums, data


def divide_chunks(l, n):
    # looping till length l 
    for i in range(0, len(l), n):
        yield l[i:i + n]


def load_s3_zarr_store(s3_zarr_path):
    fs = s3fs.S3FileSystem()
    zarr_store = s3fs.S3Map(s3_zarr_path, s3=fs)
    return zarr_store


def get_indices_done_zarr(output_zarr, zarr_dim_name, s3=False):
    if s3:
        output_zarr = load_s3_zarr_store(output_zarr)
    ds = xr.open_zarr(output_zarr)
    indices = ds[zarr_dim_name]
    return indices


def get_indices_done_csv(output_file, dim_name, is_column):
    """
    get the indices that are done in the data pulling
    :param output_file: the file that the data will be stored in
    :param dim_name: the dimension name if it's a column
    :param is_column: whether the index is stored in a column. otherwise it is
    assumed that the index is the column names themselves (i.e., you are
    appending to the file columnwise)
    :return: a array-like containing the indices that have already been pulled
    """
    with open(output_file, 'r') as f:
        df = pd.read_csv(output_file, dtype={dim_name: str})
        if is_column:
            return df[dim_name]
        else:
            return df.columns


def check_if_s3_resource_exists(s3_path):
    path_split = s3_path.split("/")
    s3 = boto3.resource('s3')
    try:
        s3.Object(*path_split).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise ValueError('something went wrong when checking if s3 object\
                    exists')
    else:
        return True


def check_if_exists(path, s3=False):
    if s3:
        return check_if_s3_resource_exists(path)
    else:
        return os.path.exists(output_file) 


def get_indices_not_done(output_file, all_indices, dim_name, file_type,
                         is_column=True, s3=False):
    # check if zarr dataset exists
    if check_if_exists(output_file, s3):
        # get the indices that are done
        if file_type == 'zarr':
            indices_done = get_indices_done_zarr(output_file, dim_name)
        elif file_type == 'csv':
            indices_done = get_indices_done_csv(output_file, dim_name,
                                                is_column)
        else:
            raise ValueError(f'Filetype {file_type} not valid.'
                             f'Should be zarr or csv')
        # get their indices
        is_done_idx = np.isin(all_indices, indices_done)
        all_array = np.array(all_indices)
        return all_array[~is_done_idx]
    else:
        return all_indices


def get_nldi_data_huc2(identifiers, out_file, get_one_func, identifier_name,
                       out_file_type='zarr'):
    not_done_identifiers = get_indices_not_done(out_file, identifiers,
                                                identifier_name, out_file_type)
    chunk_size = 40
    chunked_list = divide_chunks(not_done_identifiers, chunk_size)
    for chunk in chunked_list:
        df_list = []
        for identifier in chunk:
            try:
                print(f"trying to get data for {identifiers.index(identifier)}",
                      flush=True)
                single_df = None
                single_df = get_one_func(identifier)
                print(f"got data for {identifiers.index(identifier)}"
                      f" out of {len(identifiers)} {identifier_name}s",
                      flush=True)
                if single_df is not None:
                    df_list.append(single_df)
            except (requests.exceptions.ConnectionError,
                    json.decoder.JSONDecodeError):
                continue

        if df_list:
            df_combined = pd.concat(df_list)

            if out_file_type == 'zarr':
                append_to_zarr(df_combined, out_file, identifier_name)
            elif out_file_type == 'csv':
                append_to_csv_row_wise(df_combined, out_file)


def append_to_zarr(df, out_zarr_file, append_dim):
    ds = df.to_xarray()
    ds.to_zarr(out_zarr_file, mode='a', append_dim=append_dim)


def append_to_csv_row_wise(df, out_csv_file):
    file_exists = os.path.exists(out_csv_file)
    write_header = not file_exists
    if file_exists:
        # make sure the new data is matching the data in the csv
        columns = get_csv_columns(out_csv_file)
        df = df[columns]
    df.to_csv(out_csv_file, mode='a', header=write_header)


def append_to_csv_column_wise(df, out_csv_file):
    file_exists = os.path.exists(out_csv_file)
    if file_exists:
        # make sure the new data is matching the data in the csv
        df_old = pd.read_csv(out_csv_file)
        df_old[df.columns] = df[df.columns]
        df = df_old.copy()
    df.to_csv(out_csv_file, mode='w')


def get_csv_columns(out_csv_file):
    with open(out_csv_file, 'r') as f:
        first_line = f.readline()
    # returning all but the first column name since that is the index
    column_names = first_line.split(',')[1:]
    cleaned_names = [n.strip() for n in column_names]
    return cleaned_names


def write_indicator_file(func, file_name):
    with open(file_name, 'w') as f:
        f.write(f'successfully ran {func} \n')
        f.write(str(datetime.datetime.now()))


def make_nwis_sites_list(data_file, out_file):
    df = pd.read_parquet(data_file)
    with open(out_file, 'w') as f:
        f.write("\n".join(df.columns[1:].to_list()))


def read_nwis_sites_list():
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             'data', 'tables',
                                             'nwis_site_list_dv.csv'))
    data = pd.read_csv(data_path, header=None, dtype='str')
    return data


def read_nwis_comid():
    nwis_comid_file = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'data', 'tables',
        'nwis_comid.csv'))

    df = pd.read_csv(nwis_comid_file,
                     dtype={'nwis_site_code': str, 'comid':int})
    return df

