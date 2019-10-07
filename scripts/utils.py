"""
This module contains utility functions for getting data for hucs that may be
used in multiple, more specific applications
"""
import os
import json
import requests
import xarray as xr
import pandas as pd
import numpy as np

base_nldi_url = 'https://labs.waterdata.usgs.gov/api/nldi'
hucs = [f'{h:02}' for h in range(1, 19)]


def get_sites_for_huc2(huc2, product):
    df = pd.read_csv(f'../data/all_streamflow_sites_CONUS_{product}.csv',
                     dtype={'huc_cd': str, 'site_no': str})
    df_for_huc2 = df[df['huc_cd'].str.startswith(huc2)]
    sites_for_huc2 = df_for_huc2['site_no']
    return sites_for_huc2.to_list()


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


def get_indices_done_zarr(output_zarr, zarr_dim_name):
    ds = xr.open_zarr(output_zarr)
    indices = ds[zarr_dim_name]
    return indices


def get_indices_done_csv(output_file, dim_name):
    with open(output_file, 'r') as f:
        df = pd.read_csv(output_file, dtype={dim_name: str})
        return df[dim_name]


def get_indices_not_done(output_file, all_indices, dim_name, file_type):
    # check if zarr dataset exists
    if os.path.exists(output_file):
        # get the indices that are done
        if file_type=='zarr':
            indices_done = get_indices_done_zarr(output_file, dim_name)
        elif file_type=='csv':
            indices_done = get_indices_done_csv(output_file, dim_name)
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
    chunk_size = 20
    chunked_list = divide_chunks(not_done_identifiers, chunk_size)
    for chunk in chunked_list:
        df_list = []
        for identifier in chunk:
            try:
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

        df_combined = pd.concat(df_list)

        if out_file_type == 'zarr':
            append_to_zarr(df_combined, out_file, identifier_name)
        elif out_file_type == 'csv':
            append_to_csv(df_combined, out_file)


def append_to_zarr(df, out_zarr_file, append_dim):
    ds = df.to_xarray()
    ds.to_zarr(out_zarr_file, mode='a', append_dim=append_dim)


def append_to_csv(df, out_csv_file):
    file_exists = os.path.exists(out_csv_file)
    write_header = not file_exists
    if file_exists:
        # make sure the new data is matching the data in the csv
        columns = get_csv_columns(out_csv_file)
        df = df[columns]
    df.to_csv(out_csv_file, mode='a', header=write_header)


def get_csv_columns(out_csv_file):
    with open(out_csv_file, 'r') as f:
        first_line = f.readline()
    # returning all but the first column name since that is the index
    column_names = first_line.split(',')[1:]
    cleaned_names = [n.strip() for n in column_names]
    return cleaned_names



