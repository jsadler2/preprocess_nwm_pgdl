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

base_url = "https://cida.usgs.gov/nldi/"


def json_from_nldi_request(url):
    r = requests.get(url)
    json_content = json.loads(r.content)
    return json_content


def get_nldi_huc_data(huc):
    url = base_url + "huc12pp/{}/navigate/UT/nwissite".format(huc)
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


def get_sites_done_zarr(output_file, dim_name):
    ds = xr.open_zarr(output_zarr)
    site_codes = ds[zarr_dim_name]
    return site_codes


def get_sites_done_csv(output_file, dim_name):
    with open(output_file, 'r') as f:
        df = pd.read_csv(output_file)
        return df[dim_name]


def get_sites_not_done(output_file, all_sites, dim_name, file_type):
    # check if zarr dataset exists
    if os.path.exists(output_file):
        # get the sites that are done
        if file_type=='zarr':
            sites_done = get_sites_done_zarr(output_file, dim_name)
        elif file_type=='csv':
            sites_done = get_sites_done_csv(output_file, dim_name)
        # get their indices
        is_done_idx = np.isin(all_sites, sites_done)
        all_array = np.array(all_sites)
        return all_array[~is_done_idx]
    else:
        return all_sites

  

