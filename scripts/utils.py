"""
This module contains utility functions for getting data for hucs that may be
used in multiple, more specific applications
"""

import json
import requests
import ulmo

base_url = "https://cida.usgs.gov/nldi/"

def get_sites_with_param(huc, param=None):
    """
    Retrieves sites within a 12 digit huc from nwis that have record a given 
    param
    """

    # get all the sites in the huc
    sites_in_huc, data = get_sites_in_basin(huc)

    if param:
        # get all the sites in the huc that have param
        sites_with_param = ulmo.usgs.nwis.get_sites(sites=sites_in_huc,
                                                    parameter_code=param)
        return list(sites_with_param.keys())
    else:
        return sites_in_huc


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
