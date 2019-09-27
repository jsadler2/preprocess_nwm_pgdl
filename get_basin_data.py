# coding: utf-8
import pandas as pd
import json
import ulmo
import hydrofunctions as hf
from hydrofunctions.exceptions import HydroNoDataError
from utils import get_sites_in_basin


def get_json_site_param(huc, param, file_name=None):
    # get all the sites in the huc
    sites_in_huc, data = get_sites_in_basin(huc)

    # get all the sites in the huc that have param
    sites_with_param = ulmo.usgs.nwis.get_sites(sites=sites_in_huc,
                                                parameter_code=param)
    # get geojson just for sites with param
    sites_with_param_data = []
    for site in data['features']:
        site_id = site['properties']['identifier']
        # is returned as USGS-[site_num]
        id_num = site_id.split('-')[1]
        # check if id is in the list of sites with param
        if id_num in sites_with_param.keys():
            sites_with_param_data.append(site)
    data['features'] = sites_with_param_data
    if file_name:
        with open(file_name, 'w') as fl:
            json.dump(data, fl)
    return data


def get_data_from_sites(sites, service, parameter_code, start_date, end_date):
    data_sites = []
    sites_with_param = []
    for site in sites:
        try:
            site_data = hf.NWIS(site, service, start_date, end_date,
                                parameterCd=parameter_code)
            site_data_df = site_data.get_data().df()
            data_sites.append(site_data_df)
            sites_with_param.append(site)
            print('got data for {} ', site)
        except HydroNoDataError:
            print("no data for {}".format(site))
    data_from_sites_combined = pd.concat(data_sites, axis=1)
    return data_from_sites_combined


def get_data_for_huc(huc, param, start_date, end_date, service='dv'):
    huc_site_list, data = get_sites_in_basin(huc)
    site_data = get_data_from_sites(huc_site_list, service, param, start_date,
                                    end_date)
    return site_data


# get all sites for a HUC 12
# huc = "020402060105"
#
# parameter_code = "00060"
# start_date = "2018-01-01"
# end_date = "2019-01-10"
# service = 'dv'
