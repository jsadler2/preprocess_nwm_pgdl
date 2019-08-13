# coding: utf-8
import time
import requests
import json
import ulmo
import hydrofunctions as hf
from hydrofunctions.exceptions import HydroNoDataError

def get_json_site_param(huc, param):
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
    with open('stream_flow_sites.json', 'w') as f:
        json.dump(data, f)
    return data


def get_nldi_data(huc):
    url = "https://cida.usgs.gov/nldi/huc12pp/{}/navigate/UT/nwissite".format(huc)
    r = requests.get(url)
    json_content = json.loads(r.content)
    return json_content


def get_sites_in_basin(huc):
    data = get_nldi_data(huc)
    sites = data['features']
    site_nums = []
    for s in sites:
        site_id = s['properties']['identifier']
        # is returned as USGS-[site_num]
        id_num = site_id.split('-')[1]
        site_nums.append(id_num)
    return site_nums, data


def get_data_from_sites(sites, service, parameter_code, start_date, end_date):
    data_sites = []
    sites_with_param = []
    for site in huc_site_list:
        try:
            site_data = hf.NWIS(site, service, start_date, end_date,
                                parameterCd=parameter_code)
            site_data_df = site_data.get_data().df()
            data_sites.append(site_data_df)
            sites_with_param.append(site)
        except HydroNoDataError:
            print ("no data for {}".format(site))
    return data_sites



# get all sites for a HUC 12
huc = "020402060105"
# huc_site_list, data = get_sites_in_basin(huc)


parameter_code = "00060"
start_date = "2018-01-01"
end_date = "2019-01-10"
service = 'dv'
# data = get_data_from_sites(huc_site_list, service, start_date, end_date)

get_json_site_param(huc, parameter_code)



