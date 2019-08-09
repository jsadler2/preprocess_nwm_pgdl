# coding: utf-8
import requests
import json
import ulmo

def get_sites_with_param(site_list, param_code):
    # do it in chunks so that the urls don't get too long
    chunk_size = 10
    n_chunk = int(len(site_list)/chunk_size) + 1
    sites_with_parameter = []
    for i in range(n_chunk):
        sites = ",".join(site_list[i:i*n_chunk+1])
        print (sites)
        print (i)
        url_base = "https://waterservices.usgs.gov/nwis/site/?format=mapper&sites={}&parameterCd={}&siteStatus=all"
        url = url_base.format(sites, param_code)
        print (url)
        r = requests.get(url)
        sites_with_parameter.append(r.content)
    return sites_with_parameter
         


def get_sites_in_basin(huc):
    url = "https://cida.usgs.gov/nldi/huc12pp/{}/navigate/UT/nwissite".format(huc)
    r = requests.get(url)
    json_content = json.loads(r.content)

    sites = json_content['features']

    site_nums = []
    for s in sites:
        site_id = s['properties']['identifier']
        # is returned as USGS-[site_num]
        id_num = site_id.split('-')[1]
        site_nums.append(id_num)
    return site_nums

# huc = "020402060105"
# site_list = get_sites_in_basin(huc)

site_list = ['01445000']
parameter_code = "00060"
s = get_sites_with_param(site_list, parameter_code)
