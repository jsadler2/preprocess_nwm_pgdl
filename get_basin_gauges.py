# coding: utf-8
import time
import requests
import json
import ulmo
import hydrofunctions as hf
from hydrofunctions.exceptions import HydroNoDataError

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

# get all sites for a HUC 12
huc = "020402060105"
start_time = time.time()
huc_site_list = get_sites_in_basin(huc)
end_time = time.time()
print ("time for getting site codes:", end_time - start_time)


parameter_code = "00060"
start_date = "2018-01-01"
end_date = "2019-01-10"
service = 'dv'

data_sites = []
start_time = time.time()
for site in huc_site_list:
    try:
        site_data = hf.NWIS(site, service, start_date, end_date,
                            parameterCd=parameter_code)
        site_data_df = site_data.get_data().df()
        data_sites.append(site_data)
    except HydroNoDataError:
        print ("no data for {}".format(site))
end_time = time.time()
print ("time for getting streamflow data:", end_time - start_time)

