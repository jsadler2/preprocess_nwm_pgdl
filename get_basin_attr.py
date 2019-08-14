from utils import get_sites_with_param, base_url, json_from_nldi_request
import time


def get_one_site_attr(site_num, attr):
    # adding in while try/except this so if the server isn't reached the first
    # time, it will keep trying
    while True:
        try:
            url = base_url + "nwissite/USGS-{}/tot?characteristicId={}"
            url_site_attr = url.format(site_num, attr)
            print('getting data for {}'.format(site_num))
            st = time.time()
            data = json_from_nldi_request(url_site_attr)
            end = time.time()
            print ('elapsed time', end-st)
            attr_value = data['characteristics'][0]['characteristic_value']
            print('value is {}'.format(attr_value))
            return float(attr_value)
        except:
            continue


def get_sites_attr(sites, attr):
    # so someone can put in just one list
    if not isinstance(sites, list):
        sites = [sites]

    attr_dict = {}
    for site in sites:
        area = get_one_site_attr(site, attr)
        attr_dict[site] = area
    return attr_dict


def get_basin_areas(huc, param=None):
    sites_with_param = get_sites_with_param(huc, param)
    # print('got here')
    attr = "CAT_AREA_SQKM"
    return get_sites_attr(sites_with_param, attr)
