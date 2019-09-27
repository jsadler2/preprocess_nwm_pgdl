import pandas as pd
import json
import requests


def get_comids():
    df = pd.read_csv('comids_huc02.csv', dtype={'COMID': str})
    comids = df['COMID']
    comids_non_zero = comids[comids != '0']
    return comids_non_zero.to_list()


def generate_nldi_url(category, identifier, service):
    """

    :param category: "comid" or "nwis"
    :param identifier: comid or site_code
    :param service: "tot" or "local"
    :return:url
    """
    base_nldi_url = 'https://labs.waterdata.usgs.gov/api/' \
                    'nldi/linked-data/{}/{}/{}'
    return base_nldi_url.format(category, identifier, service)


comid_list = get_comids()

url = generate_nldi_url('comid', comid_list[50], 'tot')

response = requests.get(url)
data = json.loads(response.text)
