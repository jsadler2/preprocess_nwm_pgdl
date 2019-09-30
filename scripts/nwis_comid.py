"""
Get the nhd comid's that correspond to nwis site codes
"""

import pandas as pd
from utils import get_sites_for_huc2, hucs, generate_nldi_url,\
        json_from_nldi_request, get_nldi_data_huc2


def comid_data_to_df(nwis_site, json_data):
    comid = json_data['features'][0]['properties']['comid']
    df = pd.DataFrame(data=[str(comid)], index=[str(nwis_site)],
                      columns=['comid'])
    df.index.name = 'nwis_site_code'
    return df


def get_comid_for_one_nwis(nwis_site):
    url = generate_nldi_url('nwis', nwis_site)
    json_data = json_from_nldi_request(url)
    df = comid_data_to_df(nwis_site, json_data)
    return df


def get_comids_for_nwis_huc2(huc2, outfile, outfile_type):
    # Warning NLDI only has the "iv" NWIS sites
    sites_iv = get_sites_for_huc2(hucs[1], 'iv')
    get_nldi_data_huc2(sites_iv, outfile, get_comid_for_one_nwis,
                       'nwis_site_code', outfile_type)


if __name__ == "__main__":
    huc = '02'
    get_comids_for_nwis_huc2(huc, f'E:\\data\\nwis_comid_{huc}.csv',
                             outfile_type='csv')
