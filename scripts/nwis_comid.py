"""
Get the nhd comid's that correspond to nwis site codes
"""

import pandas as pd

from utils import get_sites_for_huc2, generate_nldi_url, \
    json_from_nldi_request, get_nldi_data_huc2, write_indicator_file


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


def get_comids_for_all_nwis(outfile, outfile_type, sites_file_name):
    # Warning NLDI only has the "iv" NWIS sites
    # get all sites numbers
    sites = get_sites_for_huc2(sites_file_name)
    print(outfile)
    get_nldi_data_huc2(sites, outfile, get_comid_for_one_nwis,
                       'nwis_site_code', outfile_type)


if __name__ == "__main__":
    out_file = snakemake.params.out_file
    site_file = snakemake.input[0]
    indicator_file = snakemake.output[0]
    get_comids_for_all_nwis(out_file, 'csv', site_file)
    write_indicator_file(get_comids_for_all_nwis, indicator_file)
