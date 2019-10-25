"""
Get the nhd comid's that correspond to nwis site codes
"""

import pandas as pd


def get_comids_for_all_nwis_nhd(outfile, sites_file_name, nhd_file_name):
    # read in all_nhd "gage" data and make sure the site code column is read in
    # as a string
    nhd_site_code_col_name = 'SOURCE_FEA'
    comid_col_name = 'FLComID'
    nhd_df = pd.read_csv(nhd_file_name, dtype={nhd_site_code_col_name:str,
                                               comid_col_name:str})

    # set the site code as the index so we can select by it using site codes
    # from the nwis data
    nhd_df.set_index(nhd_site_code_col_name, inplace=True)

    nwis_sites_df = pd.read_csv(sites_file_name, dtype=str, header=None)

    # select the nhd data that corresponds to the nwis sites
    nhd_df_discharge = nhd_df.loc[nwis_sites_df[0]]

    # rename the index
    nhd_df_discharge.index.name = 'nwis_site_code'

    # get just the comid column, rename, and save
    just_comids = nhd_df_discharge[comid_col_name]
    just_comids.name = 'comid'
    just_comids = just_comids[~just_comids.isna()]
    just_comids.to_csv(outfile, index=True, header=True)

