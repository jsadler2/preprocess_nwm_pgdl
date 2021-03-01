from get_nwis_sites import get_sites_all_hucs


if __name__ == '__main__':
    get_sites_all_hucs('iv', '00060', '../data/all_streamflow_sites_CONUS_iv1.csv')
    get_sites_all_hucs('dv', '00060', '../data/all_streamflow_sites_CONUS_dv1.csv')
