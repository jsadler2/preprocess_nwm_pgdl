import matplotlib.pyplot as plt
import pandas as pd
from utils import get_sites_with_param
import ulmo

def get_all_streamflow_sites():
    hucs = [f'{h:02}' for h in range(1, 19)]
    print ( hucs )
    d = []
    for huc in hucs:
        sites_with_param = ulmo.usgs.nwis.get_sites(huc=huc, service='iv', parameter_code="00060")
        # sites = get_sites_with_param(huc, param="00060")
        df = pd.DataFrame(sites_with_param)
        d.append(df.T)

    d_combined = pd.concat(d)
    lats_lons = d_combined['location'].apply(pd.Series)
    d_combined = pd.concat([d_combined, lats_lons], axis=1)
    del d_combined['location']

    d_combined['latitude'] = pd.to_numeric(d_combined['latitude'])
    d_combined['longitude'] = pd.to_numeric(d_combined['longitude'])

    d_combined.to_csv('all_streamflow_sites_CONUS.csv')


def merge_fips(df):
    df['state_code'] = pd.to_numeric(df['state_code'])

    df_fips = pd.read_csv('fips_state_codes.csv')
    df = df.merge(df_fips, left_on='state_code', right_on='code')
    return df

df = pd.read('all_streamflow_sites_CONUS.csv')
# d_combined.plot.scatter('longitude', 'latitude')
# plt.show()
