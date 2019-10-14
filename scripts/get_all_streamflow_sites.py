import pandas as pd
import requests
import os


def convert_response_to_df(response_text):
    tmpfile = 'tmpout'
    with open(tmpfile, 'w') as f:
        f.write(response_text)
    df = pd.read_csv(tmpfile, sep='\t', skiprows=30)
    # remove row specifying data type
    df.drop([0], axis=0, inplace=True)
    os.remove(tmpfile)
    return df


def get_streamflow_for_one_huc(huc, product):
    base_url = 'https://waterservices.usgs.gov/nwis/site/?format=rdb&huc={}' \
          '&parameterCd=00060&siteStatus=all&hasDataTypeCd={}'
    url = base_url.format(huc, product)
    response = requests.get(url)
    data = convert_response_to_df(response.text)
    return data


def convert_lat_long_to_numeric(d_combined):
    lat_col_name = 'dec_lat_va'
    lon_col_name = 'dec_long_va'
    d_combined[lat_col_name] = pd.to_numeric(d_combined[lat_col_name])
    d_combined[lon_col_name] = pd.to_numeric(d_combined[lon_col_name])
    return d_combined


def get_all_streamflow_sites(product, folder):
    hucs = [f'{h:02}' for h in range(1, 19)]
    d = []
    for huc in hucs:
        site_df = get_streamflow_for_one_huc(huc, product)
        d.append(site_df)
    d_combined = pd.concat(d)
    d_combined = convert_lat_long_to_numeric(d_combined)
    d_combined.to_csv(f'{folder}/all_streamflow_sites_CONUS_{product}.csv')


def merge_fips(df):
    df['state_code'] = pd.to_numeric(df['state_code'])
    df_fips = pd.read_csv('fips_state_codes.csv')
    df = df.merge(df_fips, left_on='state_code', right_on='code')
    return df


if __name__ == '__main__':
    get_all_streamflow_sites('iv', 'data')
    get_all_streamflow_sites('dv', 'data')
