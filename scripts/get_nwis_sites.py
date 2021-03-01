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
    df = convert_lat_long_to_numeric(df)
    os.remove(tmpfile)
    return df


def convert_lat_long_to_numeric(d_combined):
    lat_col_name = 'dec_lat_va'
    lon_col_name = 'dec_long_va'
    d_combined[lat_col_name] = pd.to_numeric(d_combined[lat_col_name])
    d_combined[lon_col_name] = pd.to_numeric(d_combined[lon_col_name])
    return d_combined


def merge_fips(df):
    df['state_code'] = pd.to_numeric(df['state_code'])
    df_fips = pd.read_csv('fips_state_codes.csv')
    df = df.merge(df_fips, left_on='state_code', right_on='code')
    return df


def get_sites_for_one_huc(huc, product, param_cd, out_file=None):
    """
    get the sites for a given huc2 and parameter code and and optionally write
    them to a csv file
    :param huc:[str] the 2-digit huc that you want the sites for
    :param product: [str] nwis product either 'iv' (instantaneous value) or
    'dv' (daily value)
    :param param_cd:
    :param out_file:[str] path to csv file where data should be written
    """
    base_url = 'https://waterservices.usgs.gov/nwis/site/?format=rdb&huc={}' \
          '&parameterCd=00060&siteStatus=all&hasDataTypeCd={}'
    url = base_url.format(huc, product)
    response = requests.get(url)
    data = convert_response_to_df(response.text)
    if out_file:
        data.to_csv(out_file)
    return data


def get_sites_all_hucs(product, param_cd, out_file=None):
    """
    get the sites for a given huc2 and parameter code and and optionally write
    them to a csv file
    :param product: [str] nwis product either 'iv' (instantaneous value) or
    'dv' (daily value)
    :param param_cd: [str] USGS NWIS 5-digit parameter code
    :param out_file:[str] path to csv file where data should be written
    """
    hucs = [f'{h:02}' for h in range(1, 19)]
    d = []
    for huc in hucs:
        site_df = get_sites_for_one_huc(huc, product, param_cd)
        d.append(site_df)
    d_combined = pd.concat(d)
    if out_file:
        d_combined.to_csv(out_file)
    return d_combined
