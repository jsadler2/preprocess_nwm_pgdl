import pandas as pd
import json
import requests
from utils import divide_chunks, get_sites_not_done


def get_comids(huc2):
    df = pd.read_csv(f'../data/catchments_huc{huc2}.csv',
                     dtype={'FEATUREID': str})
    comids = df['FEATUREID']
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


def make_nldi_call(url):
    response = requests.get(url)
    data = json.loads(response.text)
    return data


def data_to_df(json_data):
    raw_df = pd.DataFrame(json_data['characteristics'])
    raw_df['characteristic_value'] = pd.to_numeric(
        raw_df['characteristic_value'])
    # format it
    raw_trans = raw_df.T
    raw_trans.columns = raw_trans.loc['characteristic_id', :]
    raw_trans.drop('characteristic_id', axis=0, inplace=True)
    # getting rid of percent no_data
    # todo: should we keep this and use it as a sort of weight?
    raw_trans.drop('percent_nodata', axis=0, inplace=True)
    raw_trans.index = [json_data['comid']]
    return raw_trans


def get_one_comid_attr(comid):
    url = generate_nldi_url('comid', comid, 'tot')
    json_data = make_nldi_call(url)
    df = data_to_df(json_data)
    return df


def append_to_zarr(df, out_zarr_file):
    ds = df.to_xarray()
    ds.to_zarr(out_zarr_file, mode='a', append_dim='comid')


def append_to_csv(df, out_csv_file):
    with open(out_csv_file, 'a') as f:
        df.to_csv(f, header=f.tell()==0)


def get_data_all_comids_huc2(huc2, out_file, file_type='zarr'):
    comid_list = get_comids(huc2)
    not_done_comids = get_sites_not_done(out_file, comid_list, 'comid',
                                         file_type)
    chunk_size = 20
    chunked_list = divide_chunks(not_done_comids, chunk_size)
    for chunk in chunked_list:
        df_list = []
        for comid in chunk:
            try:
                comid_df = None
                comid_df = get_one_comid_attr(comid)
                print(f"got data for {comid_list.index(comid)}"
                      f" out of {len(comid_list)} comids", flush=True)
                if comid_df is not None:
                    df_list.append(comid_df)
            except requests.exceptions.ConnectionError:
                continue

        df_combined = pd.concat(df_list)
        df_combined.index.name = 'comid'

        if file_type == 'zarr':
            append_to_zarr(df_combined, out_file)
        elif file_type == 'csv':
            append_to_csv(df_combined, out_file)


if __name__ == "__main__":
    huc = '02'
    get_data_all_comids_huc2(huc, f'E:\\data\\nhd_attr\\nhd_attr_{huc}.csv',
                             file_type='csv')

