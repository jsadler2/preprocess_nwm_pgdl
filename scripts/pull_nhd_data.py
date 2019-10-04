import pandas as pd
import json
import requests
from utils import json_from_nldi_request, generate_nldi_url, get_nldi_data_huc2


def get_comids(huc2):
    df = pd.read_csv(f'../data/catchments_huc{huc2}.csv',
                     dtype={'FEATUREID': str})
    comids = df['FEATUREID']
    comids_non_zero = comids[comids != '0']
    return comids_non_zero.to_list()


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
    raw_trans.index.name = 'comid'
    return raw_trans


def get_one_comid_attr(comid):
    url = generate_nldi_url('comid', comid, 'tot')
    json_data = json_from_nldi_request(url)
    df = data_to_df(json_data)
    return df


def get_data_all_comids_huc2(huc2, out_file, out_file_type):
    comid_list = get_comids(huc2)
    get_nldi_data_huc2(comid_list, out_file, get_one_comid_attr, 'comid',
                       'csv')



if __name__ == "__main__":
    huc = '02'
    get_data_all_comids_huc2(huc, f'E:\\data\\nhd_attr\\nhd_attr_{huc}.csv',
                             out_file_type='csv')

