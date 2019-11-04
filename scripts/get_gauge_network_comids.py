import os
from utils import generate_nldi_url, json_from_nldi_request,\
    read_nwis_comid, get_indices_not_done, divide_chunks
import pandas as pd
from json.decoder import JSONDecodeError
import json

# read in the comid/nwis table
# read in the nwis ids for which we have data
# get the upstream comids for each nwis comid
# parse those out of the response
# write them to a file
# get the downstream main?


def get_us_comid_data(comid):
    """
    get the nldi json response which contains the information of upstream
    comids for one comid at an nwis site
    :param comid: [str] the comid for which you want the US information
    :return: [dict] the json data read into a python dict
    """
    url = generate_nldi_url('comid', comid, 'navigate/UT')
    data = json_from_nldi_request(url)
    return data


def parse_us_comid_data(data):
    """
    parse the us comid data from the nldi response to get out just the us
    comids
    :param data: [dict] the response from the nldi navigate/UT request
    :return: [list] a list of the comids upstream of the comid for which
    """
    comid_list = [f['properties']['nhdplus_comid'] for f in data['features']]
    return comid_list


def get_upstream_comid_list(comid):
    """
    get a list of comids that are upstream of a given comid. this is done by
    calling a nldi service and parsing the output
    :param comid: [str] the comid for which you want the US comids
    :return: [list] list of comids upstream of the given comid
    """
    data = get_us_comid_data(comid)
    return parse_us_comid_data(data)


def get_upstream_comid_one(comid):
    """
    get dict with comid and US_comid_list. as a dict it is easier to convert to
    a pandas dataframe
    :param comid: [str] the comid for which you want the US comids
    :return: [dict] contains the comid with key of 'comid' and US comids with
    key 'US_comids'
    """
    comid_list = get_upstream_comid_list(comid)
    return {'comid': comid, 'US_comids': comid_list}


def get_upstream_comids_all(out_file, comid_list=None):
    """
    get the upstream comids for a given list of comids. collect those into a
    pandas dataframe and then write those to a file. this procedure is chunked
    so that a chunk of the comids are written at a time.
    :param out_file: [str] path of file to which the upstream comid information
    should be written
    :param comid_list: [list] list of comids for which you want to get the
    upstream comid information. If no list is provided, the function will read
    in all the nwis comids by calling the read_nwis_comid method and collect
    the us comid info for all of those.
    :return: None
    """
    if not comid_list:
        comid_list = read_nwis_comid()['comid']
    write_chunk = 100
    not_done_comids = get_indices_not_done(out_file, comid_list, 'comid',
                                           'csv', is_column=True)
    chunked_not_done = divide_chunks(not_done_comids, write_chunk)
    for i, comid_chunk in enumerate(chunked_not_done):
        all_comids = []
        print(f'getting chunk {i}', flush=True)
        for comid in comid_chunk:
            try:
                print(f'getting US comids for {comid}', flush=True)
                comids_us = get_upstream_comid_one(comid)
                all_comids.append(comids_us)
            except JSONDecodeError:
                pass
        combined = pd.DataFrame(all_comids)
        print(f'writing chunk {i}', flush=True)
        combined.set_index('comid', inplace=True)
        combined.to_csv(out_file, mode='a', header=not bool(i))


def get_us_nwis_comids(comid, all_comid_df):
    """
    get all of the comids that are upstream of a given nwis comid that also
    have an nwis comid
    :param comid: [int] comid for which you want the US nwis comids
    :param all_comid_df: [pandas dataframe] dataframe with comids in one column
    and the all US comids in the other
    :return: [list] list of comids upstream of the given comid
    """
    all_us_comids = all_comid_df.loc[comid, 'US_comids']
    all_us_comids_list = string_to_list(all_us_comids)
    nwis_us_comid_mask = all_comid_df.index.isin(all_us_comids_list)
    nwis_us_comid = all_comid_df.index[nwis_us_comid_mask]
    # drop the comid in question from the result
    nwis_us_comid = nwis_us_comid.drop(comid)
    return nwis_us_comid


def combine_us_comids(idx_comids, all_comid_df):
    """
    combine the upstream comids for each comid in the idx_comids list into one
    list
    :param idx_comids: [list or pandas index] the comids whose upstream comids
    you want to combine
    :param all_comid_df: [pandas dataframe] dataframe with comids in one column
    and the all US comids in the other
    :return: [list] combined list of comids
    """
    idx_df = all_comid_df.loc[idx_comids, 'US_comids']
    combined_comids = idx_df.sum()
    combined_comid_list = string_to_list(combined_comids)
    return combined_comid_list


def string_to_list(str_of_list):
    """
    :param str_of_list: [str] representation of a list (e.g., '[1, 2, 3]')
    :return: [list] the list
    """
    str_of_list = str_of_list.replace(']', ',')
    str_of_list = str_of_list.replace('[', ',')
    str_of_list = str_of_list.split(',')
    stripped = [a.strip() for a in str_of_list if a != '']
    return stripped
    

def filter_intermediate(us_comid_file, outfile):
    """
    filter so that comids are not recorded twice. the comids for a given
    station are only the ones upstream of it and downstream of the next
    upstream gauge. this filtered data is written to a new csv file
    :param us_comid_file: [str] path to file that contains US comid data. this
    should be a csv with two columns. One columns is 'comid' and the other is
    'US_comids'. The 'US_comids' column contains a list of all comids that are 
    upstream of the comid in the 'comid' column
    :param outfile: [str] filepath to which the filtered data should be written
    :return none:
    """
    df_all = pd.read_csv(us_comid_file)
    for comid in df_all['comid']:
        relevant_comids = [df_all.index]



if __name__ == '__main__':
    get_upstream_comids_all('../data/tables/upstream_comids.csv')
