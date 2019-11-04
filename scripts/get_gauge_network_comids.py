import os
from utils import generate_nldi_url, json_from_nldi_request,\
    read_nwis_comid, get_indices_not_done, divide_chunks
import pandas as pd
from json.decoder import JSONDecodeError

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


def filter_intermediate(US_comid_table):
    """
    filter so that comids are not recorded twice. the comids for a given
    station are only the ones upstream of it and downstream of the next
    upstream gauge
    """
    pass


if __name__ == '__main__':
    get_upstream_comids_all('../data/tables/upstream_comids.csv')
