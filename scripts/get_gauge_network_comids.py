import os
from utils import generate_nldi_url, json_from_nldi_request,\
    read_nwis_comid
import pandas as pd
from json.decoder import JSONDecodeError

# read in the comid/nwis table
# read in the nwis ids for which we have data
# get the upstream comids for each nwis comid
# parse those out of the response
# write them to a file
# get the downstream main?


def get_us_comid_data(comid):
    url = generate_nldi_url('comid', comid, 'navigate/UT')
    data = json_from_nldi_request(url)
    return data


def get_upstream_comid_list(data):
    comid_list = [f['properties']['nhdplus_comid'] for f in data['features']]
    return comid_list


def get_upstream_comid_one(comid):
    data = get_us_comid_data(comid)
    comid_list = get_upstream_comid_list(data)
    return {'comid': comid, 'US_comids': comid_list}


def get_upstream_comids_all(out_file):
    nwis_comids = read_nwis_comid()['comid']
    all_comids = []
    for comid in nwis_comids:
        print(f'getting US comids for {comid}')
        try:
            comids_us = get_upstream_comid_one(comid)
            all_comids.append(comids_us)
        except JSONDecodeError:
            pass
    combined = pd.concat(all_comids)
    combined.to_csv(out_file)
    return combined


get_upstream_comids_all('../data/tables/upstream_comids.csv')
