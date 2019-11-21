import json
import geopandas as gpd
from scripts.utils import generate_nldi_url, json_from_nldi_request,\
    read_nwis_comid, get_indices_not_done, divide_chunks
import pandas as pd
from json.decoder import JSONDecodeError


def make_zero_buffer_catchments(nhd_gdb, out_file):
    nhd_gdf = gpd.read_file(nhd_gdb)
    nhd_gdf['geometry'] = nhd_gdf.geometry.buffer(0)
    nhd_gdf.to_file(out_file)


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
    comid_list = [int(f['properties']['nhdplus_comid'])
                  for f in data['features']]
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
    delete_duplicates(out_file)


def delete_duplicates(out_file):
    all_df = pd.read_csv(out_file, index_col='comid')
    all_df_cln = all_df.drop_duplicates()
    all_df_cln.to_csv(out_file)


def get_all_us_nwis_comids_from_df(comid, all_comid_df):
    """
    retrieve all us_comids from the all_comid_df for a given comid
    :param comid: [int] the comid for which you want the upstream comids
    :param all_comid_df: [pandas dataframe] dataframe with comids in one column
    and the all US comids in the other
    :return: [pandas series] all comids upstream of the given comid
    """
    all_us_comids = all_comid_df.loc[comid, 'US_comids']
    all_us_comids_list = str_list_to_series(all_us_comids)
    return all_us_comids_list


def get_us_nwis_comids(comid, all_comid_df):
    """
    get all of the comids that are upstream of a given nwis comid that also
    have an nwis comid
    :param comid: [int] comid for which you want the US nwis comids
    :param all_comid_df: [pandas dataframe] dataframe with comids in one column
    and the all US comids in the other
    :return: [list] list of comids upstream of the given comid
    """
    all_us_comids_list = get_all_us_nwis_comids_from_df(comid, all_comid_df)
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
    combined_comid_list = str_list_to_series(combined_comids)
    return combined_comid_list


def str_list_to_series(str_of_list):
    """
    convert a string representation of a list into a list and then a pandas
    Series
    :param str_of_list: [str] representation of a list (e.g., '[1, 2, 3]')
    :return: [pandas series]
    """
    str_of_list = str_of_list.replace(']', ',')
    str_of_list = str_of_list.replace('[', ',')
    str_of_list = str_of_list.split(',')
    stripped = [int(a.strip()) for a in str_of_list if a != '']
    return pd.Series(stripped)


def get_intermediate_comids(nwis_comid, all_comid_df):
    """
    get all of the comids between an nwis_comid and the next nwis_comids
    upstream. first get all of the comids upstream of the nwis comid. then get
    the nwis comids upstream of the nwis_comid. then combine all upstream comids
    of the upstream nwis comids. then take out the combined comids from the
    full set of upstream comids of the nwis_comid.
    :param nwis_comid: [int] the nwis comid for which you want the intermediate
    comids
    :param all_comid_df: [pandas dataframe] dataframe with comids in one column
    and the all US comids in the other
    :return: [list] a list of intermediate comids
    """
    all_us_comids = get_all_us_nwis_comids_from_df(nwis_comid, all_comid_df)
    us_nwis_comids = get_us_nwis_comids(nwis_comid, all_comid_df)
    if len(us_nwis_comids) > 0:
        all_comids_us_of_us_nwis = combine_us_comids(us_nwis_comids,
                                                     all_comid_df)
        intermediate_comid_mask = all_us_comids.isin(all_comids_us_of_us_nwis)
        intermediate_comids = all_us_comids[~intermediate_comid_mask]
    else:
        intermediate_comids = all_us_comids
    return intermediate_comids.to_list()


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
    :return: [pandas dataframe] dataframe with intermediate values
    """
    df_all = pd.read_csv(us_comid_file, index_col='comid')
    intermediate_all = []
    for comid in df_all.index:
        intermediate_comids = get_intermediate_comids(comid, df_all)
        intermediate_all.append({'comid': comid,
                                 'intermediate_comids': intermediate_comids})
    intermediate_df = pd.DataFrame(intermediate_all)
    intermediate_df.to_csv(outfile, index=False)
    return intermediate_df


def check_intermediate(intermediate_df):
    """
    check to make sure in the intermediate comids there are no duplicates
    :param intermediate_df: [pandas dataframe] dataframe with columns 'comid'
    and 'intermediate_comids'
    :return:None
    """
    combined = intermediate_df['intermediate_comids'].sum()
    intermediate_series = str_list_to_series(combined)
    check = intermediate_series.is_unique
    if not check:
        raise ValueError('something went wrong with getting the intermediate'
                         'comids')



def dissolve_intermediate(inter_comid_file, full_gdf, out_file):
    """
    dissolve the intermediate nhd catchments based on info in the intermediate
    comid file and the full geometry file. Save to a new file (geojson)
    :param inter_comid_file: [str] path to a csv file that contains the
    intermediate comid information with one column the 'comid' and the other
    column 'intermediate_comids'
    :param full_geom_file: [geopandas geodataframe] gdf with all the nhd
    catchments
    :param out_file: [str] path to a geojson file to which the dissolved data
    will be saved
    :return: [geoppandas geodataframe] gdf with the dissolved catchments
    """
    inter_df = pd.read_csv(inter_comid_file)
    inter_comid_col = 'intermediate_comids'
    inter_df[inter_comid_col] = inter_df[inter_comid_col].apply(json.loads)
    # explodes converts the list of intermediate comids into their own rows
    int_ex = inter_df.explode('intermediate_comids')
    # set the intermediate_comids as index b/c we will join on that
    int_ex.set_index('intermediate_comids', inplace=True)
    # we name the column dissolve comid b/c we will dissolved based on that
    int_ex.columns = ['dissolve_comid']
    # set the index as the comid
    full_gdf.set_index('FEATUREID', inplace=True)

    full_gdf = full_gdf.join(int_ex)
    print("performing dissolve")
    full_gdf_diss = full_gdf.dissolve('dissolve_comid')
    print("writing dissolved file")
    full_gdf_diss.reset_index(inplace=True)
    full_gdf_diss.to_file(out_file, driver='GPKG')
    return full_gdf_diss


def dissolve_intermediate_all_conus(int_comid_file, outfile):
    nhd_gdb = ("D:\\nhd\\NHDPlusV21_NationalData_Seamless_Geodatabase_Lower48_07"
               "\\NHDPlusNationalData"
               "\\NHDPlusV21_National_Seamless_Flattened_Lower48.gdb")
    layer = 'Catchment'
    cathment_gdf = gpd.read_file(nhd_gdb, layer=layer)
    print("read in nhd catchments")
    dissolve_intermediate(int_comid_file, cathment_gdf, outfile)


if __name__ == '__main__':
    pass
