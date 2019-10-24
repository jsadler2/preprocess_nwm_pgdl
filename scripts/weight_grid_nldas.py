import os
import numpy as np
from pull_nldas import get_urs_pass_user, connect_to_urs
import xarray as xr
import pandas as pd
import geopandas as gpd
from osgeo import gdal
import math


def make_example_nc(netrc_file_path, data_dir):
    user, password = get_urs_pass_user(netrc_file_path)
    ds = connect_to_urs(user, password)
    ds.pressfc[0, :, :].to_netcdf('{}sample_nldas.nc')


# netrc_file = 'C:\\Users\\jsadler\\.netrc'
# make_example_nc(netrc_file, "../data")

def calculate_weight_matrix_one_chunk(nhd_catchments, grid_gdf):
    # project catchments into same projection as grid
    nhd_catchments_proj = nhd_catchments.to_crs(grid_gdf.crs)

    nhd_catchments_proj['orig_area'] = nhd_catchments_proj.geometry.area
    inter = gpd.overlay(grid_gdf, nhd_catchments_proj, how='intersection')
    inter['new_area'] = inter.geometry.area
    inter['weighted_area'] = inter['new_area'] / inter['orig_area']
    matrix_df = inter.pivot(index='FEATUREID', columns='DN',
                            values='weighted_area')
    # convert all column names to strings (parquet needs str col names)
    matrix_df.columns = [str(c) for c in matrix_df.columns]
    return matrix_df


def calculate_weight_matrix_chunks(nhd_gdb, grid_file, out_dir):
    grid_gdf = gpd.read_file(grid_file)

    layer = 'Catchment'
    cathment_gdf = gpd.read_file(nhd_gdb, layer=layer)
    print("I've read in all the catchments", flush=True)
    nrows = cathment_gdf.shape[0]
    num_splits = 15
    num_per_chunk = nrows / num_splits
    file_list = []
    for n in range(num_splits):
        file_name = f'{out_dir}wgt_matrix_{n}_of_{num_splits}.parquet'
        start_chunk = math.floor(num_per_chunk * n)
        end_chunk = math.floor(num_per_chunk * (n + 1))
        print(f"getting wgt matrix for {start_chunk} to {end_chunk}",
              flush=True)
        nhd_chunk = cathment_gdf.iloc[start_chunk: end_chunk, :]
        chunk_wgts = calculate_weight_matrix_one_chunk(nhd_chunk, grid_gdf)
        chunk_wgts.to_parquet(file_name)
    return file_list


def chunks_to_float16(chunk_folder):
    for f in os.listdir(chunk_folder):
        if f.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(chunk_folder, f))
            df = df.fillna(0)
            df = df * 255
            df = df.astype('uint8')
            new_file_name = os.path.join(chunk_folder,
                                         f.replace('.parquet',
                                                   '_uint8.parquet'))
            df.to_parquet(new_file_name)


def get_all_col_or_idx(df_list, col_or_idx='col'):
    """
    get a sorted index of all unique column or index items in a list of data
    frames (get the unique set)
    :param df_list: [list of pandas dataframes] the list of pandas dataframes
    :param col_or_idx: [str] 'col' or 'index'; whether you are wanting the
    column items or the index items
    :return: [pandas index] unique and sorted items
    """
    all_comids = np.array([])
    for df in df_list:
        if col_or_idx == 'col':
            all_comids = np.append(all_comids, df.columns)
        elif col_or_idx == 'index':
            all_comids = np.append(all_comids, df.index)
        else:
            raise ValueError('col_or_idx arg needs to be "col" or "index"')

    all_comids = all_comids.astype('uint32')
    comids_unique = np.unique(all_comids)
    comids_sorted = np.sort(comids_unique)
    comids_index = pd.Index(comids_sorted)

    if col_or_idx == 'col':
        comids_index = comids_index.astype(str)

    return comids_index


def create_placeholder_df(df_list):
    """
    create a place holder df of zeros with the combined indices and columns
    :param df_list: [list of dataframes] list of dataframes for which you will
    create the placeholder df
    :return: [pandas df] df of all zeros
    """
    all_cols = get_all_col_or_idx(df_list, 'col')
    print(len(all_cols))
    all_idx = get_all_col_or_idx(df_list, 'index')
    df = pd.DataFrame(0, columns=all_cols, index=all_idx, dtype='uint8')
    return df


def resize_individual_df(placeholder, indvi_df):
    """
    resize the df chunks to the same as the overall placeholder and replace
    the NaNs with zeros and convert back to uint8
    :param placeholder: [pandas df] the placeholder df with the overall
    dimensions, index, and columns
    :param indvi_df: [pandas df] the indivual df (a chunk of the overall one)
    :return: [pandas df] the individual df but indexed like the placeholder with
    the correct dtype
    """
    resized = indvi_df.reindex_like(placeholder)
    resized = resized.fillna(0)
    resized = resized.astype('uint8')
    return resized


def combine_dfs_into_placeholder(placeholder, df_list):
    """
    combine the individual chunks of the weight matrix
    :param placeholder: [pandas df] df of zeros with the combined index and
    columns from all the dfs
    :param df_list: [list of dfs] the list containing the individual chunks of
    the weight matrix as dfs
    :return: [df] a combined weight matrix
    """
    for d in df_list:
        resized = resize_individual_df(placeholder, d)
        placeholder = placeholder + resized
    return placeholder


def df_to_zarr(df):
    data_array = xr.DataArray(df.values,
                              [('comid', df.index),
                               ('nldas_grid_no', df.columns)])
    data_set = xr.Dataset({'weight': data_array})
    return data_set


def merge_weight_grid(chunk_folder, all_file_name):
    """
    read and merge the individual weight grid parquet files
    :param chunk_folder: [str] path to where the individual files are located.
    it is assumed that the files end with "uint8.parquet"
    :param all_file_name: [str] filename that you want the combined data to be
    stored in. it assumed that the folder is the same as the one where the
    individual files are stored
    :return: None
    """
    for f in os.listdir(chunk_folder):
        if f.endswith('uint8.parquet'):
            print('reading in ', f, flush=True)
            df = pd.read_parquet(os.path.join(chunk_folder, f))
            df = df/100.
            ds = df_to_zarr(df)
            ds.to_zarr(os.path.join(chunk_folder, all_file_name),
                       append_dim='comid', mode='a')
            

if __name__ == '__main__':
    nhd_gdb = ("D:\\nhd\\NHDPlusV21_NationalData_Seamless_Geodatabase_Lower48_07"
               "\\NHDPlusNationalData"
               "\\NHDPlusV21_National_Seamless_Flattened_Lower48.gdb")
    # target_dir = ("D:\\nhd\\catchments\\")
    # split_catchment_geojson(nhd_gdb, target_dir)
    grid_gdb = "../data/nldas_grid_proj.geojson"
    out_dir = "../data/wgt_matrix_chunks/"

    # calculate_weight_matrix_chunks(nhd_gdb, grid_gdb, out_dir)
    merge_weight_grid(out_dir, "weight_matrix_all")
    # chunks_to_float16(out_dir)
