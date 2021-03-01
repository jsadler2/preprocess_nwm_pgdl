import os
import numpy as np
from pull_nldas import get_urs_pass_user, connect_to_urs
from utils import convert_df_to_dataset, divide_chunks
import xarray as xr
import pandas as pd
import geopandas as gpd
import rioxarray
from osgeo import gdal, osr, ogr
import math


def make_example_nc(netrc_file_path, out_file):
    user, password = get_urs_pass_user(netrc_file_path)
    ds = connect_to_urs(user, password)
    ds = ds.isel(time=0)
    ds[['pressfc']].to_netcdf(out_file)


def make_blank_weight_grid(catchment_ids, grid_ids, out_zarr):
    catchment_chunk_size = 10000
    chunked_catchments = divide_chunks(catchment_ids, catchment_chunk_size)
    i = 0
    for indices in chunked_catchments:
        print(f'doing chunk {i}', flush=True)
        blank = pd.DataFrame(0, index=indices, columns=grid_ids, dtype='float32')
        col_name = 'nldas_grid_no'
        idx_name = 'nhd_comid'
        chunks = {col_name: 10000, idx_name: 30000}
        ds = convert_df_to_dataset(blank, col_name, idx_name, 'weight',
                                   chunks)
        ds.to_zarr(out_zarr, mode='a', append_dim=idx_name)


def make_blank_all_comid_nldas(all_comid_file, out_zarr):
    df = pd.read_csv(all_comid_file)
    all_comids = df[df.columns[-1]]
    # nldas has 224 rows and 464 columns
    nldas_ids = list(range(224*464))
    make_blank_weight_grid(all_comids, nldas_ids, out_zarr)


def create_grid_num_nc(sample_nc_path, out_file):
    ds = xr.open_dataset(sample_nc_path)
    lat_num = len(ds.lat)
    lon_num = len(ds.lon)
    total_num_grids = lat_num * lon_num
    num_list = list(range(total_num_grids))
    num_array = np.array(num_list)
    num_array = num_array.reshape(lat_num, lon_num)
    col_name = 'grid_num'
    ds[col_name] = (('lat', 'lon'), num_array)
    ds[[col_name]].to_netcdf(out_file)


def nc_to_tif(nc_file, out_file):
    ds = xr.open_dataset(nc_file)
    ds.rio.set_crs('epsg:4326')
    grid_num_arr = ds['grid_num']
    grid_num_arr.rio.set_spatial_dims('lon', 'lat', inplace=True)
    grid_num_arr.rio.to_raster(out_file)


def tif_to_polygon(tif_file, polygon_out):
    """
    convert a netcdf file into a vector polygon representation of the grid
    contained in the netcdf file
    :param tif_file: [str] file path to the tif file
    :param polygon_out: [str] file path to where the polygon file should be
    saved
    :return: None
    """
    raster_ds = gdal.Open(tif_file)
    band = raster_ds.GetRasterBand(1)
    driver = ogr.GetDriverByName('GeoJSON')
    out_source = driver.CreateDataSource(polygon_out)
    outLayer = out_source.CreateLayer("grid")
    newField = ogr.FieldDefn('grid_num', ogr.OFTInteger)
    outLayer.CreateField(newField)
    gdal.Polygonize(band, None, outLayer, 0, [], callback=None)
    out_source.Destroy()
    raster_ds = None


def project_grid_vector(grid_vector_file, out_file, target_epsg=5070):
    gdf = gpd.read_file(grid_vector_file)
    gdf_proj = gdf.to_crs({'init': f'epsg:{target_epsg}'})
    gdf_proj.to_file(out_file, driver='GeoJSON')


def calculate_weight_matrix_one_chunk(nhd_catchments, grid_gdf, polygon_id_col,
                                      grid_id_col='grid_num',
                                      str_col_names=False,
                                      ):
    """
    calculate the weight matrix for a subset (or theoretically all) nhd
    catchments which are stored in a geodataframe
    :param nhd_catchments: [geodataframe] (subset of) nhd catchment layer
    :param grid_gdf: [geodataframe] the grid for which the weight matrix will
    :param str_col_names: [bool] whether the col names should be converted to
    a string be calculated
    :return:
    """

    # create blank df to populate so that all have the same shape
    num_grid_cells = grid_gdf.shape[0]
    blank_df = pd.DataFrame(0, index=nhd_catchments[polygon_id_col],
                            columns=range(num_grid_cells))

    # get the original area before doing the intersection
    nhd_catchments['orig_area'] = nhd_catchments.geometry.area

    inter = gpd.overlay(grid_gdf, nhd_catchments, how='intersection')

    # get the area after the intersection
    inter['new_area'] = inter.geometry.area

    # get the weighted area
    inter['weighted_area'] = inter['new_area'] / inter['orig_area']

    # pivot so we get the weight matrix
    matrix_df = inter.pivot(index=polygon_id_col, columns=grid_id_col,
                            values='weighted_area')

    # add the weight matrix to the blank
    matrix_df = blank_df + matrix_df

    # replace any nan with zero
    matrix_df.fillna(0, inplace=True)

    if str_col_names:
        # convert all column names to strings (parquet needs str col names)
        matrix_df.columns = [str(c) for c in matrix_df.columns]
    return matrix_df


def calculate_weight_matrix_chunks(polygon_file, grid_file, out_zarr_store,
                                   num_splits=15, layer=None,
                                   polygon_id_col='FEATUREID'):
    """
    calculate the weight matrix in chunks for all nhd catchments over a given
    grid. The output of this is a zarr data store
    :param polygon_file: [str] file path to the nhd geodatabase with the
    catchment layer
    :param grid_file: [str] file path to the geometric file that has the grid.
    This should be a projected, vectorized representation of the grid
    :param out_zarr_store: [str] path to the output zarr store
    :return: None
    """
    grid_gdf = gpd.read_file(grid_file)

    catchment_gdf = gpd.read_file(polygon_file, layer=layer)
    print("read in all catchments", flush=True)
    # project catchments into same projection as grid
    catchment_gdf = catchment_gdf.to_crs(grid_gdf.crs)
    print("projected all catchments", flush=True)
    nrows = catchment_gdf.shape[0]
    num_per_chunk = nrows / num_splits
    for n in range(num_splits):
        start_chunk = math.floor(num_per_chunk * n)
        end_chunk = math.floor(num_per_chunk * (n + 1))
        print(f"getting wgt matrix for {start_chunk} to {end_chunk}",
              flush=True)
        nhd_chunk = catchment_gdf.iloc[start_chunk: end_chunk, :]
        chunk_wgts = calculate_weight_matrix_one_chunk(nhd_chunk, grid_gdf,
                                                       polygon_id_col)
        save_zarr(chunk_wgts, out_zarr_store)


def save_zarr(chunk_df, out_zarr):
    col_name = 'nldas_grid_no'
    idx_name = 'nhd_comid'
    chunks = {col_name: 10000, idx_name: 30000}
    ds = convert_df_to_dataset(chunk_df, col_name, idx_name, 'weight',
                               chunks)
    ds.to_zarr(out_zarr, mode='a', append_dim=idx_name)



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


def resize_individual_df_cols(all_cols, indvi_df):
    """
    resize the df chunks to the same cols as the overall placeholder and replace
    the NaNs with zeros and convert back to uint8. this is to ensure all have
    the same number of columns so we can append to the zarr dataset
    :param all_cols: [pandas index] all the column names (all the numbers in the
    NLDAS grid)
    :param indvi_df: [pandas df] the individual df (a chunk of the overall one)
    :return: [pandas df] the individual df but with all columns
    """
    # convert cols to int
    indvi_df.columns = indvi_df.columns.astype('uint32')

    # get columns not in individual df
    in_individual = np.isin(all_cols, indvi_df.columns)
    missing_cols = all_cols[~in_individual]
    # blank df with new cols
    blank_df = pd.DataFrame(0, columns=missing_cols,
                            index=indvi_df.index, dtype='uint8')
    combined = pd.concat([indvi_df, blank_df], axis=1)
    combined = combined.reindex(sorted(combined.columns), axis=1)
    return combined


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
        resized = resize_individual_df_cols(placeholder, d)
        placeholder = placeholder + resized
    return placeholder


def get_chunked_files_list(chunk_folder):
    file_list = []
    for f in os.listdir(chunk_folder):
        if f.endswith('uint8.parquet'):
            file_path = os.path.join(chunk_folder, f)
            file_list.append(file_path)
    return file_list


def get_df_list(chunk_folder):
    df_list = []
    file_list = get_chunked_files_list(chunk_folder)
    for f in file_list:
        print('reading in ', f, flush=True)
        df = pd.read_parquet(f)
        df_list.append(df)
    return df_list


def get_cols_from_chunk_folder(chunk_folder):
    df_list = get_df_list(chunk_folder)
    cols = get_all_col_or_idx(df_list, 'col')
    return cols


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
    all_cols = get_cols_from_chunk_folder(chunk_folder)
    i = 0
    chunk_files = get_chunked_files_list(chunk_folder)
    for f in chunk_files:
        d = pd.read_parquet(f)
        nrows = d.shape[0]
        num_mini_splits = 20
        num_per_split = nrows / num_mini_splits
        for n in range(num_mini_splits):
            start_chunk = math.floor(num_per_split * n)
            end_chunk = math.floor(num_per_split * (n + 1))
            print(f"processing mini-chunk {start_chunk} to {end_chunk}",
                  flush=True)
            mini_chunk = d.iloc[start_chunk: end_chunk, :]
            resized = resize_individual_df_cols(all_cols, mini_chunk)
            resized = resized/255.

