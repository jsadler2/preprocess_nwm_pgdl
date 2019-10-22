from pull_nldas import get_urs_pass_user, connect_to_urs
import xarray as xr
import pandas as pd
import geopandas as gpd
from osgeo import gdal
import math

def make_example_nc(netrc_file_path, data_dir):
    user, password  = get_urs_pass_user(netrc_file_path)
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
    inter['weighted_area'] = inter['new_area']/inter['orig_area']
    matrix_df = inter.pivot(index='FEATUREID', columns='DN',
			    values='weighted_area')
    return matrix_df

def calculate_weight_matrix(nhd_gdb, grid_file, out_file):
    grid_gdf = gpd.read_file(grid_file)

    layer = 'Catchment'
    cathment_gdf = gpd.read_file(nhd_gdb, layer=layer)
    print("I've read in all the catchments", flush=True)
    nrows = cathment_gdf.shape[0]
    num_splits = 15
    num_per_chunk = nrows/num_splits
    chunk_list = []
    for n in range(num_splits):
        start_chunk = math.floor(num_per_chunk*n)
        end_chunk = math.floor(num_per_chunk*(n+1))
        print(f"getting wgt matrix for {start_chunk} to {end_chunk}",
              flush=True)
        nhd_chunk = cathment_gdf.iloc[start_chunk: end_chunk, :]
        chunk_wgts = calculate_weight_matrix_one_chunk(nhd_chunk, grid_gdf)
        chunk_list.append(chunk_wgts)
    all_chunks = pd.concat(chunk_list)
    all_chunks.to_parquet(out_file)
    # return chunk_list

nhd_gdb = ("D:\\nhd\\NHDPlusV21_NationalData_Seamless_Geodatabase_Lower48_07"
           "\\NHDPlusNationalData"
           "\\NHDPlusV21_National_Seamless_Flattened_Lower48.gdb")
# target_dir = ("D:\\nhd\\catchments\\")
# split_catchment_geojson(nhd_gdb, target_dir)
grid_gdb = ("../data/nldas_grid_proj.geojson")
out_file = ("../data/weight_matrix.parquet")

calculate_weight_matrix(nhd_gdb, grid_gdb, out_file)
