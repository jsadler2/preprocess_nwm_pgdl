from pull_nldas import get_urs_pass_user, connect_to_urs
import xarray as xr
import geopandas as gpd
from osgeo import gdal
import math

def make_example_nc(netrc_file_path, data_dir):
    user, password  = get_urs_pass_user(netrc_file_path)
    ds = connect_to_urs(user, password) 
    ds.pressfc[0, :, :].to_netcdf('{}sample_nldas.nc')

# netrc_file = 'C:\\Users\\jsadler\\.netrc'
# make_example_nc(netrc_file, "../data")

def split_catchment_geojson(nhd_gdb, target_dir):
    layer = 'Catchment'
    gdf = gpd.read_file(nhd_gdb, layer=layer)
    print("I've read in all the catchments")
    nrows = gdf.shape[0]
    num_splits = 20
    num_per_chunk = nrows/num_splits
    for n in range(num_splits):
        if n > 10:
            new_filename = f'{target_dir}nhd_catchments_split_{n}.geojson'
            start_chunk = math.floor(num_per_chunk*n)
            end_chunk = math.floor(num_per_chunk*(n+1))
            print(f"I'm going to write {start_chunk} to {end_chunk}",
                  flush=True)
            gdf.iloc[start_chunk: end_chunk, :].to_file(new_filename,
                                                        driver='GeoJSON')

nhd_gdb = ("D:\\nhd\\NHDPlusV21_NationalData_Seamless_Geodatabase_Lower48_07"
           "\\NHDPlusNationalData"
           "\\NHDPlusV21_National_Seamless_Flattened_Lower48.gdb")
target_dir = ("D:\\nhd\\catchments\\")
split_catchment_geojson(nhd_gdb, target_dir)

