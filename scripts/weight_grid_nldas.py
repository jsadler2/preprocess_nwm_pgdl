from pull_nldas import get_urs_pass_user, connect_to_urs
import xarray as xr
from osgeo import gdal

def make_example_nc(netrc_file_path, data_dir):
    user, password  = get_urs_pass_user(netrc_file_path)
    ds = connect_to_urs(user, password) 
    ds.pressfc[0, :, :].to_netcdf('{}sample_nldas.nc')

netrc_file = 'C:\\Users\\jsadler\\.netrc'
make_example_nc(netrc_file, "../data")

