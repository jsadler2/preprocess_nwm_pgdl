import boto3
import xarray as xr
import fsspec

# Standard or standard-ish imports
import warnings

# Neither of these are accessed directly, but both need to be installed; they're used
# via fsspec
import adlfs

account_name = 'daymeteuwest'
container_name = 'daymet-zarr'

from prefect import task, Flow, Parameter
from prefect.engine.results import S3Result
from scripts import weight_map as wm
from scripts import subset_nhd

# set the profile name based on ~/.aws/credentials entry
boto3.setup_default_session(profile_name='ds-drb-creds')

s3_result = S3Result(bucket='ds-drb-data', location='prefect_out/{task_name}')

# INPUTS
huc4s = ['1401', '1402']

fs = fsspec.filesystem('s3', profile='ds-drb-creds', anon=False)
huc4file = fs.open("ds-drb-data/wbdhu4_a_us_september2020.gpkg")
nhd_catchment_file = fs.open("ds-drb-data/nhd_Catchment.gpkg")

# consolidated=True speeds of reading the metadata
store = fsspec.get_mapper('az://' + container_name + '/daily/na.zarr', account_name=account_name)
ds_input = xr.open_zarr(store, consolidated=True)

#OUTPUT file
out_zarr = fs.get_mapper("ds-drb-data/ucrb/ucrb_prepped")


@task
def write_to_zarr(ds, outfile):
    xr.to_zarr(outfile)


# FLOW
with Flow("my flow", result=s3_result) as flow:
    huc4_gdf = subset_nhd.huc4_gdf(huc4file, huc4s)
    nhd_subset_gdf = subset_nhd.subset_nhd_catchments(nhd_catchment_file , huc4_gdf)
    ds_subset = subset_nhd.subset_ds(ds_input, huc4_gdf)

    weight_map = wm.get_weight_map(ds_subset, nhd_subset_gdf)
    aggregated = wm.get_aggregated_data(ds_subset, weight_map)
    write_to_zarr(aggregated, out_zarr)

flow.run()

