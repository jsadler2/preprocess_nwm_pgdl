import boto3

# set the profile name based on ~/.aws/credentials entry
boto3.setup_default_session(profile_name='ds-drb-creds')

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
S3 = S3RemoteProvider()

# add scripts dir to path
import sys
scripts_path =  os.path.abspath("scripts/")
sys.path.insert(0, scripts_path)

import weight_grid_nldas as wt
from apply_weight_grid import apply_nldas_weight_grid

out_dir = "nwm_dl_data"


def filter_zattrs_io(orig):
    filtered = []
    for filename in orig:
        if os.path.split(filename)[1] == '.zattrs':
            filtered.append(os.path.split(filename)[0])
        else:
            filtered.append(filename)
    return filtered

def filter_zattrs(inputs, outputs):
    return filter_zattrs_io(inputs), filter_zattrs_io(outputs)


rule all:
    input:
        S3.remote(f'ds-drb-data/{out_dir}/taylor_river_drivers/.zattrs')

rule make_sample_netcdf:
    input:
        S3.remote("ds-drb-data/nldas/.zattrs"),
    output:
        S3.remote(f"ds-drb-data/{out_dir}/weight_grid/sample.nc"),
    run:
        input, output = filter_zattrs(input, output)
        wt.make_example_nc(input[0], output[0])

rule make_grid_num_nc:
    input:
        rules.make_sample_netcdf.output
    output:
        S3.remote(f"ds-drb-data/{out_dir}/weight_grid/grid_num.nc"),
    run:
        wt.create_grid_num_nc(input[0], output[0])

rule convert_nc_to_geotiff:
    input:
        rules.make_grid_num_nc.output
    output:
        S3.remote(f'ds-drb-data/{out_dir}/weight_grid/grid_num.tif')
    run:
        wt.nc_to_tif(input[0], output[0])

rule convert_tif_to_polygon:
    input:
        rules.convert_nc_to_geotiff.output
    output:
        S3.remote(f'ds-drb-data/{out_dir}/weight_grid/grid_num.json')
    run:
        wt.tif_to_polygon(input[0], output[0])

rule project_grid_vector_to_5070:
    input:
        rules.convert_tif_to_polygon.output
    output:
        S3.remote(f'ds-drb-data/{out_dir}/weight_grid/grid_num_proj.json')
    run:
        wt.project_grid_vector(input[0], output[0], 5070)

rule weight_matrix:
    input:
        S3.remote(f'ds-drb-data/taylor_river_nhd_catchments.geojson'),
        S3.remote(f'ds-drb-data/{out_dir}/weight_grid/grid_num_proj.json'),
    output:
        S3.remote(f'ds-drb-data/{out_dir}/weight_grid/taylor_river_weight_grid/.zattrs')
    run:
        input, output = filter_zattrs(input, output)
        wt.calculate_weight_matrix_chunks(input[0], input[1], output[0],
                                         num_splits=1)

rule apply_weight_matrix:
    input:
        S3.remote(f'ds-drb-data/{out_dir}/weight_grid/taylor_river_weight_grid/.zattrs'),
        S3.remote("ds-drb-data/nldas/.zattrs"),
    output:
        S3.remote(f'ds-drb-data/{out_dir}/taylor_river_drivers/.zattrs')
    run:
        input, output = filter_zattrs(input, output)
        apply_nldas_weight_grid(input[0], input[1], output[0])
