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

out_dir = "nwm_dl_data"


rule all:
    input:
        weight_grid = f'{indicator_dir}weight_matrix_nwis_net',

rule make_sample_netcdf:
    input:
        S3.remote("ds-drb-data/nldas"),
    output:
        S3.remote(f"ds-drb-data/{out_dir}/weight_grid/sample.nc"),
    run:
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
        S3.remote(f'ds-drb-data/{out_dir}/weight_grid/grid_num_proj.json')
        polygon_file = f'{data_dir}/nwis_network/dissolved_nwis_cln.gpkg',
        grid = rules.project_grid_vector_to_5070.output
    output:
        rules.all.input.weight_grid
    run:
        out_zarr = f'{data_dir}weight_grid/nwis_net_weight_grid'
        wt.calculate_weight_matrix_chunks(input[0], input[1], out_zarr,
                                         num_splits=10,
                                         polygon_id_col='dissolve_comid')
        write_indicator_file(wt.calculate_weight_matrix_chunks, output[0])

