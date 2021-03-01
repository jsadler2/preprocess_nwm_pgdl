import scripts.utils as su
import geopandas as gpd

# add scripts dir to path
import sys
scripts_path =  os.path.abspath("scripts/")
sys.path.insert(0, scripts_path)

from nwis_comid import get_comids_for_all_nwis_nhd
import get_gauge_network_comids as gt
import weight_grid_nldas as wt
from combine_nhd_attr import filter_combine_nhd_files, combine_attr_to_nwis_net 
from utils import write_indicator_file

HUCS = [f'{h:02}' for h in range (1, 19)]
indicator_dir = "data/indicators/"
data_dir = "D:/nwm-ml-data/"
nhd_gdb = "D:/nhd/NHDPlusV21_NationalData_Seamless_Geodatabase_Lower48_07/NHDPlusNationalData/NHDPlusV21_National_Seamless_Flattened_Lower48.gdb"


nldas_zarr_store_type = 's3'
nldas_zarr_store = f"{data_dir}/nldas/nldas2"

rule all:
    input:
        daily_discharge = expand("{indicator_dir}daily_discharge_huc_{huc}.txt",
                                 huc=HUCS, indicator_dir=indicator_dir),
        nwis_comid_table = f"data/tables/nwis_comid.csv",
        nldas_indicator = f"{indicator_dir}/nldas_indicator_{nldas_zarr_store_type}.txt",
        nwis_site_list = "data/tables/nwis_site_list_dv.csv",
        nwis_network_file = f"{data_dir}nwis_network/dissolved_nwis_network.gpkg",
        nhd_filtered_values = f'{data_dir}nhd_cat_attr/nhd_filtered_values.parquet',
        weight_grid = f'{indicator_dir}weight_matrix_nwis_net',
        nhd_nwis_net = f'{data_dir}nwis_network/nwis_nhd_attr.parquet'

rule get_all_sites:
    output:
        expand("data/all_streamflow_sites_CONUS_{product}.csv",
        product=['iv', 'dv'])
    script:
        "scripts/get_all_streamflow_sites.py"

rule get_daily_discharge:
    input:
        rules.get_all_sites.output
    params:
        hucs=HUCS,
        out_data_files = expand(data_dir+"streamflow_data/discharge_data_{huc}_daily.csv", huc=HUCS)
    output:
        rules.all.input.daily_discharge
    script:
        "scripts/get_all_daily_streamflow_data.py"

rule get_nwis_comid_table:
    input:
        "data/raw/nhd_nwis_all.csv",
        rules.all.input.nwis_site_list
    output:
        rules.all.input.nwis_comid_table
    run:
        print(input)
        get_comids_for_all_nwis_nhd(output[0], input[1], input[0])

rule get_nhd_characteristic_subset_list:
    input:
        metadata_file="data/raw/nhd_characteristics_metadata_table.csv",
        exclude_file="data/tables/nhd_categories_to_exclude.yml"
    output:
        "data/tables/nhd_categories_filtered.csv",
    script:
        "scripts/get_nhd_characteristic_list.py"

rule nldas_to_zarr_store:
    params:
        zarr_store = nldas_zarr_store,
        netrc_file = 'C:/Users/jsadler/.netrc'
    output:
        rules.all.input.nldas_indicator
    script:
        "scripts/pull_nldas.py"

rule make_nwis_sites_list_dv:
    input:
        f"{data_dir}streamflow_data/all_daily_discharge.parquet"
    output:
        "data/tables/nwis_site_list_dv.csv"
        # rules.all.input.nwis_site_list
    run:
        su.make_nwis_sites_list(input[0], output[0])

rule us_comids_nwis_network:
    input:
        rules.all.input.nwis_comid_table
    output:
        f"{data_dir}nwis_network/upstream_comids_cln.csv"
    run:
        gt.get_upstream_comids_all(output[0])

rule intermediate_nwis_comids:
    input:
        rules.us_comids_nwis_network.output
    output:
        f"{data_dir}nwis_network/intermediate_comids.csv"
    run:
        gt.filter_intermediate(input[0], output[0])


rule make_zero_buffer_catchments:
    input:
        nhd_gdb
    output:
        "D:/nhd/catchments/catchment_buffer_zero.gpkg",
    run:
        gt.make_zero_buffer_catchments(input[0], output[0])

rule dissolve_nwis_network:
    input:
        rules.make_zero_buffer_catchments.output,
        rules.intermediate_nwis_comids.output
    output:
        rules.all.input.nwis_network_file
    run:
        gdf = gpd.read_file(input[0])
        print('have read in file')
        gt.dissolve_intermediate(input[1], gdf, output[0])

rule make_sample_netcdf:
    input:
        netrc_file="C:/users/jsadler/.netrc"
    output:
        f'{data_dir}weight_grid/sample_nldas.nc'
    run:
        wt.make_example_nc(input[0], output[0])

rule make_grid_num_nc:
    input:
        rules.make_sample_netcdf.output
    output:
        f'{data_dir}weight_grid/grid_num.nc'
    run:
        wt.create_grid_num_nc(input[0], output[0])

rule convert_nc_to_geotiff:
    input:
        rules.make_grid_num_nc.output
    output:
        f'{data_dir}weight_grid/grid_num.tif'
    run:
        wt.nc_to_tif(input[0], output[0])

rule convert_tif_to_polygon:
    input:
        rules.convert_nc_to_geotiff.output
    output:
        f'{data_dir}weight_grid/grid_num.json'
    run:
        wt.tif_to_polygon(input[0], output[0])

rule project_grid_vector_to_5070:
    input:
        rules.convert_tif_to_polygon.output
    output:
        f'{data_dir}weight_grid/grid_num_proj.json'
    run:
        wt.project_grid_vector(input[0], output[0], 5070)

# todo: add rule for making the "clean" version of the dissolved nwis. 
# I originally did this using a gui function in QGIS

rule weight_matrix_nwis_net:
    input:
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

rule combine_filtered_nhd_attributes:
    input:
        rules.get_nhd_characteristic_subset_list.output
    output:
        rules.all.input.nhd_filtered_values
    run:
        filter_combine_nhd_files(output[0])

rule consolidate_nhd_attr_to_nwis_net:
    input:
        rules.intermediate_nwis_comids.output,
        rules.combine_filtered_nhd_attributes.output
    output:
        rules.all.input.nhd_nwis_net
    run:
        combine_attr_to_nwis_net(input[0], input[1], output[0])

rule combine_all_nhd_attributes:
    output:
        f"{data_dir}nhd_cat_attr/nhd_all_cat_attr.parquet"
    run:
        filter_combine_nhd_files(output[0], filtered=False)

rule nwis_nhd:
    input:
        nhd_gdb
    output:
        f"{data_dir}nwis_network/nhd_nwis_gages.json"
    run:
        gt.get_nhd_gages_in_nwis(input[0], output[0])
