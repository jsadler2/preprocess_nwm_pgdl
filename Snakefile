import scripts.utils as su
from scripts.nwis_comid import get_comids_for_all_nwis_nhd
import scripts.get_gauge_network_comids as gt

HUCS = [f'{h:02}' for h in range (1, 19)]
indicator_dir = "data/indicators/"
data_dir = "D:\\nwm-ml-data\\"

nldas_zarr_store_type = 's3'
nldas_zarr_store = f"{data_dir}\\nldas\\nldas2"

rule all:
    input:
        daily_discharge = expand("{indicator_dir}daily_discharge_huc_{huc}.txt",
                                 huc=HUCS, indicator_dir=indicator_dir),
        nwis_comid_table = "data/tables/nwis_comid.csv",
        nhd_categories = "data/tables/nhd_categories_filtered.csv",
        nldas_indicator = f"{indicator_dir}/nldas_indicator_{nldas_zarr_store_type}.txt",
        nwis_site_list = "data/tables/nwis_site_list_dv.csv",
        nwis_network_file = f"{data_dir}/nwis_network/dissolved_nwis_network.json"

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
        out_data_files = expand(data_dir+"streamflow_data\\discharge_data_{huc}_daily.csv", huc=HUCS)
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
        rules.all.input.nhd_categories
    script:
        "scripts/get_nhd_characteristic_list.py"

rule nldas_to_zarr_store:
    params:
        zarr_store = nldas_zarr_store,
        netrc_file = 'C:\\Users\\jsadler\\.netrc'
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
        f"{data_dir}/nwis_network/upstream_comids.csv"
    run:
        gt.get_upstream_comids_all(output[0])

rule intermediate_nwis_comids:
    input:
        rules.us_comids_nwis_network.output
    output:
        f"{data_dir}/nwis_network/intermediate_comids.csv"
    run:
        gt.filter_intermediate(input[0], output[0])

rule dissolve_nwis_network:
    input:
        rules.intermediate_nwis_comids.output
    output:
        rules.all.input.nwis_network_file
    run:
        gt.dissolve_intermediate_all_conus()
