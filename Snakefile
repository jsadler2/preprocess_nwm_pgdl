HUCS = [f'{h:02}' for h in range (1, 19)]
indicator_dir = "data/indicators/"
data_dir = "E:\\data\\"

rule all:
    input:
        daily_discharge = expand("{indicator_dir}/daily_discharge_huc_{huc}.txt",
                                 huc=HUCS, indicator_dir=indicator_dir),
        nwis_comid_table = f"{data_dir}\\nwis_comids.csv",
        nhd_categories = "data/nhd_categories_filtered.csv"


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
        # the input is the sites 'iv' table
        rules.get_all_sites.output[0]
    output:
        rules.all.input.nwis_comid_table
    script:
        "scripts/nwis_comid.py"

rule get_nhd_characteristic_subset_list:
    input:
        metadata_file="data/raw/nhd_characteristics_metadata_table.csv",
        exclude_file="data/nhd_categories_to_exclude.yml"
    output:
        "data/nhd_categories_filtered.csv"
    script:
        "scripts/get_nhd_characteristic_list.py"
