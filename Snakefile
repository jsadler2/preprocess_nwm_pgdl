PRODUCTS = ['iv', 'dv']
HUCS = [f'{h:02}' for h in range (1, 19)]

rule get_all_sites:
    output:
        expand("data/all_streamflow_sites_CONUS_{product}.csv", product=PRODUCTS)
    shell:
        "python scripts/get_all_streamflow_sites.py"

rule get_daily_discharge:
    output:
        expand("data/daily_discharge/indicator_{huc}.txt", huc=HUCS)
    shell:
        "python scripts/get_daily_basin_data.py"

