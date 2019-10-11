PRODUCTS = ['iv', 'dv']
HUCS = [f'{h:02}' for h in range (1, 19)]

workdir: "scripts/"

rule get_all_sites:
    input:
        "get_all_streamflow_sites.py"
    output:
        expand("data/all_streamflow_sites_CONUS_{product}.csv", product=PRODUCTS)
    shell:
        "python {input}"

rule get_daily_discharge:
    input:
        "get_daily_basin_data.py", rules.get_all_sites.output
    output:
        expand("data/daily_discharge/indicator_{huc}.txt", huc=HUCS)
    shell:
        "python {input[0]}"


