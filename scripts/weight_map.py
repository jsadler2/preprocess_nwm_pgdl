from prefect import task
import xagg as xa

@task
def get_weight_map(ds, gdf):
    return xa.pixel_overlaps(ds, gdf)

@task
def get_aggregated_data(ds, weight_map):
    return xa.aggregate(ds, weight_map)
