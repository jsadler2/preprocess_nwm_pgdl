import geopandas as gpd
from prefect import task

@task
def huc4_gdf(huc4file, huc4s, checkpoint=True):
    gdf_full = gpd.read_file(huc4file).set_index('huc4')
    gdf_subset = gdf_full.loc[huc4s]
    return gdf_subset

@task
def subset_nhd_catchments(nhdfile, gdf_mask):
    nhd_gdf = gpd.read_file(nhdfile, layer='Catchment', bbox=gdf_mask)
    nhd_subset = gpd.clip(nhd_gdf, gdf_mask)
    return nhd_subset


@task
def subset_ds(ds, gdf_mask, epsg=102009):
    gdf_mask_proj = gdf_mask.to_crs(epsg)
    minx, miny, maxx, maxy = gdf_mask.total_bounds
    ds_subset = ds.sel(lat=slice(miny, maxy), long=slice(minx, maxx))
    ds_subset = ds_subset.isel(time=slice(0, 10))
    return ds_subset

