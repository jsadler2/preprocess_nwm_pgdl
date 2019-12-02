# coding: utf-8
import xarray as xr


def apply_nldas_weight_grid(weight_grid_zarr, dataset_zarr, out_store):
    ds = xr.open_zarr(weight_grid_zarr)
    ds_nldas = xr.open_zarr(dataset_zarr)
    ds_nldas_st = ds_nldas.stack(nldas_grid_no=['lat', 'lon'])
    ds_nldas_st = ds_nldas_st.assign_coords(nldas_grid_no=range(len(ds.nldas_grid_no)))
    w = ds.weight
    data_dict = {}
    for var_name in ds_nldas_st._variables:
        if var_name not in ('time', 'nldas_grid_no'):
            var_array = ds_nldas_st[var_name]
            print(type(var_array))
            var_array = var_array.fillna(0)
            var_array = w.dot(var_array)
            data_dict[var_name] = var_array
    weigheted_ds = xr.Dataset(data_dict)
    weigheted_ds.to_zarr(out_store)

if __name__ == "__main__":
    weight_grid_store = "D:/nwm-ml-data/weight_grid/weight_grid_dissolved1"
    nldas_store = "D:/nwm-ml-data/nldas/nldas2"
    out_store = "D:/nwm-ml-data/weight_grid/dissolved_nldas"
    apply_nldas_weight_grid(weight_grid_store, nldas_store, out_store)

