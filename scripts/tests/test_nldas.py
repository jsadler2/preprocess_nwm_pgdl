import shutil
import os
from pull_nldas import delete_last_time_chunk
import xarray as xr


def test_delete_last_time_chunk():
    orig_zarr_store = 'E:/data/nldas/nldas2_c'
    # tmp_zarr_store = 'data/sm_nldas_tmp'
    # shutil.copytree(orig_zarr_store, tmp_zarr_store)
    ds_orig = xr.open_zarr(orig_zarr_store)
    delete_last_time_chunk(orig_zarr_store)
    ds_del = xr.open_zarr(orig_zarr_store)

    orig_time_chunk_size = ds_orig.spfh2m.chunks[0][0]
    del_time_chunk_size = ds_del.spfh2m.chunks[0][0]

    assert orig_time_chunk_size == del_time_chunk_size
    orig_time_size = ds_orig.spfh2m.shape[0]
    new_true_size = orig_time_size - orig_time_chunk_size
    assert ds_del.spfh2m.shape[0] == new_true_size 
    assert ds_del.time.shape[0] == new_true_size 
    assert ds_del.apcpsfc.shape[0] == new_true_size 

