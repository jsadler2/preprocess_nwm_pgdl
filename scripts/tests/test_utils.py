import utils as ut
import random
import pandas as pd
import xarray as xr
import numpy as np


def test_convert_df_to_ds():
    dates = pd.date_range(start='2019-01-01', end='2019-01-31')
    columns = np.random.randn(10)*100
    df = pd.DataFrame(1, index=dates, columns=columns)
    data_name = 'my_data'
    col_name = 'my_col'
    idx_name = 'my_idx'
    ds = ut.convert_df_to_dataset(df, col_name, idx_name, data_name)
    assert len(ds.data_vars) == 1
    assert list(dict(ds.data_vars.variables).keys())[0] == data_name
    assert len(ds.coords) == 2
    assert idx_name in list(ds.coords._names)
    assert col_name in list(ds.coords._names)
    assert len(ds[col_name]) == 10
    assert len(ds[idx_name]) == 31



