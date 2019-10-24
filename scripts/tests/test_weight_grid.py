import weight_grid_nldas as wt
import pandas as pd

col1 = ['0', '12', '3']
col2 = ['0', '11', '3']
idx1 = [1, 2, 3]
idx2 = [4, 5, 6]
df1 = pd.DataFrame([[0, 1, 2], [3, 4, 5], [6, 7, 8]], columns=col1, index=idx1)
df2 = pd.DataFrame([[0, 1, 2], [3, 4, 5], [6, 7, 8]], columns=col2, index=idx2)
df_list = [df1, df2]

combined_cols = pd.Index(['0', '3', '11', '12'])
combined_idx = pd.Index(list(range(1, 7)), dtype='uint8')


def test_get_all_cols():
    all_cols = wt.get_all_col_or_idx(df_list, 'col')
    assert all_cols.equals(combined_cols)

    all_idx = wt.get_all_col_or_idx(df_list, 'index')
    assert all_idx.equals(combined_idx)


def test_create_placeholder():
    df = wt.create_placeholder_df(df_list)
    true_df = pd.DataFrame(0, columns=combined_cols, index=combined_idx,
                           dtype='uint8')
    assert true_df.equals(df)
    assert df['0'].dtype == 'uint8'


def test_combine():
    pl = wt.create_placeholder_df(df_list)
    combined_data = [[0, 2, 0, 1], [3, 5, 0, 4], [6, 8, 0, 7],
                     [0, 2, 1, 0], [3, 5, 4, 0], [6, 8, 7, 0]]
    true_df = pd.DataFrame(combined_data, index=combined_idx,
                           columns=combined_cols, dtype='uint8')
    combine_df = wt.combine_dfs_into_placeholder(pl, df_list)
    assert true_df.equals(combine_df)


def test_resize_individual():
    pl = wt.create_placeholder_df(df_list)
    resized = wt.resize_individual_df(pl, df1)
    assert resized.shape[0] == pl.shape[0]
    assert resized.shape[1] == pl.shape[1]
    assert resized.iloc[0].dtype == pl.iloc[0].dtype

