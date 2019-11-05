import os
import get_gauge_network_comids as gt
import pandas as pd

toy_sample_csv_num = 'sample_us_comid_file_numbers.csv'
toy_sample_df_num = pd.read_csv(toy_sample_csv_num, index_col='comid')

real_sample_csv = '../../data/test/gauge_network_catchments/' \
                  'us_comids-01474500.csv'
real_sample_df = pd.read_csv(real_sample_csv, index_col='comid')


def test_get_us_comids():
    sample_nwis_comid = '4781847'
    true_us_comids = [4781847, 4781845, 4782739, 4781853, 4781857, 4781859]
    true_sort = sorted(true_us_comids)
    us_comids = gt.get_upstream_comid_list(sample_nwis_comid)
    us_sort = sorted(us_comids)
    print(us_sort)
    assert true_sort == us_sort


def test_get_nwis_us():
    sample_nwis_comid = 4782017
    true_us_nwis_comids = [4783213, 4783303, 4783101]
    srt_true = sorted(true_us_nwis_comids)

    us_nwis_comids = gt.get_us_nwis_comids(sample_nwis_comid, real_sample_df)
    us_nwis_list = us_nwis_comids.to_list()
    assert sorted(us_nwis_list) == srt_true


def test_combine_toy():
    combine_idx = [5, 7, 4]
    true_combined = pd.Series([5, 6, 7, 8, 7, 8, 4])
    combined = gt.combine_us_comids(combine_idx, toy_sample_df_num)
    assert true_combined.equals(combined)


def test_combine_real():
    combine_idx = [4782005, 4780759]
    true_combined = [4782005, 4782003, 4780617, 4780759, 4780705, 4780779,
                     4780677, 4780701]
    true_combined = pd.Series(true_combined)

    combined = gt.combine_us_comids(combine_idx, real_sample_df)
    assert true_combined.equals(combined)


def test_intermediate_toy_multiple():
    intermediate = gt.get_intermediate_comids(1, toy_sample_df_num)
    true_intermediate = [1, 2, 3]
    assert sorted(true_intermediate) == sorted(intermediate)


def test_intermediate_toy_single():
    intermediate = gt.get_intermediate_comids(4, toy_sample_df_num)
    true_intermediate = [4]
    assert sorted(true_intermediate) == sorted(intermediate)


def test_intermediate_all_toy_num():
    true_data = [
        {'comid': 1,
         'intermediate_comids': [1, 2, 3]},
        {'comid': 5,
         'intermediate_comids': [5, 6]},
        {'comid': 7,
         'intermediate_comids': [7, 8]},
        {'comid': 11,
         'intermediate_comids': [11, 12, 13]},
        {'comid': 4,
         'intermediate_comids': [4]},
        {'comid': 9,
         'intermediate_comids': [9, 10]},
    ]
    true_int_df = pd.DataFrame(true_data)
    int_df = gt.filter_intermediate(toy_sample_csv_num, 'temp.csv')
    os.remove('temp.csv')
    assert true_int_df.equals(int_df)
