import get_gauge_network_comids as gt
import pandas as pd

toy_sample_csv = 'sample_us_comid_file.csv'
toy_sample_df = pd.read_csv(toy_sample_csv, index_col='comid')

real_sample_csv = 'gauge_network_catchments/us_comids-01474500.csv'
real_sample_df = pd.read_csv(real_sample_csv, index_col='comid')


def test_get_us_comids():
    sample_nwis_comid = '4781847'
    true_us_comids = ['4781847', '4781845', '4782739', '4781853', '4781857',
                      '4781859'] 
    true_sort = sorted(true_us_comids)
    us_comids = gt.get_upstream_comid_list(sample_nwis_comid)
    us_sort = sorted(us_comids)
    print(us_sort)
    assert true_sort == us_sort


def test_get_nwis_us():
    sample_nwis_comid = 4782017
    true_us_nwis_comids = [4783213, 4783303, 4783101]
    srt_true = sorted(true_us_nwis_comids)
    all_comids_file = 'gauge_network_catchments/us_comids-01474500.csv'
    all_comids_df = pd.read_csv(all_comids_file, dtype={'comid':int})
    all_comids_df.set_index('comid', inplace=True)

    us_nwis_comids = gt.get_us_nwis_comids(sample_nwis_comid, all_comids_df)
    us_nwis_list = us_nwis_comids.to_list()
    assert sorted(us_nwis_list) == srt_true


def test_combine_toy():
    combine_idx = ['a', 'd', 'g']
    true_combined = ['a', 'b', 'c', 'd', 'e', 'f', 'd', 'e', 'f', 'h', 'i']

    combined = gt.combine_us_comids(combine_idx, toy_sample_df)
    assert true_combined == combined


def test_combine_real():
    combine_idx = [4782005, 4780759]
    true_combined = [4782005, 4782003, 4780617, 4780759, 4780705, 4780779,
                     4780677, 4780701]
    true_combined = [str(c) for c in true_combined]

    combined = gt.combine_us_comids(combine_idx, real_sample_df)
    assert sorted(true_combined) == sorted(combined)


