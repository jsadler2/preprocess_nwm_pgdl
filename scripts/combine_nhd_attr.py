import os
import pandas as pd


def get_all_feather_files():
    """
    get a list of the feather files from a folder that has a bunch of feather
    files that contain the "cat" nhd attributes ("cat" meaning that the
    attributes are for the individual catchment and not accumulated)
    :return: list of feather files
    """
    feather_dir = "D:/nhd/feather"
    feather_files = []
    for path, subdirs, files in os.walk(feather_dir):
        for f in files:
            if f.endswith('cat.feather'):
                f_path = os.path.join(path, f)
                feather_files.append(f_path)
    return feather_files


def read_nhd_categories():
    """
    read the nhd categories to include/exclude
    """
    nhd_category_file = '../data/tables/nhd_categories_filtered.csv'
    df = pd.read_csv(nhd_categories_filtered)
    nhd_cats = df['ID'].to_list()
    return nhd_cats


def get_blank_df(feather_file):
    df = pd.read_feather(feather_file)
    df_blank = pd.DataFrame(index=df['COMID'])
    return df_blank


def get_categories_in_feather(nhd_cats, columns):
    """
    get any categories in the feather file
    """
    in_cats = columns[columns.isin(nhd_cats)]
    return in_cats


def combine_nhd_files():
    """
    """
    feather_files = get_all_feather_files()
    nhd_cats = read_nhd_categories()
    df_blank = get_blank_df(feather_files[0])
    for feather_file in feather_files:
        df = pd.read_feather(feather_file)
        cols = get_categories_in_feather(nhd_cats, df.columns)
        if len(cols) > 0:
            df_blank[cols] = df[cols]
    return df_blank

