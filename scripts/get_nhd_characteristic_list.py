import pandas as pd
import yaml


def remove_items(metadata_df, items_to_remove, col_name):
    """
    remove items from the dataframe. could be themes or ids
    :param metadata_df: [dataframe] the complete metadata df
    :param items_to_remove: [list of strings] the items to remove
    :param col_name: [str] usually 'themeLabel' or 'ID"
    :return: [dataframe] filtered dataframe
    """
    for item in items_to_remove:
        if col_name == 'ID':
            item = "CAT_" + item
        mask = metadata_df[col_name].str.startswith(item)
        metadata_df = metadata_df[~mask]
    return metadata_df


def exclude_categories_nhd_metadata(metadata_file, exclude_file, output_file):
    orig_df = pd.read_csv(metadata_file)
    with open(exclude_file, 'r') as f:
        exclude_data = yaml.safe_load(f)

    # remove all the non-cat ones
    only_cat = orig_df[orig_df['ID'].str.startswith('CAT')]

    # remove IDS
    filtered_ids = remove_items(only_cat, exclude_data['IDS_EXCLUDE'].split(),
                                'ID')

    # remove theme labels
    filtered_themes = remove_items(filtered_ids,
                                   exclude_data['THEMES_EXCLUDE'].split(),
                                   'themeLabel'
                                   )

    # add back in anything in the include ids
    include_df = orig_df[orig_df['ID'].isin(
        exclude_data['IDS_INCLUDE'].split())]
    final_df = pd.concat([filtered_themes, include_df], axis=0)
    final_df.to_csv(output_file, index=False)


if __name__ == '__main__':
    exclude_list = snakemake.input.exclude_file
    metadata = snakemake.input.metadata_file
    output = snakemake.output[0]
    exclude_categories_nhd_metadata(metadata, exclude_list, output)

