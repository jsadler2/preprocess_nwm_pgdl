from streamflow_data_retrival import get_all_streamflow_data_for_huc2
from utils import write_indicator_file

hucs = snakemake.params.hucs
out_file_names = snakemake.params.out_data_files
indicator_file_name = snakemake.output
# snakemake.input[0] is the 'iv'
sites_file = snakemake.input[1]

i = 0
for huc in hucs:
    get_all_streamflow_data_for_huc2(huc, out_file_names[i], sites_file,
                                     num_sites_per_chunk=20, time_scale='D',
                                     output_format='csv')

    write_indicator_file(get_all_streamflow_data_for_huc2,
                         file_name=indicator_file_name[i])
    i += 1



