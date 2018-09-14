######################################################################################
# This python script is called after a simulation if the
# post-processing argument is set to 'y' when calling
# start_simulation.sh. You need to adjust this script
# to perform analysis which is relevant for your simulation.
#
# This script is called with an argument 'filebasename'
# which is the first part of a path to a result-file,
# except the run-index or filetype. That is 'filebasename'
# could be for example 'path/to/result/folder/2018-09-12T:12:00:00CEST_key_5'.
# The task of this script is then to find the result-files that
# which path starts like this and analyse these. This could for example be the files:
#  *   'path/to/result/folder/2018-09-12T:12:00:00CEST_key_5_run_0.db'
#  *   'path/to/result/folder/2018-09-12T:12:00:00CEST_key_5_run_1.db'
#  *   'path/to/result/folder/2018-09-12T:12:00:00CEST_key_5_run_2.db'
# a CSV file containing the keys from a given paramset.csv file
# without the runindices. This is such that post-processing
# can be done for different keys in parallell on the cluster.
#
# Author: Axel Dahlberg
######################################################################################

import sys
from easysquid.toolbox import get_file_paths

# Specific import for this simulation
from simulations.analysis_sql_data import analyse_single_file


def main(filebasename):
    """
    Here you should define what analysis you wish to do.
    :param filebasename:
    :return: None
    """
    # What do we expect the file names to end with?
    # This should be changed if you use a different
    # file type for your simulations.
    endswith = ".db"

    # Get the file paths
    file_paths = get_file_paths(filebasename=filebasename, endswith=endswith)

    # Here we call an analysis script, this should be changed to what is relevant
    # for your simulation
    analysis_folder = filebasename + "_analysis"
    for file_path in file_paths:
        analyse_single_file(file_path, no_plot=True, save_figs=True, save_output=True, analysis_folder=analysis_folder)


if __name__ == '__main__':
    filebasename = sys.argv[1]
    main(filebasename=filebasename)
