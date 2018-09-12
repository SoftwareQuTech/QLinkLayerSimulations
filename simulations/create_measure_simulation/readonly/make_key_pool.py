################################################################
# This python script is simple used to construct
# a CSV file containing the keys from a given paramset.csv file
# without the runindices. This is such that post-processing
# can be done for different keys in parallell on the cluster.
#
# Author: Axel Dahlberg
################################################################

import sys

def main(paramsetfile, tmp_key_file):
    """
    Creates a CSV file with the name 'tmp_key_file' which
    lines is the first column of paramsetfile (without repetitions)
    :param paramsetfile: str
    :param tmp_key_file: str
    :return: None
    """
    keys = []
    with open(paramsetfile, 'r') as f:
        for line in f.readlines():
            key = line.split(' ')[0]
            if key not in keys:
                keys.append(key)
    with open(tmp_key_file, 'w') as f:
        for key in keys:
            f.write(key + '\n')

if __name__ == '__main__':
    paramsetfile = sys.argv[1]
    tmp_key_file = sys.argv[2]
    main(paramsetfile=paramsetfile, tmp_key_file=tmp_key_file)