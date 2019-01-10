import os
import json
import sys
from simulations.analysis_sql_data import get_data


def main(results_path):
    data_dct = get_data(results_path)
    data_filename = os.path.join(results_path, "all_data.json")

    with open(data_filename, 'w') as f:
        json.dump(data_dct, f, indent=4)


if __name__ == '__main__':
    results_path = sys.argv[1]
    main(results_path)
