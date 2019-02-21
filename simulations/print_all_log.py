import os
from argparse import ArgumentParser


def main(results_folder):
    """
    This a very simple script to print warning and error messages from the log files of a simulation.
    Adjust the 'to_avoid' and 'to_include' variables to change which lines to include.
    :param results_folder: str
        The path to the results folder containing the log files (ending with log.out or log_reduced.out).
    :return: None
    """
    to_avoid = ["INFO", "FutureWarning", "from ._conv import register_converters"]
    to_include = ["WARNING", "ERROR"]
    for filename in os.listdir(results_folder):
        if filename.startswith(results_folder[:10]):
            if filename.endswith("log.out") or filename.endswith("log_reduced.out"):
                to_print = ""
                with open(os.path.join(results_folder, filename), 'r') as f:
                    for line in f.readlines():
                        for s in to_avoid:
                            if s in line:
                                break
                        else:
                            for s in to_include:
                                if s in line:
                                    to_print += line
                                    break
                if len(to_print) > 0:
                    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                    print(filename)
                    print(to_print[:-1])
                    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                    print("")
                    print("")
                    print("")
                    print("")
                    print("")
                    print("")


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--results_folder', required=True, type=str,
                        help="The path to the results folder to consider.")

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(**vars(args))
