import json
import sys
import os

def main(results_folder):
    to_avoid = ["INFO", "FutureWarning", "from ._conv import register_converters"]
    for filename in os.listdir(results_folder):
        if filename.startswith(results_folder[:10]) and filename.endswith("log.out"):
            to_print = ""
            with open(os.path.join(results_folder, filename), 'r') as f:
                for line in f.readlines():
                    for s in to_avoid:
                        if s in line:
                            break
                    else:
                        to_print += line
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

if __name__ == '__main__':
    results_folder = sys.argv[1]
    main(results_folder)