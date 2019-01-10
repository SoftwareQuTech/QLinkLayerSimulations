import json
import sys

def main(all_data_file):
    with open(all_data_file, 'r') as f:
        data_dct = json.load(f)

    for key, data in data_dct.items():
        num_succ = len(data["gen_attempts"]["0"])
        to_print = "key {} had {} successes".format(key, num_succ)
        if "fidelities" in data:
            fids = data["fidelities"]
            avg_fid = sum(fids)/len(fids)
            to_print += "with avg_fid = {}".format(avg_fid)
        elif "Z_data" in data:
            avg_X_err = data["X_data"][0]
            avg_Y_err = data["Y_data"][0]
            avg_Z_err = data["Z_data"][0]
            to_print += "quberr = ({}, {}, {})".format(avg_X_err, avg_Y_err, avg_Z_err)
        print(to_print)

if __name__ == '__main__':
    all_data_file = sys.argv[1]
    main(all_data_file)
