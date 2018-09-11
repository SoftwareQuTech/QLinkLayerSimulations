import json
import itertools
import os
import sys

#######################
# Mandatory paramaters
#######################

description_string = "Testing to setup a simulation"
easysquid_directory = "/Users/adahlberg/Documents/EasySquid/"  # full absolute path
netsquid_directory = "/Users/adahlberg/Documents/NetSQUID/"  # full absolute path
number_of_runs = 1
outputdirname = "simulation_results"

#########################
# Optional parameters
#########################

qlinklayer_directory = "/Users/adahlberg/Documents/QLinkLayer/"
config_dir = "simulations/template_simulation_setup/setupsim/config"
config_files = []
for root, dirs, files in os.walk(qlinklayer_directory + config_dir):
    for filename in files:
        config_files.append(root + "/" + filename)

opt_params = {
    "config": [config_files[0]],
    "origin_bias": 1,
    "create_prob": 1,
    "min_pairs": 1,
    "max_pairs": 1,
    "tmax_pair": 0,
    "request_overlap": True,
    "request_cycle": 0,
    "num_requests": 0,
    "max_sim_time": 0,
    "max_wall_time": 1 * 1 * 10,
    "max_mhp_cycle": 200000,
    "enable_pdb": False,
    "alphaA": 0.1,
    "alphaB": 0.1,
    "measure_directly": True,
    "t0": 0,
    "wall_time_per_timestep": 1,
    "save_additional_data": True,
    "collect_queue_data": True}

################################################################
#           BELOW HERE SHOULD NOT BE CHANGED                   #
################################################################

sim_dir_env = "SIMULATION_DIR"

# Check that the simulation path is set
if sim_dir_env not in os.environ:
    print("The environment variable {} must be set to the path to the simulation folder"
          "before running this script!".format(sim_dir_env))
    sys.exit()
else:
    sim_dir = os.getenv(sim_dir_env)
    if not os.path.isdir(sim_dir):
        print("The environment variable {} is not a path to a folder.")
        sys.exit()

# Check that sim_dir ends with '/'
if not sim_dir[-1] == '/':
    sim_dir += "/"

#########################
# Output simdetails.ini
#########################
general_params = {"EASYSQUIDDIR": easysquid_directory,
                  "NETSQUIDDIR": netsquid_directory,
                  "DESCRIPTION": description_string,
                  "NUMRUNS": number_of_runs,
                  "OUTPUTDIRNAME": outputdirname
                  }

if "number_of_runs" in list(general_params.keys()):
    assert (type(number_of_runs) == int)

# merging the two dictionaries
params = {"general_params": general_params,
          "opt_params": opt_params}


def save_to_ini(data, filename):
    if os.path.isfile(filename):
        input(
            """
                About to overwrite {}.
                If this is fine with you, press enter.
                If not, then abort using CTRL+C""".format(filename))
    with open(filename, 'w') as simdetailsfile:
        for key, value in data.items():
            if isinstance(value, str):
                simdetailsfile.write("{}=\"{}\"\n".format(key, value))
            else:
                simdetailsfile.write("{}={}\n".format(key, value))


def save_to_json(data, filename):
    if os.path.isfile(filename):
        input(
            """
                About to overwrite {}.
                If this is fine with you, press enter.
                If not, then abort using CTRL+C""".format(filename))
    with open(filename, 'w') as simdetailsfile:
        json.dump(data, simdetailsfile, indent=4)


def save_to_csv(param_combinations_keys, nrruns, filename):
    if os.path.isfile(filename):
        input(
            """
                About to overwrite {}.
                If this is fine with you, press enter.
                If not, then abort using CTRL+C""".format(filename))
    with open(filename, 'w') as simdetailsfile:
        for key in param_combinations_keys:
            for i in range(nrruns):
                simdetailsfile.write("{} {}\n".format(key, i))


save_to_ini(data=general_params, filename=sim_dir + "setupsim/simdetails.ini")

# make all parameters that were not a list (i.e. they consist
# of a single element only, into a list
allparams = []
for value in list(opt_params.values()):
    if isinstance(value, list):
        allparams.append(value)
    else:
        allparams.append([value])

# create a dictionary `paramcombinations` with keys integers
# and as values all possible combinations of the parameters
# (that is, `paramcombinations` is like the cartesian product
# of all parameter choices).
# First try if user already defined this dictionary
try:
    paramcombinations
except NameError:
    paramcombinations = {}
    counter = 0
    for parametertuple in itertools.product(*allparams):
        pardict = {}
        for keyindex, key in enumerate(list(opt_params.keys())):
            pardict[key] = parametertuple[keyindex]
        paramcombinations[counter] = pardict
        counter += 1

# write the cartesian product to a file
save_to_json(data=paramcombinations, filename=sim_dir + 'setupsim/paramcombinations.json')

# Prepare CSV file for stopos
save_to_csv(param_combinations_keys=paramcombinations.keys(), nrruns=number_of_runs,
            filename=sim_dir + "setupsim/paramset.csv")
