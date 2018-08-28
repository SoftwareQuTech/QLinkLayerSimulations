import json
import itertools
import os
import sys


#######################
# Mandatory paramaters
#######################

description_string = "A brief simulation test run"
easysquid_directory = "/home/dahlberg/EasySquid/"  # full absolute path
netsquid_directory = "/home/dahlberg/NetSQUID/"  # full absolute path
number_of_runs = 3
outputdirname = "zoo"


#########################
# Optional parameters
#########################

opt_params = {"number_of_lions": 4,
              "number_of_tigers": [2, 3],
              "number_of_remaining_wildebeasts": [5.0, 3.3, 2.5]
             }

################################################################
#           BELOW HERE SHOULD NOT BE CHANGED                   #
################################################################

sim_dir_env = "SIMULATION_DIR"

# Check that the simulation path is set
if not sim_dir_env in os.environ:
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
    assert(type(number_of_runs) == int)

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
            for _ in range(nrruns):
                simdetailsfile.write("{}\n".format(key))


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
save_to_csv(param_combinations_keys=paramcombinations.keys(), nrruns=number_of_runs, filename=sim_dir + "setupsim/paramset.csv")
