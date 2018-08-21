import json
import itertools
import os.path


#######################
# Mandatory paramaters
#######################

description_string = "Testing to setup a simulation"
easysquid_directory = "/Users/adahlberg/Documents/EasySquid/"  # full absolute path
netsquid_directory = "/Users/adahlberg//Documents/NetSquid/"  # full absolute path
number_of_runs = 1
outputdirname = "simulation_results"

#########################
# Optional parameters
#########################

opt_params = {"config": '/Users/adahlberg/Documents/QLinkLayer/simulations/template_simulation_setup/setupsim/config/lab_configs/network_with_cav_no_conv.json',
# opt_params = {"config": "",
              "origin_bias": 0,
              "create_prob": 1,
              "min_pairs": 1,
              "max_pairs": 1,
              "tmax_pair": 10000,
              "request_overlap": False,
              "request_freq": 1e-3,
              "num_requests": 10,
              "max_sim_time": 1,
              "max_wall_time": 10,
              "enable_pdb" : False
             }

################################################################
#           BELOW HERE SHOULD NOT BE CHANGED                   #
################################################################

#########################
# Output simdetails.json
#########################
general_params = {"easysquid_directory": easysquid_directory,
                  "netsquid_directory": netsquid_directory,
                  "description": description_string,
                  "number_of_runs": number_of_runs,
                  "outputdirname": outputdirname
                 }

if "number_of_runs" in list(general_params.keys()):
    assert(type(number_of_runs) == int)

# merging the two dictionaries
params = {"general_params": general_params,
          "opt_params": opt_params}


def save_to_json(data, filename):
    if os.path.isfile(filename):
        input(
"""
    About to overwrite {}.
    If this is fine with you, press enter.
    If not, then abort using CTRL+C""".format(filename))
    with open(filename, 'w') as simdetailsfile:
        json.dump(data, simdetailsfile, indent=4)


save_to_json(data=params, filename='simdetails.json')


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
save_to_json(data=paramcombinations, filename='paramcombinations.json')
