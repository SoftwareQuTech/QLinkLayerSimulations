import json
import itertools
import os.path


#######################
# Mandatory paramaters
#######################

description_string = "A brief simulation test run"
easysquid_directory = "/Users/adahlberg/Documents/EasySquid/"  # full absolute path
netsquid_directory = "/Users/adahlberg//Documents/NetSquid/"  # full absolute path
outputdirname = "my_zoo"
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
