import os
import sys
import json
import itertools

import netsquid
import easysquid


######################################################################################
#
# This script is used to setup simulation details and parameters
# by either running the script ../setupsim/set_simdetails_and_paramcombinations.py
# or calling the function setup_sim_parameters directly.
#
######################################################################################

def get_sim_dir():
    path_to_here = os.path.dirname(os.path.abspath(__file__))
    sim_dir = "/".join(path_to_here.split("/")[:-1]) + "/"

    return sim_dir


def get_general_params(description_string, number_of_runs, outputdirname, other_gen_params=None):
    assert (type(number_of_runs) == int)

    # Get paths to easysquid and netsquid
    path_to_netsquid___init__ = os.path.abspath(netsquid.__file__)
    path_to_netsquid = "/".join(path_to_netsquid___init__.split("/")[:-2])

    path_to_easysquid___init__ = os.path.abspath(easysquid.__file__)
    path_to_easysquid = "/".join(path_to_easysquid___init__.split("/")[:-2])

    general_params = {"EASYSQUIDDIR": path_to_easysquid,
                      "NETSQUIDDIR": path_to_netsquid,
                      "DESCRIPTION": description_string,
                      "NUMRUNS": number_of_runs,
                      "OUTPUTDIRNAME": outputdirname
                      }
    if isinstance(other_gen_params, dict):
        general_params.update(other_gen_params)
    return general_params


def get_paramcombinations(opt_params):
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
    paramcombinations = {}
    counter = 0
    for parametertuple in itertools.product(*allparams):
        pardict = {}
        for keyindex, key in enumerate(list(opt_params.keys())):
            pardict[key] = parametertuple[keyindex]
        paramcombinations[counter] = pardict
        counter += 1

    return paramcombinations


def save_to_ini(data, filename, ask_for_input=True):
    if ask_for_input:
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


def save_to_json(data, filename, ask_for_input=True):
    if ask_for_input:
        if os.path.isfile(filename):
            input(
                """
                    About to overwrite {}.
                    If this is fine with you, press enter.
                    If not, then abort using CTRL+C""".format(filename))

    with open(filename, 'w') as simdetailsfile:
        json.dump(data, simdetailsfile, indent=4)


def save_to_csv(param_combinations_keys, nrruns, filename, ask_for_input=True):
    if ask_for_input:
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


def setup_sim_parameters(params, description_string, number_of_runs, outputdirname, make_paramcombinations=True,
                         ask_for_input=True, **other_gen_params):
    """
    This is the main function and should be called to setup the simulation details and parameters.
    Called by the script set_simdetails_and_paramcombinations.py

    Outputs files simdetails.ini, paramcombinations.json and paramset.csv

    :param make_paramcombinations: bool
        If True, then a cartesian product will be taken between the parameters to constuct a simulation with all
        parameter combinations.
        If False, it is assumed that 'params' is a dict of dict for example

         params = {
             "few_tigers_many_wildbeasts": {
                 "number_of_lions": 4,
                 "number_of_tigers": 2,
                 "number_of_remaining_wildebeasts": 5.0
             },
             "few_tigers_medium_wildbeasts": {
                 "number_of_lions": 4,
                 "number_of_tigers": 2,
                 "number_of_remaining_wildebeasts": 3.3
             },
             "many_tigers_medium_wildbeasts": {
                 "number_of_lions": 4,
                 "number_of_tigers": 3,
                 "number_of_remaining_wildebeasts": 3.3
             },
             "many_tigers_few_wildbeasts": {
                 "number_of_lions": 4,
                 "number_of_tigers": 3,
                 "number_of_remaining_wildebeasts": 2.5
             }
         }
    :param ask_for_input: bool
        If True, then you will be asked for confirmation before overwriting files, otherwise not.
    :return:
    """

    sim_dir = get_sim_dir()
    general_params = get_general_params(description_string, number_of_runs, outputdirname, other_gen_params)
    if make_paramcombinations:
        paramcombinations = get_paramcombinations(params)
    else:
        paramcombinations = params

    # write ini file with simdetails
    save_to_ini(data=general_params, filename=sim_dir + "setupsim/simdetails.ini", ask_for_input=ask_for_input)

    # write the combinations of parameters to a file
    save_to_json(data=paramcombinations, filename=sim_dir + 'setupsim/paramcombinations.json',
                 ask_for_input=ask_for_input)

    # Prepare CSV file for stopos
    save_to_csv(param_combinations_keys=paramcombinations.keys(), nrruns=number_of_runs,
                filename=sim_dir + "setupsim/paramset.csv", ask_for_input=ask_for_input)
