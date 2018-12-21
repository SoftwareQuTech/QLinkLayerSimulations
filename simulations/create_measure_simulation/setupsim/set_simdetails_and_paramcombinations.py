import os

import qlinklayer
import SimulaQron.cqc

#######################
# Mandatory paramaters
#######################

description_string = "Simulation of EGP under CREATE+measure scenario"
number_of_runs = 1
outputdirname = "CREATE_and_measure"

# Get paths to QLinkLayer and SimulaQron folders
path_to_qlinklayer___init__ = os.path.abspath(qlinklayer.__file__)
path_to_qlinklayer = "/".join(path_to_qlinklayer___init__.split("/")[:-2])

path_to_cqc___init__ = os.path.abspath(SimulaQron.cqc.__file__)
path_to_SimulaQron = "/".join(path_to_cqc___init__.split("/")[:-2])

#########################
# Optional parameters
#########################

config_dir = "setupsim/config"
# config_files=[]
# for root, dirs, files in os.walk(qlinklayer_directory + config_dir):
#     for filename in files:
#         config_files.append(root + "/" + filename)

# Create a dictionary with the config files as keys and their corresponding success probabilities as values
# This will be used to simulate situations where the request probability is slightly lower than the
# success probability and also significantly lower.

config_to_p_succ = {
    "no_noise/no_losses.json": 0.18962460137276416,
    "no_noise/no_noise.json": 0.19,
    "lab/networks_no_cavity_no_conversion.json": 7.015991568047906e-05,
    "lab/networks_no_cavity_with_conversion.json": 2.1629224382238053e-05,
    "lab/networks_with_cavity_no_conversion.json": 0.0011008895034067229,
    "lab/networks_with_cavity_with_conversion.json": 0.00033126325807618113,
    "qlink/networks_no_cavity_no_conversion.json": 7.999995199289722e-07,
    "qlink/networks_no_cavity_with_conversion.json": 5.944852544500876e-06,
    "qlink/networks_with_cavity_no_conversion.json": 7.999995199289722e-07,
    "qlink/networks_with_cavity_with_conversion.json": 8.243310647958359e-05
}

# Create a dictionary that relates the name of the simulation to the config file and req freq factor
name_to_scenario = {
    # "NoNoise_NC_NC_LRF": ("no_noise/no_noise.json", 0.2),
    # "NoNoise_NC_NC_HRF": ("no_noise/no_noise.json", 0.8)
    # "NoLoss_NC_NC_LRF": ("no_noise/no_losses.json", 0.2),
    # "NoLoss_NC_NC_HRF": ("no_noise/no_losses.json", 0.8),
    # "Lab_NC_NC_LRF": ("lab/networks_no_cavity_no_conversion.json", 0.2),
    # "Lab_NC_NC_HRF": ("lab/networks_no_cavity_no_conversion.json", 0.8)
    # "Lab_NC_WC_LRF": ("lab/networks_no_cavity_with_conversion.json", 0.2),
    # "Lab_NC_WC_HRF": ("lab/networks_no_cavity_with_conversion.json", 0.8),
    # "Lab_WC_NC_LRF": ("lab/networks_with_cavity_no_conversion.json", 0.2),
    # "Lab_WC_NC_HRF": ("lab/networks_with_cavity_no_conversion.json", 0.8),
    # "Lab_WC_WC_LRF": ("lab/networks_with_cavity_with_conversion.json", 0.2),
    # "Lab_WC_WC_HRF": ("lab/networks_with_cavity_with_conversion.json", 0.8),
    # "QLink_NC_NC_LRF": ("qlink/networks_no_cavity_no_conversion.json", 0.2),
    # "QLink_NC_NC_HRF": ("qlink/networks_no_cavity_no_conversion.json", 0.8),
    # "QLink_NC_WC_LRF": ("qlink/networks_no_cavity_with_conversion.json", 0.2),
    # "QLink_NC_WC_HRF": ("qlink/networks_no_cavity_with_conversion.json", 0.8),
    # "QLink_WC_NC_LRF": ("qlink/networks_with_cavity_no_conversion.json", 0.2),
    # "QLink_WC_NC_HRF": ("qlink/networks_with_cavity_no_conversion.json", 0.8),
    # "QLink_WC_WC_LRF": ("qlink/networks_with_cavity_with_conversion.json", 0.2),
    "QLink_WC_WC_HRF": ("qlink/networks_with_cavity_with_conversion.json", 0.8)
}

# create paramcombinations

min_pairs = 1
max_pairs = 1
tmax_pair = 0
num_requests = 0
measure_directly = False

opt_params = {
    "request_cycle": 0,
    "max_sim_time": 0,
    "max_wall_time": 4 * 24 * 3600,
    "max_mhp_cycle": 1000000,
    "enable_pdb": False,
    "alphaA": 0.1,
    "alphaB": 0.1,
    "t0": 0,
    "wall_time_per_timestep": 1 * 10,
    "save_additional_data": True,
    "collect_queue_data": True}

paramcombinations = {}
# create paramcombinations
for name, scenario in name_to_scenario.items():
    config_file = scenario[0]
    freq_req_factor = scenario[1]
    param_set = {}
    param_set.update(opt_params)
    param_set["config"] = config_dir + "/" + config_file
    p_succ = config_to_p_succ[config_file]
    params = {"num_pairs": [min_pairs, max_pairs],
               "tmax_pair": tmax_pair,
               "min_fidelity": 0.8,
               "purpose_id": 0,
               "priority": 0,
               "store": False,
               "atomic": False,
               "measure_directly": measure_directly}
    request_paramsA = {"reqs": {"prob": freq_req_factor * p_succ,
                                "number_request": num_requests,
                                "params": params}}
    request_paramsB = {"reqs": {"prob": 0,
                                "number_request": 0,
                                "params": params}}
    param_set["request_paramsA"] = request_paramsA
    param_set["request_paramsB"] = request_paramsB
    paramcombinations[name] = param_set

################################################################
#           BELOW HERE SHOULD NOT BE CHANGED                   #
################################################################

import os
import importlib.util


def main(ask_for_input=True):
    abspath_to_this_file = os.path.abspath(__file__)
    abspath_to_create_file = "/".join(
        abspath_to_this_file.split("/")[:-2]) + "/readonly/create_simdetails_and_paramcombinations.py"

    # Load the functions from the file ../readyonly/create_simdetails_and_paramcombinations.py
    spec = importlib.util.spec_from_file_location("module.name", abspath_to_create_file)
    create_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(create_module)
    try:
        paramcombinations
    except NameError:
        create_module.setup_sim_parameters(opt_params, description_string, number_of_runs, outputdirname,
                                           make_paramcombinations=True, ask_for_input=ask_for_input,
                                           QLINKLAYERDIR=path_to_qlinklayer,
                                           SIMULAQRONDIR=path_to_SimulaQron)
        return

    create_module.setup_sim_parameters(paramcombinations, description_string, number_of_runs, outputdirname,
                                       make_paramcombinations=False, ask_for_input=ask_for_input,
                                       QLINKLAYERDIR=path_to_qlinklayer,
                                       SIMULAQRONDIR=path_to_SimulaQron)


if __name__ == '__main__':
    main()
