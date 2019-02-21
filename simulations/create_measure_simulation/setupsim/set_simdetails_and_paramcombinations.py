#######################
# Mandatory paramaters
#######################

description = "Simulation of EGP under CREATE+measure scenario"
num_runs = 1
sim_name = "CREATE_and_measure"

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
    "NoLoss_NC_NC_HRF": ("no_noise/no_losses.json", 0.8)
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
    # "QLink_WC_WC_HRF": ("qlink/networks_with_cavity_with_conversion.json", 0.8)
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
    "max_mhp_cycle": 10000,
    "enable_pdb": False,
    "alphaA": [0.1, 0.3],
    "alphaB": [0.1, 0.3],
    "t0": 0,
    "wall_time_per_timestep": 1 * 1,
    "save_additional_data": True,
    "collect_queue_data": True,
    "log_to_file": True,
    "log_level": 10,
    "filter_debug_logging": True,
    "log_to_console": False
}

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
              "store": True,
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

print(len(paramcombinations))

################################################################
#           BELOW HERE SHOULD NOT BE CHANGED                   #
################################################################

import os
from easysquid.simulations import create_simdetails


def _get_sim_dir():
    path_to_here = os.path.dirname(os.path.abspath(__file__))
    sim_dir = os.path.split(path_to_here)[0]
    return sim_dir


def main(ask_for_input=True):
    """
    This function creates the collection of combinations of parameters that will
    be inputted into the simulation (`opt_params`), if these have not been provided
    already in the parameter `paramcombinations`. Subsequently produces several
    files that together contain all details for the simulations to run (to be precise:
    `simdetails.ini`, `paramcombinations.json` and `paramset.csv`).
    """
    sim_dir = _get_sim_dir()
    try:
        paramcombinations
    except NameError:
        create_simdetails.setup_sim_parameters(opt_params, sim_dir, description, num_runs, sim_name,
                                               make_paramcombinations=True, ask_for_input=ask_for_input)
        return

    create_simdetails.setup_sim_parameters(paramcombinations, sim_dir, description, num_runs, sim_name,
                                           make_paramcombinations=False, ask_for_input=ask_for_input)


if __name__ == '__main__':
    main()
