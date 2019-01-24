import os

import qlinklayer
import SimulaQron.cqc
from qlinklayer.feu import estimate_success_probability, get_assigned_brigh_state_population

#######################
# Mandatory paramaters
#######################

description_string = "Simulation of EGP under varying request frequencies"
number_of_runs = 40
outputdirname = "req_freq_sweep"

# Get paths to QLinkLayer and SimulaQron folders
path_to_qlinklayer___init__ = os.path.abspath(qlinklayer.__file__)
path_to_qlinklayer = "/".join(path_to_qlinklayer___init__.split("/")[:-2])

path_to_cqc___init__ = os.path.abspath(SimulaQron.cqc.__file__)
path_to_SimulaQron = "/".join(path_to_cqc___init__.split("/")[:-2])

#########################
# Optional parameters
#########################
def single_type_request_params(type, config_file, origin_prob, p_fraction, num_pairs):
    if type == "NL":
        params = {"num_pairs": num_pairs,
                  "tmax_pair": 0,
                  "min_fidelity": 0.8,
                  "purpose_id": 0,
                  "priority": 0,
                  "store": True,
                  "atomic": False,
                  "measure_directly": False
                  }
    elif type == "CK":
        params = {"num_pairs": num_pairs,
                  "tmax_pair": 0,
                  "min_fidelity": 0.8,
                  "purpose_id": 0,
                  "priority": 1,
                  "store": True,
                  "atomic": True,
                  "measure_directly": False
                  }
    elif type == "MD":
        params = {"num_pairs": num_pairs,
                  "tmax_pair": 0,
                  # "min_fidelity": 0.88,
                  "min_fidelity": 0.8,
                  "purpose_id": 0,
                  "priority": 2,
                  "store": False,
                  "atomic": False,
                  "measure_directly": True
                  }
    else:
        raise ValueError("Unknown type")

    # Compute success probability
    alphaA = get_assigned_brigh_state_population(config_file, params["min_fidelity"],
                                                 allowed_alphasA=constant_params["alphaA"],
                                                 allowed_alphasB=constant_params["alphaB"],
                                                 nodeID=0)
    alphaB = get_assigned_brigh_state_population(config_file, params["min_fidelity"],
                                                 allowed_alphasA=constant_params["alphaA"],
                                                 allowed_alphasB=constant_params["alphaB"],
                                                 nodeID=1)
    p_succ = estimate_success_probability(config_file, alphaA=alphaA, alphaB=alphaB)

    req_prob = p_fraction * origin_prob * p_succ

    request_params = {"prob": req_prob,
                      "params": params
                      }

    return request_params

constant_params = {
    "max_sim_time": 0,
    "max_wall_time": 5 * 24 * 60 * 60 - 2 * 60,
    "max_mhp_cycle": 10000,
    "t0": 0,
    "enable_pdb": False,
    "wall_time_per_timestep": 5 * 60,
    "save_additional_data": True,
    "collect_queue_data": True,
    "request_cycle": 0,
    "alphaA": [0.05, 0.1, 0.3],
    "alphaB": [0.05, 0.1, 0.3]
}

config_dir = "setupsim/config"

configs = {"LAB_NC_NC": "lab/networks_no_cavity_no_conversion.json",
           "QLINK_WC_WC": "qlink/networks_with_cavity_with_conversion.json",
           # "QLINK_WC_WC_HIGH_C_LOSS": "qlink/networks_with_cavity_with_conversion_high_c_loss.json",
           }

config_file_path = os.path.join(config_dir, configs["QLINK_WC_WC"])
egp_queue_weights = [0]
num_priorities = 1

origin_prob = 1 / 2
num_pairs = [1, 3]
paramcombinations = {}
for type in ["MD", "NL"]:
    for p_fraction, p_fraction_name in zip([0.5, 0.6, 0.7, 0.8, 0.99], ["050", "060", "070", "080", "099"]):
        request_paramsA = single_type_request_params(type, config_file_path, origin_prob, p_fraction, num_pairs)
        request_paramsB = None

        simulation_run_params = {"config": config_file_path,
                                 "request_paramsA": request_paramsA,
                                 "request_paramsB": request_paramsB,
                                 "egp_queue_weights": egp_queue_weights,
                                 "num_priorities": num_priorities}

        simulation_run_params.update(constant_params)
        run_name = "QL2020_{}_req_frac_{}_FCFS".format(type, p_fraction_name)
        paramcombinations[run_name] = simulation_run_params

print(len(paramcombinations))

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
