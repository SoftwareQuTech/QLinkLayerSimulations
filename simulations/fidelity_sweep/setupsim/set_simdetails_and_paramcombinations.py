import os

import numpy as np

from qlinklayer.feu import estimate_success_probability, get_assigned_brigh_state_population

#######################
# Mandatory paramaters
#######################

description = "Simulation of EGP under varying request frequencies"
num_runs = 1
sim_name = "fidelity_sweep"

constant_params = {
    "max_sim_time": 0,
    # "max_wall_time": 24 * 60 * 60 - 2 * 60,
    "max_wall_time": 10 * 60,
    "max_mhp_cycle": 10,
    "t0": 0,
    "enable_pdb": False,
    "wall_time_per_timestep": 1 * 60,
    "save_additional_data": True,
    "collect_queue_data": True,
    "request_cycle": 0,
    "alphaA": list(np.linspace(0, 0.5, 100)),
    "alphaB": list(np.linspace(0, 0.5, 100)),
    "log_to_file": True,
    "log_level": 10,
    "filter_debug_logging": True,
    "log_to_console": False
}


#########################
# Optional parameters
#########################
def single_type_request_params(type, config_file, origin_prob, p_fraction, num_pairs, fidelity=0.8):
    if type == "NL":
        params = {"num_pairs": num_pairs,
                  "tmax_pair": 0,
                  "min_fidelity": fidelity,
                  "purpose_id": 0,
                  "priority": 0,
                  "store": True,
                  "atomic": False,
                  "measure_directly": False
                  }
    elif type == "CK":
        params = {"num_pairs": num_pairs,
                  "tmax_pair": 0,
                  "min_fidelity": fidelity,
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
                  "min_fidelity": fidelity,
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
p_fraction = 0.99
p_fraction_name = "high"
for type in ["MD", "NL"]:
    for min_fid, min_fid_name in zip([0.6, 0.65, 0.7, 0.75, 0.8, 0.85], ["060", "065", "070", "075", "080", "085"]):
        request_paramsA = {type: single_type_request_params(type, config_file_path, origin_prob, p_fraction, num_pairs, fidelity=min_fid)}
        request_paramsB = {type: single_type_request_params(type, config_file_path, origin_prob, p_fraction, num_pairs, fidelity=min_fid)}

        simulation_run_params = {"config": config_file_path,
                                 "request_paramsA": request_paramsA,
                                 "request_paramsB": request_paramsB,
                                 "egp_queue_weights": egp_queue_weights,
                                 "num_priorities": num_priorities}

        simulation_run_params.update(constant_params)
        run_name = "QL2020_{}_req_frac_{}_min_fid_{}_FCFS".format(type, p_fraction_name, min_fid_name)
        paramcombinations[run_name] = simulation_run_params

print(len(paramcombinations) * num_runs)

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

