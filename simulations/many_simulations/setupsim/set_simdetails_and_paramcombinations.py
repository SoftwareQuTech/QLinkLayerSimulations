import os

import qlinklayer
from simulaqron.toolbox import get_simulaqron_path
from qlinklayer.feu import estimate_success_probability, get_assigned_brigh_state_population

#######################
# Mandatory paramaters
#######################

description_string = "Simulation of EGP under CREATE+measure scenario"
number_of_runs = 10
outputdirname = "CREATE_and_measure"

# Get paths to QLinkLayer and SimulaQron folders
path_to_qlinklayer___init__ = os.path.abspath(qlinklayer.__file__)
path_to_qlinklayer = "/".join(path_to_qlinklayer___init__.split("/")[:-2])

path_to_SimulaQron = get_simulaqron_path.main()

#########################
# Optional parameters
#########################

constant_params = {
    "max_sim_time": 0,
    "max_wall_time": 1 * 24 * 60 * 60 - 2 * 60,
    "max_mhp_cycle": 0,
    "t0": 0,
    "enable_pdb": False,
    "wall_time_per_timestep": 1 * 30,
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

for filename in os.listdir(os.path.join(config_dir, "qlink")):
    if "high_c_loss" in filename:
        p_loss = filename[-10:-5]
        configs["QLINK_WC_WC_HIGH_C_LOSS_{}".format(p_loss)] = "qlink/{}".format(filename)

# config_to_p_succ = {
#     "no_noise/no_losses.json": 0.18962460137276416,
#     "no_noise/no_noise.json": 0.19,
#     "lab/networks_no_cavity_no_conversion.json": 7.015991568047906e-05,
#     "lab/networks_no_cavity_with_conversion.json": 2.1629224382238053e-05,
#     "lab/networks_with_cavity_no_conversion.json": 0.0011008895034067229,
#     "lab/networks_with_cavity_with_conversion.json": 0.00033126325807618113,
#     "qlink/networks_no_cavity_no_conversion.json": 7.999995199289722e-07,
#     "qlink/networks_no_cavity_with_conversion.json": 5.944852544500876e-06,
#     "qlink/networks_with_cavity_no_conversion.json": 7.999995199289722e-07,
#     "qlink/networks_with_cavity_with_conversion.json": 8.243310647958359e-05,
#     "qlink/networks_with_cavity_with_conversion_high_c_loss.json": 8.243310647958359e-05
# }

num_pairs_dct = {"max1": 1,
                 "max3": [1, 3],
                 "max255": 255,
                 "3": 3}

p_req_fractions = {"ultra": 1.5,
                   "high": 0.99,
                   "low": 0.7}

origin_probs = {"originA": (1, 0),
                "originB": (0, 1),
                "originAB": (1/2, 1/2)}

weights_fractions = {"high": 10,
                     "low": 2}

mixes = {"uniform": {"NL": 1/3, "CK": 1/3, "MD": 1/3},
         "moreNL": {"NL": 4/6, "CK": 1/6, "MD": 1/6},
         "moreCK": {"NL": 1/6, "CK": 4/6, "MD": 1/6},
         "moreMD": {"NL": 1/6, "CK": 1/6, "MD": 4/6},
         "noNLmoreCK": {"NL": 0, "CK": 4/5, "MD": 1/5},
         "noNLmoreMD": {"NL": 0, "CK": 1/5, "MD": 4/5}
         }

# Weights and num queues
weights_dct = {"FIFO": ([0], 1),
               "higherWFQ": ([0, 10, 1], 3),
               "lowerWFQ": ([0, 2, 1], 3)
               }


def single_type_request_params(type, config_file, origin_prob, p_fraction, num_pairs):
    if type == "NL":
        params = {"num_pairs": num_pairs,
                  "tmax_pair": 0,
                  "min_fidelity": 0.8,
                  "purpose_id": 0,
                  "priority": 0,
                  "store": True,
                  "atomic": True,
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


def mixed_request_params(config_file_path, origin_probs, p_fractions, num_pairs):
    request_params = {}
    for type in origin_probs.keys():
        # p = probs[type]
        num_pair = num_pairs[type]

        # Get params of this type
        params = single_type_request_params(type, config_file_path, origin_probs[type], p_fractions[type], num_pair)
        request_params[type] = params

    return request_params


paramcombinations = {}

# Simulation scenarios
config_name = "QLINK_WC_WC"
config = configs[config_name]
config_file_path = os.path.join(config_dir, config)
p_base_fraction_name = "high"
p_base_fraction = p_req_fractions[p_base_fraction_name]
for weights_name in ["FIFO", "higherWFQ"]:
    sched_params = weights_dct[weights_name]
    weights = sched_params[0]
    num_priorities = sched_params[1]
    for mix_name in ["uniform", "noNLmoreMD"]:
        mix = mixes[mix_name]
        num_pairs = {"NL": 2, "CK": 2, "MD": 10}
        p_fractions = {type: m * p_base_fraction for type, m in mix.items()}
        p_origins = {type: 1/2 for type in mix.keys()}
        request_paramsA = mixed_request_params(config_file_path, p_origins, p_fractions, num_pairs)
        request_paramsB = mixed_request_params(config_file_path, p_origins, p_fractions, num_pairs)
        simulation_run_params = {"config": config_file_path,
                                 "request_paramsA": request_paramsA,
                                 "request_paramsB": request_paramsB,
                                 "egp_queue_weights": weights,
                                 "num_priorities": num_priorities}
        simulation_run_params.update(constant_params)
        run_name = "{}_mix_{}_sched_{}".format(config_name, mix_name, weights_name)
        paramcombinations[run_name] = simulation_run_params

print(len(paramcombinations) * number_of_runs)

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
