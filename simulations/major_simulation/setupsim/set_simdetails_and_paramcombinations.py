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

constant_params = {
    "max_sim_time": 0,
    "max_wall_time": 0,
    "max_mhp_cycle": 80,
    "t0": 0,
    "enable_pdb": False,
    "wall_time_per_timestep": 1 * 60,
    "save_additional_data": True,
    "collect_queue_data": True,
    "request_cycle": 0,
    "alphaA": [0.1, 0.3],
    "alphaB": [0.1, 0.3]
}

config_dir = "setupsim/config"

configs = {"LAB_NC_NC": "lab/networks_no_cavity_no_conversion.json",
           "QLINK_WC_WC": "qlink/networks_with_cavity_with_conversion.json",
           "QLINK_WC_WC_HIGH_C_LOSS": "qlink/networks_with_cavity_with_conversion_high_c_loss.json",
           }

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
    "qlink/networks_with_cavity_with_conversion.json": 8.243310647958359e-05,
    "qlink/networks_with_cavity_with_conversion_high_c_loss.json": 8.243310647958359e-05
}

num_pairs_dct = {"max1": 1,
                 "max3": [1,3],
                 "max255": 255}

p_req_fractions = {"ultra": 0.99,
                   "high": 0.8,
                   "low": 0.2}

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


def single_type_request_params(type, prob, num_pairs):
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
                  "min_fidelity": 0.5,
                  "purpose_id": 0,
                  "priority": 2,
                  "store": False,
                  "atomic": False,
                  "measure_directly": True
                  }
    else:
        raise ValueError("Unknown type")

    request_params = {"prob": prob,
                      "params": params
                      }

    return request_params


def mixed_request_params(probs, num_pairs):
    request_params = {}
    for type in probs.keys():
        p = probs[type]
        num_pair = num_pairs[type]

        # Get params of this type
        params = single_type_request_params(type, p, num_pair)
        request_params[type] = params

    return request_params


paramcombinations = {}

# Single type requests
for config_name in ["LAB_NC_NC", "QLINK_WC_WC"]:
    config = configs[config_name]
    config_file_path = os.path.join(config_dir, config)
    p_succ = config_to_p_succ[config]
    for type in ["NL", "CK", "MD"]:
        if type == "MD":
            num_pairs_names = ["max1", "max3", "max255"]
        else:
            num_pairs_names = ["max1", "max3"]
        for num_pairs_name in num_pairs_names:
            num_pairs = num_pairs_dct[num_pairs_name]
            for p_fraction_name, p_fraction in p_req_fractions.items():
                for origin_prob_name, origin_prob in origin_probs.items():
                    p_requestA = p_fraction * origin_prob[0] * p_succ
                    p_requestB = p_fraction * origin_prob[1] * p_succ
                    request_paramsA = {type: single_type_request_params(type, p_requestA, num_pairs)}
                    request_paramsB = {type: single_type_request_params(type, p_requestB, num_pairs)}
                    simulation_run_params = {"config": config_file_path,
                                             "request_paramsA": request_paramsA,
                                             "request_paramsB": request_paramsB,
                                             "egp_queue_weights": [0],
                                             "num_priorities": 1
                                             }
                    simulation_run_params.update(constant_params)
                    run_name = "{}_{}_{}_req_frac_{}_origin_{}_weights_FIFO".format(config_name, type, num_pairs_name, p_fraction_name, origin_prob_name)
                    paramcombinations[run_name] = simulation_run_params

# Mixed requests
origin_prob = (1/2, 1/2)
for config_name in ["LAB_NC_NC", "QLINK_WC_WC"]:
    config = configs[config_name]
    config_file_path = os.path.join(config_dir, config)
    p_succ = config_to_p_succ[config]
    for weights_name, sched_params in weights_dct.items():
        weights = sched_params[0]
        num_priorities = sched_params[1]
        for mix_name, mix in mixes.items():
            p_fraction = p_req_fractions["high"]
            probs = {type: origin_prob[0] * p_succ * m * p_fraction for type, m in mix.items()}
            if mix_name == "uniform":
                num_pairs = {"NL": 1, "CK": 1, "MD": 1}
            else:
                num_pairs = {"NL": [1, 3], "CK": [1, 3], "MD": [1, 255]}
            request_paramsA = mixed_request_params(probs, num_pairs)
            request_paramsB = mixed_request_params(probs, num_pairs)
            simulation_run_params = {"config": config_file_path,
                                     "request_paramsA": request_paramsA,
                                     "request_paramsB": request_paramsB,
                                     "egp_queue_weights": weights,
                                     "num_priorities": num_priorities}
            simulation_run_params.update(constant_params)
            run_name = "{}_mix_{}_weights_{}".format(config_name, mix_name, weights_name)
            paramcombinations[run_name] = simulation_run_params

# Added exaggerated classical noise scenario
config_name = "QLINK_WC_WC_HIGH_C_LOSS"
config = configs[config_name]
p_succ = config_to_p_succ[config]
weights_name = "lowerWFQ"
sched_params = weights_dct[weights_name]
weights = sched_params[0]
num_priorities = sched_params[1]
p_fraction = p_req_fractions["high"]
mix_name = "uniform"
mix = mixes[mix_name]
probs = {type: origin_prob[0] * p_succ * m * p_fraction for type, m in mix.items()}
num_pairs = {"NL": [1, 3], "CK": [1, 3], "MD": [1, 255]}
request_paramsA = mixed_request_params(probs, num_pairs)
request_paramsB = mixed_request_params(probs, num_pairs)
simulation_run_params = {"config": config_file_path,
                         "request_paramsA": request_paramsA,
                         "request_paramsB": request_paramsB,
                         "egp_queue_weights": weights,
                         "num_priorities": num_priorities}
simulation_run_params.update(constant_params)
run_name = "{}_mix_{}_weights_{}".format(config_name, mix_name, weights_name)
paramcombinations[run_name] = simulation_run_params

print(len(paramcombinations))

# opt_params = {
#     "create_probB": 0,
#     "min_pairs": 1,
#     "max_pairs": 1,
#     "tmax_pair": 0,
#     "request_cycle": 0,
#     "num_requests": 0,
#     "max_sim_time": 0,
#     "max_wall_time": 4 * 24 * 3600,
#     "max_mhp_cycle": 80,
#     "enable_pdb": False,
#     "alphaA": 0.1,
#     "alphaB": 0.1,
#     "measure_directly": True,
#     "t0": 0,
#     "wall_time_per_timestep": 1 * 1,
#     "save_additional_data": True,
#     "collect_queue_data": True}
#
# paramcombinations = {}
# # create paramcombinations
# for name, scenario in name_to_scenario.items():
#     config_file = scenario[0]
#     freq_req_factor = scenario[1]
#     param_set = {}
#     param_set.update(opt_params)
#     param_set["config"] = config_dir + "/" + config_file
#     p_succ = config_to_p_succ[config_file]
#     param_set["create_probA"] = freq_req_factor * p_succ
#     paramcombinations[name] = param_set

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
