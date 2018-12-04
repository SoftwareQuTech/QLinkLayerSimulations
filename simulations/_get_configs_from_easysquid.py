########################################################################################################################
#
# This file is a just to automate the process of copying the config files from EasySquid and changing the type for the
# mhp_conn to be node_centric_heralded_fibre_connection. We should probably find a better way in the future.
#
# Author: Axel Dahlberg
########################################################################################################################

import shutil
import os
import json
from argparse import ArgumentParser

path_to_this_file = os.path.realpath(__file__)
path_to_this_folder = "/".join(path_to_this_file.split("/")[:-1])
path_to_this_config_folder = os.path.join(path_to_this_folder, "create_measure_simulation/setupsim/config")

NODE_CENTRIC_HERALDED_FIBRE_CONNECTION = "node_centric_heralded_fibre_connection"


def _remove_current_files():
    for folder in os.listdir(path_to_this_config_folder):
        dst = os.path.join(path_to_this_config_folder, folder)
        if os.path.exists(dst):
            shutil.rmtree(dst)


def copy_files_from_easysquid(path_to_easysquid):
    _remove_current_files()

    path_to_network_configs = os.path.join(path_to_easysquid, "config/networks/NV")
    for folder in os.listdir(path_to_network_configs):
        src = os.path.join(path_to_network_configs, folder)
        dst = os.path.join(path_to_this_config_folder, folder)
        shutil.copytree(src, dst)


def change_connnection_type():
    for dirpath, dirname, filenames in os.walk(path_to_this_config_folder):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)

            # Read config file
            with open(file_path, 'r') as f:
                config_dct = json.load(f)

            # Get the connection config name of the mhp connection
            conn_config_name = _get_conn_config_name_of_mhp_conn(config_dct)

            # Update the type to use node centric heralded fibre connection
            config_dct["conn_configs"][conn_config_name]["type"] = NODE_CENTRIC_HERALDED_FIBRE_CONNECTION

            # Update the comment in the file
            config_dct["AutoGenerate"].append("This file was then later modified by /path/to/QLinkLayer/simulations/"
                                              "_get_configs_from_easysquid.py")

            # Write to file again
            with open(file_path, 'w') as f:
                json.dump(config_dct, f, indent=2)


def _get_conn_config_name_of_mhp_conn(config_dct):
    # Get the connection config name of the mhp connection
    for connection in config_dct["connections"]:
        if connection["conn_ID"] == "mhp_conn":
            break
    else:
        raise RuntimeError("Could not find a connection with conn_ID='mhp_conn'")
    conn_config_name = connection["conn_config"]
    return conn_config_name


def _get_qpd_config_name_of_qpd(config_dct):
    qpd_config_name = None
    # Get qpd config name
    for node in config_dct["nodes"].values():
        qpd_config_name = node["qpd_config"]
    if qpd_config_name is None:
        raise RuntimeError("Could not find the qpd config name of node.")
    return qpd_config_name


def make_no_loss_and_no_noise_files():
    # Create folder
    no_noise_folder = os.path.join(path_to_this_config_folder, "no_noise")
    os.mkdir(no_noise_folder)

    # Create no losses file
    src = os.path.join(path_to_this_config_folder, "lab", "networks_no_cavity_no_conversion.json")
    dst = os.path.join(no_noise_folder, "no_losses.json")
    shutil.copyfile(src, dst)
    _update_no_losses_file(dst)

    # Create no noise file
    src = os.path.join(no_noise_folder, "no_losses.json")
    dst = os.path.join(no_noise_folder, "no_noise.json")
    shutil.copyfile(src, dst)
    _update_no_noise_file(dst)


def _update_no_losses_file(path_to_file):
    with open(path_to_file, 'r') as f:
        config_dct = json.load(f)

    # Get the connection config name of the mhp connection
    conn_config_name = _get_conn_config_name_of_mhp_conn(config_dct)

    # Update loss parameters of connection
    conn_parameters = config_dct["conn_configs"][conn_config_name]["parameters"]
    conn_parameters["p_loss_init"] = 0
    conn_parameters["p_loss_length"] = 0
    conn_parameters["dark_rate"] = 0
    conn_parameters["detection_eff"] = 1
    conn_parameters["visibility"] = 1

    # Get qpd config name
    qpd_config_name = _get_qpd_config_name_of_qpd(config_dct)
    qpd_parameters = config_dct["qpd_config"][qpd_config_name]["parameters"]

    # Update loss parameters of photon emission
    photon_emission_noise = qpd_parameters["photon_emission"]["photon_emission_noise"]
    photon_emission_noise["p_zero_phonon"] = 1
    photon_emission_noise["collection_eff"] = 1

    with open(path_to_file, 'w') as f:
        json.dump(config_dct, f, indent=2)


def _update_no_noise_file(path_to_file):
    with open(path_to_file, 'r') as f:
        config_dct = json.load(f)

    # Get qpd config name
    qpd_config_name = _get_qpd_config_name_of_qpd(config_dct)
    qpd_parameters = config_dct["qpd_config"][qpd_config_name]["parameters"]

    # Update noise parameters of photon emission
    photon_emission_noise = qpd_parameters["photon_emission"]["photon_emission_noise"]
    photon_emission_noise["delta_w"] = [0] * len(photon_emission_noise["delta_w"])
    photon_emission_noise["tau_decay"] = [0] * len(photon_emission_noise["tau_decay"])
    photon_emission_noise["delta_phi"] = 0
    photon_emission_noise["tau_emission"] = 0

    # Update noise parameters of gates
    gates = qpd_parameters["gates"]
    for gate_group_name, gate_group in gates.items():
        for gate_name, gate in gate_group.items():
            noise_model = gate["noise_model"]
            for noise_name, noise in noise_model.items():
                for error_rate_name, error_rate in noise.items():
                    noise[error_rate_name] = 0

    # Update noise parameters of qubit decoherence
    qubits = qpd_parameters["qubits"]
    for qubit in qubits:
        noise_model = qubit["noise_model"]
        for noise_name, noise in noise_model.items():
            if noise_name == "T1T2":
                noise["T1"] = 0
                noise["T2"] = 0

    with open(path_to_file, 'w') as f:
        json.dump(config_dct, f, indent=2)


def main(path_to_easysquid):
    copy_files_from_easysquid(path_to_easysquid=path_to_easysquid)
    change_connnection_type()
    make_no_loss_and_no_noise_files()


def parse_args():
    parser = ArgumentParser()

    parser.add_argument('--path_to_easysquid', required=True, type=str,
                        help="Absolute path to the EasySquid repo")

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(path_to_easysquid=args.path_to_easysquid)
