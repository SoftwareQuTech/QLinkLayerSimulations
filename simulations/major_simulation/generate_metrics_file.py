import sys
import os
import json
import sqlite3
from collections import namedtuple

from easysquid.toolbox import logger
from simulations.analysis_sql_data import parse_table_data_from_sql, calc_fidelity
from qlinklayer.datacollection import EGPCreateDataPoint, EGPOKDataPoint, EGPStateDataPoint

MetricsTuple = namedtuple("Metrics", ["fidelity", "QBER", "latency_per_pair", "throughput", "success_fraction", "avg_queue_length"])
MetricsTuple.__new__.__defaults__ = (None,) * len(MetricsTuple._fields)


def get_table_data_by_timestamp(filename, base_table_name):
    data = parse_table_data_from_sql(filename, base_table_name)

    data_by_timestamp = {}
    for raw_datapoint in data:
        if base_table_name == "EGP_Creates":
            datapoint = EGPCreateDataPoint(raw_datapoint)
        elif base_table_name == "EGP_OKs":
            datapoint = EGPOKDataPoint(raw_datapoint)
        elif base_table_name == "EGP_Qubit_States":
            datapoint = EGPStateDataPoint(raw_datapoint)
        else:
            raise ValueError("Unknown base_table_name = {}".format(base_table_name))
        timestamp = datapoint.timestamp
        if timestamp in data_by_timestamp:
            data_by_timestamp[timestamp].append(datapoint)
        else:
            data_by_timestamp[timestamp] = [datapoint]

    return data_by_timestamp

# def get_table_data_by_origin_and_create_id(filename, base_table_name):
#     data = parse_table_data_from_sql(filename, base_table_name)
#
#     data_by_node_and_create_id = {}
#     for raw_datapoint in data:
#         if base_table_name == "EGP_Creates":
#             datapoint = EGPCreateDataPoint(raw_datapoint)
#             origin_id = datapoint.node_id
#         elif base_table_name == "EGP_OKs":
#             datapoint = EGPOKDataPoint(raw_datapoint)
#             origin_id =
#         else:
#             raise ValueError("Unknown base_table_name = {}".format(base_table_name))
#         create_id = datapoint.create_id
#         node_id = datapoint.
#         if create_id in data_by_node_and_create_id:
#             raise RuntimeError("Duplicate Create ID = {}".format(create_id))
#         else:
#             data_by_timestamp[create_id] = datapoint
#
#     return data_by_timestamp

def get_creates_and_oks_by_create_id(filename):
    creates_data = parse_table_data_from_sql(filename, "EGP_Creates")
    oks_data = parse_table_data_from_sql(filename, "EGP_OKs")

    creates_and_oks_by_create_id = {}
    ok_keys_by_timestamp_and_node_id = {}

    # Get the creates
    for raw_datapoint in creates_data:
        datapoint = EGPCreateDataPoint(raw_datapoint)
        create_id = datapoint.create_id
        node_id = datapoint.node_id
        absolute_create_id = node_id, create_id
        if absolute_create_id in creates_and_oks_by_create_id:
            raise RuntimeError("Duplicate Absolute Create ID = {} for Creates".format(absolute_create_id))
        else:
            creates_and_oks_by_create_id[absolute_create_id] = {"create": datapoint, "oks": {}}

    # Get the oks
    for raw_datapoint in oks_data:
        datapoint = EGPOKDataPoint(raw_datapoint)
        create_id = datapoint.create_id
        origin_id = datapoint.origin_id
        absolute_create_id = origin_id, create_id
        node_id = datapoint.node_id
        mhp_seq = datapoint.mhp_seq
        timestamp = datapoint.timestamp
        oks_dct = creates_and_oks_by_create_id[absolute_create_id]["oks"]
        if node_id not in oks_dct:
            oks_dct[node_id] = {mhp_seq: datapoint}
        elif mhp_seq in oks_dct[node_id]:
            raise RuntimeError("Duplicate entry for Absolute Create ID = {}, Node ID = {} and MHP Seq = {}".format(absolute_create_id, node_id, mhp_seq))
        else:
            oks_dct[node_id][mhp_seq] = datapoint

        if timestamp in ok_keys_by_timestamp_and_node_id:
            ok_keys_by_timestamp_and_node_id[timestamp].append([absolute_create_id, node_id, mhp_seq])
        else:
            ok_keys_by_timestamp_and_node_id[timestamp] = [[absolute_create_id, node_id, mhp_seq]]

    return creates_and_oks_by_create_id, ok_keys_by_timestamp_and_node_id

# def get_table_data_by_timestamp(filename, base_table_name):
#     data = parse_table_data_from_sql(filename, base_table_name)
#
#     data_by_timestamp = {}
#     for datapoint in data:
#         timestamp = datapoint[0]
#         if timestamp in data_by_timestamp:
#             data_by_timestamp[timestamp].append(datapoint)
#         else:
#             data_by_timestamp[timestamp] = [datapoint]
#
#     return data_by_timestamp


def add_qubit_states(states_data, creates_and_oks_by_create_id, ok_keys_by_timestamp):
    for raw_datapoint in states_data:
        state_datapoint = EGPStateDataPoint(raw_datapoint)
        timestamp = state_datapoint.timestamp
        ok_keys = ok_keys_by_timestamp[timestamp]
        if len(ok_keys) != 1:
            raise RuntimeError("The timestamp {} of this qubit state does not have a unique corresponding create datapoint".format(timestamp))
        absolute_create_id, node_id, mhp_seq = ok_keys[0]

        # Add this qubit state datapoint to the data structure
        # for node_id, ok_node_dct in creates_and_oks_by_create_id[absolute_create_id]:
        ok_datapoint = creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq]
        creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq] = {"ok": ok_datapoint, "state": state_datapoint}


def sort_data_by_request(filename):
    states_data = parse_table_data_from_sql(filename, "EGP_Qubit_States")

    creates_and_oks_by_create_id, ok_keys_by_timestamp = get_creates_and_oks_by_create_id(filename)

    add_qubit_states(states_data, creates_and_oks_by_create_id, ok_keys_by_timestamp)

    return creates_and_oks_by_create_id


def get_metrics_from_single_file(filename):

    # EGP_Creates
    # EGP_Local_Queue_X_X
    # EGP_OKs
    # EGP_Qubit_States
    # conn = sqlite3.connect(filename)
    # c = conn.cursor()
    # c.execute("SELECT name FROM sqlite_master WHERE type='table'")

    fids_per_prio = {}

    creates_and_oks_by_create_id = sort_data_by_request(filename)
    for create_id, request_data in creates_and_oks_by_create_id.items():
        create_datapoint = request_data["create"]
        priority = create_datapoint.priority
        for node_id, node_oks in request_data["oks"].items():
            for mhp_seq, ok_data in node_oks.items():
                if isinstance(ok_data, dict):
                    state_datapoint = ok_data["state"]
                    d_matrix = state_datapoint.density_matrix
                    assert (state_datapoint.outcome1 == state_datapoint.outcome2)
                    outcome = state_datapoint.outcome1
                    fid = calc_fidelity(outcome, d_matrix)
                    if priority not in fids_per_prio:
                        fids_per_prio[priority] = [fid]
                    else:
                        fids_per_prio[priority].append(fid)

    import pdb
    pdb.set_trace()


    # states_by_timestamp = get_table_data_by_timestamp(filename, "EGP_Qubit_States")
    # oks_by_timestamp = get_table_data_by_timestamp(filename, "EGP_OKs")
    #
    # for timestamp, oks in oks_by_timestamp.items():
    #     if len(oks) != 1:
    #         print("{} : {}".format(timestamp, oks))
    #
    # for timestamp, states in states_by_timestamp.items():
    #     if len(states) != 1:
    #         print("{} : {}".format(timestamp, states))
    #
    # # states_data = parse_table_data_from_sql(filename, "EGP_Qubit_States")
    # # oks_data = parse_table_data_from_sql(filename, "EGP_OKs")
    #
    # import pdb
    # pdb.set_trace()


def main(results_folder):
    print(MetricsTuple())
    all_metrics = {}
    for entry in os.listdir(results_folder):
        if entry.endswith(".db"):
            scenario_key = entry.split("_key_")[1].split("_run_")[0]
            metrics = get_metrics_from_single_file(os.path.join(results_folder, entry))
            all_metrics[scenario_key] = metrics
            print(scenario_key)
            break

if __name__ == '__main__':
    results_folder = sys.argv[1]
    main(results_folder)