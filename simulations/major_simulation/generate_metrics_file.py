import sys
import os
import json
import sqlite3
import csv
from math import ceil
from collections import namedtuple

from easysquid.toolbox import logger
from simulations.analysis_sql_data import parse_table_data_from_sql, calc_fidelity, parse_raw_queue_data
from qlinklayer.datacollection import EGPCreateDataPoint, EGPOKDataPoint, EGPStateDataPoint, EGPQubErrDataPoint

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
        try:
            oks_dct = creates_and_oks_by_create_id[absolute_create_id]["oks"]
        except KeyError:
            logger.warning("OK with Absolute Create ID {} with no corresponding Create".format(absolute_create_id))
            oks_dct = None
        if oks_dct is not None:
            if node_id not in oks_dct:
                # We will later add state and quberr so make this a dict
                oks_dct[node_id] = {mhp_seq: {"ok": datapoint}}
            elif mhp_seq in oks_dct[node_id]:
                raise RuntimeError("Duplicate entry for Absolute Create ID = {}, Node ID = {} and MHP Seq = {}".format(absolute_create_id, node_id, mhp_seq))
            else:
                # We will later add state and quberr so make this a dict
                oks_dct[node_id][mhp_seq] = {"ok": datapoint}

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
            # Check if both nodes received OK for same create ID at the same time
            if len(ok_keys) == 2:
                if ok_keys[0][0] != ok_keys[1][0]:
                    raise RuntimeError("The timestamp {} of this qubit state does not have a unique corresponding ok datapoint".format(timestamp))
            else:
                raise RuntimeError("The timestamp {} of this qubit state does not have a unique corresponding ok datapoint".format(timestamp))
        absolute_create_id, node_id, mhp_seq = ok_keys[0]

        # Add this qubit state datapoint to the data structure
        if "state" in creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq]:
            raise RuntimeError("OK for Absolute Create ID {}, Node ID {} and MHP Seq {} already has a state".format(absolute_create_id, node_id, mhp_seq))
        creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq]["state"] = state_datapoint


def add_quberr(quberr_data, creates_and_oks_by_create_id, ok_keys_by_timestamp):
    max_timestamp = -1
    for raw_datapoint in quberr_data:
        quberr_datapoint = EGPQubErrDataPoint(raw_datapoint)
        if quberr_datapoint.timestamp > max_timestamp:
            max_timestamp = quberr_datapoint.timestamp
    for raw_datapoint in quberr_data:
        quberr_datapoint = EGPQubErrDataPoint(raw_datapoint)
        if quberr_datapoint.success:
            timestamp = quberr_datapoint.timestamp
            try:
                ok_keys = ok_keys_by_timestamp[timestamp]
            except KeyError:
                logger.warning("No OK corresponding to QBER datapoint with timestamp {}".format(timestamp))
                ok_keys = None
            if ok_keys is not None:
                if len(ok_keys) != 1:
                    # Check if both nodes received OK for same create ID at the same time
                    if len(ok_keys) == 2:
                        if ok_keys[0][0] != ok_keys[1][0]:
                            raise RuntimeError("The timestamp {} of this QBER datapoint does not have a unique corresponding ok datapoint".format(timestamp))
                    else:
                        raise RuntimeError("The timestamp {} of this QBER datapoint does not have a unique corresponding ok datapoint".format(timestamp))
                absolute_create_id, node_id, mhp_seq = ok_keys[0]

                # Add this QBER datapoint to the data structure
                if "QBER" in creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq]:
                    raise RuntimeError("OK for Absolute Create ID {}, Node ID {} and MHP Seq {} already has a QBER".format(absolute_create_id, node_id, mhp_seq))
                creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq]["QBER"] = quberr_datapoint


def sort_data_by_request(filename):
    states_data = parse_table_data_from_sql(filename, "EGP_Qubit_States")
    quberr_data = parse_table_data_from_sql(filename, "EGP_QubErr")

    creates_and_oks_by_create_id, ok_keys_by_timestamp = get_creates_and_oks_by_create_id(filename)

    add_qubit_states(states_data, creates_and_oks_by_create_id, ok_keys_by_timestamp)

    add_quberr(quberr_data, creates_and_oks_by_create_id, ok_keys_by_timestamp)

    return creates_and_oks_by_create_id


def get_metrics_from_single_file(filename):

    creates_and_oks_by_create_id = sort_data_by_request(filename)

    ##########################
    # Nr OKs and outstanding #
    ##########################

    nr_oks_per_prio = {i: 0 for i in range(3)}
    nr_reqs_per_prio = {i: 0 for i in range(3)}
    nr_outstanding_req_per_prio = {i: 0 for i in range(3)}
    nr_outstanding_pairs_per_prio = {i: 0 for i in range(3)}
    for create_id, create_data in creates_and_oks_by_create_id.items():
        node_id = 0
        priority = create_data["create"].priority
        nr_reqs_per_prio[priority] += 1
        if node_id in create_data["oks"]:
            oks = create_data["oks"][node_id]
            nr_oks_per_prio[priority] += len(oks)

            pairs_left = create_data["create"].num_pairs - len(oks)
            if pairs_left > 0:
                nr_outstanding_req_per_prio[priority] += 1
                nr_outstanding_pairs_per_prio[priority] += pairs_left
        else:
            nr_outstanding_req_per_prio[priority] += 1
            nr_outstanding_pairs_per_prio[priority] += create_data["create"].num_pairs

    ##########
    # Errors #
    ##########

    errors_data = parse_table_data_from_sql(filename, "EGP_Errors")
    num_errors = len(errors_data)

    ################################
    # Fidelities, QBER and latency #
    ################################

    fids_per_prio = {}
    qber_per_prio = {}
    latencies_per_prio_per_node = {}
    attempts_per_prio = {i: 0 for i in range(3)}
    cycles_per_attempt_per_prio = {i: [] for i in range(3)}
    priorities = []

    for create_id, request_data in creates_and_oks_by_create_id.items():
        create_datapoint = request_data["create"]
        priority = create_datapoint.priority
        if priority not in priorities:
            priorities.append(priority)
        for node_id, node_oks in request_data["oks"].items():
            for mhp_seq, ok_data in node_oks.items():

                # Qubit state
                if "state" in ok_data:
                    state_datapoint = ok_data["state"]
                    d_matrix = state_datapoint.density_matrix
                    assert (state_datapoint.outcome1 == state_datapoint.outcome2)
                    outcome = state_datapoint.outcome1
                    fid = calc_fidelity(outcome, d_matrix)
                    if priority not in fids_per_prio:
                        fids_per_prio[priority] = [fid]
                    else:
                        fids_per_prio[priority].append(fid)

                # QBER
                if "QBER" in ok_data:
                    qber_datapoint = ok_data["QBER"]
                    qberxyz = {"X": qber_datapoint.x_err, "Y": qber_datapoint.y_err, "Z": qber_datapoint.z_err}
                    if priority not in qber_per_prio:
                        qber_per_prio[priority] = {"X": [], "Y": [], "Z": []}
                    for basis, qber in qberxyz.items():
                        if qber != -1:
                            qber_per_prio[priority][basis].append(qber)

                # Latency
                create_time = create_datapoint.create_time
                ok_time = ok_data["ok"].timestamp
                latency = (ok_time - create_time)
                if priority not in latencies_per_prio_per_node:
                    latencies_per_prio_per_node[priority] = {}
                if node_id not in latencies_per_prio_per_node[priority]:
                    latencies_per_prio_per_node[priority][node_id] = []
                latencies_per_prio_per_node[priority][node_id].append(latency)

                # Attempts
                # Only collect for one node
                if node_id == 0:
                    ok_datapoint = ok_data["ok"]
                    attempts_per_prio[priority] += ok_datapoint.attempts

                # Cycles per attempt
                ok_datapoint = ok_data["ok"]
                cycles_per_attempt_per_prio[priority].append(ok_datapoint.used_cycles / ok_datapoint.attempts)

    avg_fid_per_prio = {priority: sum(fids)/len(fids) for priority, fids in fids_per_prio.items()}

    avg_qber_per_prio = {}
    for priority, qbersxyz in qber_per_prio.items():
        avg_qber_per_prio[priority] = {basis: sum(qbers)/len(qbers) if len(qbers) > 0 else 0 for basis, qbers in qbersxyz.items()}
        avg_qber_per_prio[priority]["fid"] = 1 - sum(avg_qber_per_prio[priority].values()) / 2

    avg_latencies_per_prio_per_node = {}
    for priority, latencies_per_node in latencies_per_prio_per_node.items():
        avg_latencies_per_prio_per_node[priority] = {}
        for node_id, latencies in latencies_per_node.items():
            avg_latencies_per_prio_per_node[priority][node_id] = sum(latencies) / len(latencies)

    avg_cycles_per_attempt_per_prio = {priority: sum(c_p_a) / len(c_p_a) if len(c_p_a) > 0 else None for priority, c_p_a in cycles_per_attempt_per_prio.items()}

    #################
    # Queue Lengths #
    #################
    # conn = sqlite3.connect(filename)
    # c = conn.cursor()
    # c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    # all_table_names = c.fetchall()
    # queue_ids = []
    # for table_name in all_table_names:
    #     table_name = table_name[0]
    #     if table_name.startswith("EGP_Local_Queue_A"):
    #         queue_id = int(table_name.split('_')[-2])
    #         if queue_id not in queue_ids:
    #             queue_ids.append(queue_id)
    queue_ids = range(3)

    raw_all_queue_data = {qid: parse_table_data_from_sql(filename, "EGP_Local_Queue_A_{}".format(qid)) for qid in queue_ids}
    all_queue_lengths = {}
    times_non_idle = {}
    for qid, raw_queue_data in raw_all_queue_data.items():
        queue_data = parse_raw_queue_data(raw_queue_data)
        all_queue_lengths[qid] = queue_data[0]
        times_non_idle[qid] = queue_data[-1]
    all_queue_lengths = {qid: parse_raw_queue_data(raw_queue_data)[0] for qid, raw_queue_data in raw_all_queue_data.items()}
    all_avg_queue_lengths = {qid: sum(queue_lengths)/len(queue_lengths) for qid, queue_lengths in all_queue_lengths.items()}

    ##############
    # Throughput #
    ##############

    # Simulation time
    additional_data_filename = filename[:-3] + "_additional_data.json"
    with open(additional_data_filename, 'r') as f:
        additional_data = json.load(f)
    total_matrix_time = additional_data["total_real_time"]
    mhp_cycle = additional_data["mhp_t_cycle"]
    total_mhp_cycles = ceil(total_matrix_time / mhp_cycle)
    cycles_non_idle = times_non_idle[0] / mhp_cycle
    print("Nr MHP cycles non idle {}".format(cycles_non_idle))
    print("Nr MHP cycles {}".format(total_mhp_cycles))
    print("P Succ per attempt = {}".format(additional_data["p_succ"]))
    print("Attempts: " + "".join(["{} = {}, ".format(prio, attempts) for prio, attempts in attempts_per_prio.items()]))
    print("Cycles per attempts: " + "".join(["{} = {}, ".format(prio, cycles_non_idle / attempts) for prio, attempts in attempts_per_prio.items() if attempts > 0]))
    print("Cycles per attempts (real): " + "".join(["{} = {}, ".format(prio, c_p_a) for prio, c_p_a in avg_cycles_per_attempt_per_prio.items()]))

    if "FIFO" in filename:
        avg_throughput_per_prio = {priority: nr_oks / (times_non_idle[0] * 1e-9) for priority, nr_oks in nr_oks_per_prio.items()}
    else:
        avg_throughput_per_prio = {}
        for priority, nr_oks in nr_oks_per_prio.items():
            if times_non_idle[priority] == 0:
                avg_throughput_per_prio[priority] = 0
            else:
                avg_throughput_per_prio[priority] = nr_oks / (times_non_idle[priority] * 1e-9)

    ########################
    # Outstanding requests #
    ########################

    #############################
    # Construct dict of metrics #
    #############################

    metrics = {}
    for priority in range(3):
        if priority == 0:
            prio_name = "NL"
        elif priority == 1:
            prio_name = "CK"
        elif priority == 2:
            prio_name = "MD"
        else:
            raise RuntimeError("Unkown priority {}".format(priority))

        metrics["NrReqs_Prio{}".format(prio_name)] = nr_reqs_per_prio[priority]
        metrics["NrOKs_Prio{}".format(prio_name)] = nr_oks_per_prio[priority]
        metrics["NrRemReq_Prio{}".format(prio_name)] = nr_outstanding_req_per_prio[priority]
        metrics["NrRemPairs_Prio{}".format(prio_name)] = nr_outstanding_pairs_per_prio[priority]
        metrics["AvgThroughp_Prio{} (1/s)".format(prio_name)] = avg_throughput_per_prio[priority]
        metrics["AvgCyc_per_Att_Prio{}".format(prio_name)] = avg_cycles_per_attempt_per_prio[priority]

        try:
            avg_latencies_per_node = avg_latencies_per_prio_per_node[priority]
        except KeyError:
            avg_latencies_per_node = {}
        for node_id in range(2):
            try:
                metrics["AvgLaten_Prio{}_NodeID{} (s)".format(prio_name, node_id)] = avg_latencies_per_node[node_id] * 1e-9
            except KeyError:
                metrics["AvgLaten_Prio{}_NodeID{} (s)".format(prio_name, node_id)] = None

        if priority < 2:
            try:
                metrics["AvgFid_Prio{}".format(prio_name)] = avg_fid_per_prio[priority]
            except KeyError:
                metrics["AvgFid_Prio{}".format(prio_name)] = None
        else:
                try:
                    metrics["AvgFid_Prio{}".format(prio_name)] = avg_qber_per_prio[priority]["fid"]
                except KeyError:
                    metrics["AvgFid_Prio{}".format(prio_name)] = None
                for basis in ["X", "Y", "Z"]:
                    try:
                        metrics["AvgQBER{}_Prio{}".format(basis, prio_name)] = avg_qber_per_prio[priority]["{}".format(basis)]
                    except KeyError:
                        metrics["AvgQBER{}_Prio{}".format(basis, prio_name)] = None

    metrics["NumErrors"] = num_errors

    metrics["TotalMatrixT (s)"] = total_matrix_time * 1e-9

    for qid in range(3):
        try:
            avg_queue_length = all_avg_queue_lengths[qid]
        except KeyError:
            avg_queue_length = None
        try:
            time_idle = (total_matrix_time - times_non_idle[qid]) * 1e-9
        except KeyError:
            time_idle = None
        metrics["AvgQeueuLen_QID{}".format(qid)] = avg_queue_length
        metrics["QueueIdle_QID{}".format(qid)] = time_idle

    return metrics


def main(results_folder):
    all_metrics = []
    for entry in sorted(os.listdir(results_folder)):
        if entry.endswith(".db"):
            scenario_key = entry.split("_key_")[1].split("_run_")[0]
            # if scenario_key == "LAB_NC_NC_MD_max3_req_frac_high_origin_originA_weights_FIFO":
            print(scenario_key)
            metrics = get_metrics_from_single_file(os.path.join(results_folder, entry))
            metrics["Name"] = scenario_key
            all_metrics.append(metrics)

    if len(all_metrics) > 0:
        csv_filename = os.path.join(results_folder, "metrics.csv")
        print(csv_filename)
        with open(csv_filename, 'w', newline='') as f:
            fieldnames = ["Name", "NumErrors", "TotalMatrixT (s)"] + ["QueueIdle_QID{}".format(qid) for qid in range(3)]
            fieldnames += ["AvgCyc_per_Att_Prio{}".format(p) for p in ["NL", "CK", "MD"]]
            fieldnames += sum([["Nr{}_Prio{}".format(what_nr, p) for p in ["NL", "CK", "MD"]] for what_nr in ["Reqs", "OKs", "RemReq", "RemPairs"]], [])
            fieldnames += [fieldname for fieldname in all_metrics[0].keys() if fieldname not in fieldnames]
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(all_metrics)


if __name__ == '__main__':
    results_folder = sys.argv[1]
    main(results_folder)
