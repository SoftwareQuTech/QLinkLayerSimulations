import os
import json
import csv
from argparse import ArgumentParser
import glob
import numpy as np
from xlsxwriter.workbook import Workbook
from collections import defaultdict

from easysquid.toolbox import logger
from simulations.analysis_sql_data import parse_table_data_from_sql, calc_fidelity, parse_raw_queue_data,\
    get_datacollection_version
from qlinklayer.datacollection import EGPCreateDataPoint, EGPOKDataPoint, EGPStateDataPoint, EGPQubErrDataPoint,\
    EGPErrorDataPoint
from qlinklayer.egp import NodeCentricEGP


def get_creates_and_oks_by_create_id(filename, expired_create_ids):
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
        expired = (create_id in expired_create_ids)
        if absolute_create_id in creates_and_oks_by_create_id:
            raise RuntimeError("Duplicate Absolute Create ID = {} for Creates".format(absolute_create_id))
        else:
            creates_and_oks_by_create_id[absolute_create_id] = {"create": datapoint, "oks": {}, "expired": expired}

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
                raise RuntimeError("Duplicate entry for"
                                   "Absolute Create ID = {}, Node ID = {} and MHP Seq = {}".format(absolute_create_id,
                                                                                                   node_id,
                                                                                                   mhp_seq))
            else:
                # We will later add state and quberr so make this a dict
                oks_dct[node_id][mhp_seq] = {"ok": datapoint}

            if timestamp in ok_keys_by_timestamp_and_node_id:
                ok_keys_by_timestamp_and_node_id[timestamp].append([absolute_create_id, node_id, mhp_seq])
            else:
                ok_keys_by_timestamp_and_node_id[timestamp] = [[absolute_create_id, node_id, mhp_seq]]

    return creates_and_oks_by_create_id, ok_keys_by_timestamp_and_node_id


def add_qubit_states(states_data, creates_and_oks_by_create_id, ok_keys_by_timestamp):
    for raw_datapoint in states_data:
        state_datapoint = EGPStateDataPoint(raw_datapoint)
        timestamp = state_datapoint.timestamp
        ok_keys = ok_keys_by_timestamp[timestamp]
        if len(ok_keys) != 1:
            # Check if both nodes received OK for same create ID at the same time
            if len(ok_keys) == 2:
                if ok_keys[0][0] != ok_keys[1][0]:
                    raise RuntimeError("The timestamp {} of this qubit state does not"
                                       "have a unique corresponding ok datapoint".format(timestamp))
            else:
                raise RuntimeError("The timestamp {} of this qubit state does not"
                                   "have a unique corresponding ok datapoint".format(timestamp))
        absolute_create_id, node_id, mhp_seq = ok_keys[0]

        # Add this qubit state datapoint to the data structure
        if "state" in creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq]:
            raise RuntimeError("OK for Absolute Create ID {},"
                               "Node ID {} and MHP Seq {} already has a state".format(absolute_create_id, node_id,
                                                                                      mhp_seq))
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
                            raise RuntimeError("The timestamp {} of this QBER datapoint"
                                               "does not have a unique corresponding ok datapoint".format(timestamp))
                    else:
                        raise RuntimeError("The timestamp {} of this QBER datapoint"
                                           "does not have a unique corresponding ok datapoint".format(timestamp))
                absolute_create_id, node_id, mhp_seq = ok_keys[0]

                # Add this QBER datapoint to the data structure
                if "QBER" in creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq]:
                    raise RuntimeError("OK for Absolute Create ID {},"
                                       "Node ID {} and MHP Seq {} already has a QBER".format(absolute_create_id,
                                                                                             node_id,
                                                                                             mhp_seq))
                creates_and_oks_by_create_id[absolute_create_id]["oks"][node_id][mhp_seq]["QBER"] = quberr_datapoint


def sort_data_by_request(filename):
    states_data = parse_table_data_from_sql(filename, "EGP_Qubit_States")
    quberr_data = parse_table_data_from_sql(filename, "EGP_QubErr")
    error_datapoints = list(map(lambda d: EGPErrorDataPoint(d), parse_table_data_from_sql(filename, "EGP_Errors")))
    expired_create_ids = [dp.create_id for dp in error_datapoints if dp.error_code == NodeCentricEGP.ERR_EXPIRE]

    creates_and_oks_by_create_id, ok_keys_by_timestamp = get_creates_and_oks_by_create_id(filename, expired_create_ids)

    add_qubit_states(states_data, creates_and_oks_by_create_id, ok_keys_by_timestamp)

    add_quberr(quberr_data, creates_and_oks_by_create_id, ok_keys_by_timestamp)

    return creates_and_oks_by_create_id, ok_keys_by_timestamp


def parse_thoughput(creates_and_oks_by_create_id, max_time, num_points=10000, time_window=1e9, min_time=0,
                    in_seconds=False):

    priorities = list(range(3))

    if max_time == 0:
        return {p: [(0, -1)] * num_points for p in priorities}

    timestamps_per_prio = {p: [] for p in priorities}
    for create_id, create_data in creates_and_oks_by_create_id.items():
        if not create_data["expired"]:
            priority = create_data["create"].priority
            for node_id, node_data in create_data["oks"].items():
                # Don't count OKs double
                if node_id == 0:
                    for mhp_seq, mhp_data in node_data.items():
                        ok = mhp_data["ok"]
                        timestamps_per_prio[priority].append(ok.timestamp)

    throughputs_per_prio = {}
    for priority, timestamps in timestamps_per_prio.items():
        timestamps = sorted(timestamps)
        time_diff = max_time - min_time
        shift = (time_diff - time_window) / (num_points - 1)
        if shift > time_window:
            logger.warning("Got to short time-window {} (s), making it {} (s)".format(time_window * 1e-9, shift * 1e-9))
            time_window = shift

        left_side = min_time
        position = 0
        throughputs = []
        for _ in range(num_points):
            right_side = left_side + time_window
            i = position
            num_oks_in_window = 0
            while i < len(timestamps):
                t = timestamps[i]
                if t < left_side:
                    position += 1
                elif t >= right_side:
                    break
                else:
                    num_oks_in_window += 1
                i += 1
            if in_seconds:
                throughputs.append((left_side * 1e-9, num_oks_in_window / time_window * 1e9))
            else:
                throughputs.append((left_side, num_oks_in_window / time_window))
            left_side += shift

        throughputs_per_prio[priority] = throughputs

    return throughputs_per_prio


def get_avg_std_num(numbers):
    return np.mean(numbers), np.std(numbers), len(numbers)


def add_metric_data(metrics, metric_name, avg_std_num):
    for i, tp in enumerate(["Avg", "Std", "Num"]):
        metrics[tp + metric_name] = avg_std_num[i]


def get_raw_metric_data(filename):
    # Get the correct datacollection version
    get_datacollection_version(filename)

    creates_and_oks_by_create_id, ok_keys_by_timestamp = sort_data_by_request(filename)

    ##########################
    # Nr OKs and outstanding #
    ##########################

    nr_oks_per_prio = {i: 0 for i in range(3)}
    nr_reqs_per_prio = {i: 0 for i in range(3)}
    nr_outstanding_req_per_prio = {i: 0 for i in range(3)}
    nr_outstanding_pairs_per_prio = {i: 0 for i in range(3)}
    nr_expired_req_per_prio = {i: 0 for i in range(3)}
    for create_id, create_data in creates_and_oks_by_create_id.items():
        priority = create_data["create"].priority
        if create_data["expired"]:
            nr_expired_req_per_prio[priority] += 1
        else:
            node_id = 0
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

    ################################
    # Fidelities, QBER and latency #
    ################################

    fids_per_prio = {}
    qber_per_prio = {}
    pair_latencies_per_prio_per_node = {}
    req_latencies_per_prio_per_node = {}
    scaled_req_latencies_per_prio_per_node = {}
    attempts_per_prio = {i: 0 for i in range(3)}
    cycles_per_attempt_per_prio = {i: [] for i in range(3)}
    priorities = []

    for create_id, request_data in creates_and_oks_by_create_id.items():
        if not request_data["expired"]:
            create_datapoint = request_data["create"]
            priority = create_datapoint.priority
            if priority not in priorities:
                priorities.append(priority)
            for node_id, node_oks in request_data["oks"].items():
                max_latency = -1
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
                    if latency > max_latency:
                        max_latency = latency
                        max_latency_time = ok_time
                    if priority not in pair_latencies_per_prio_per_node:
                        pair_latencies_per_prio_per_node[priority] = {}
                    if node_id not in pair_latencies_per_prio_per_node[priority]:
                        pair_latencies_per_prio_per_node[priority][node_id] = []
                    pair_latencies_per_prio_per_node[priority][node_id].append((ok_time, latency))

                    # Attempts
                    # Only collect for one node
                    if node_id == 0:
                        ok_datapoint = ok_data["ok"]
                        attempts_per_prio[priority] += ok_datapoint.attempts

                    # Cycles per attempt
                    ok_datapoint = ok_data["ok"]
                    cycles_per_attempt_per_prio[priority].append(ok_datapoint.used_cycles / ok_datapoint.attempts)

                num_pairs = create_datapoint.num_pairs
                if len(node_oks) == num_pairs:
                    if priority not in req_latencies_per_prio_per_node:
                        req_latencies_per_prio_per_node[priority] = {}
                        scaled_req_latencies_per_prio_per_node[priority] = {}
                    if node_id not in req_latencies_per_prio_per_node[priority]:
                        req_latencies_per_prio_per_node[priority][node_id] = []
                        scaled_req_latencies_per_prio_per_node[priority][node_id] = []
                    req_latencies_per_prio_per_node[priority][node_id].append((max_latency_time, max_latency))
                    scaled_req_latencies_per_prio_per_node[priority][node_id].append((max_latency_time,
                                                                                      max_latency / num_pairs))
    ###############
    # Matrix time #
    ###############

    # Simulation time
    additional_data_filename = filename[:-3] + "_additional_data.json"
    with open(additional_data_filename, 'r') as f:
        try:
            additional_data = json.load(f)
        except json.decoder.JSONDecodeError:
            total_matrix_time = 0
        else:
            total_matrix_time = additional_data["total_real_time"]

    ##############
    # Throughput #
    ##############

    throughputs_per_prio = parse_thoughput(creates_and_oks_by_create_id, total_matrix_time)

    all_raw_metric_data = {}
    all_raw_metric_data["nr_oks_per_prio"] = nr_oks_per_prio
    all_raw_metric_data["nr_reqs_per_prio"] = nr_reqs_per_prio
    all_raw_metric_data["nr_outstanding_req_per_prio"] = nr_outstanding_req_per_prio
    all_raw_metric_data["nr_outstanding_pairs_per_prio"] = nr_outstanding_pairs_per_prio
    all_raw_metric_data["nr_expired_req_per_prio"] = nr_expired_req_per_prio
    all_raw_metric_data["cycles_per_attempt_per_prio"] = cycles_per_attempt_per_prio
    all_raw_metric_data["fids_per_prio"] = fids_per_prio
    all_raw_metric_data["qber_per_prio"] = qber_per_prio
    all_raw_metric_data["pair_latencies_per_prio_per_node"] = pair_latencies_per_prio_per_node
    all_raw_metric_data["req_latencies_per_prio_per_node"] = req_latencies_per_prio_per_node
    all_raw_metric_data["scaled_req_latencies_per_prio_per_node"] = scaled_req_latencies_per_prio_per_node
    all_raw_metric_data["throughputs_per_prio"] = throughputs_per_prio

    return all_raw_metric_data


def get_metrics_from_single_file(filename):
    all_raw_metric_data = get_raw_metric_data(filename)
    nr_oks_per_prio = all_raw_metric_data["nr_oks_per_prio"]
    nr_reqs_per_prio = all_raw_metric_data["nr_reqs_per_prio"]
    nr_outstanding_req_per_prio = all_raw_metric_data["nr_outstanding_req_per_prio"]
    nr_outstanding_pairs_per_prio = all_raw_metric_data["nr_outstanding_pairs_per_prio"]
    # nr_expired_req_per_prio = all_raw_metric_data["nr_expired_req_per_prio"]
    cycles_per_attempt_per_prio = all_raw_metric_data["cycles_per_attempt_per_prio"]
    fids_per_prio = all_raw_metric_data["fids_per_prio"]
    qber_per_prio = all_raw_metric_data["qber_per_prio"]
    pair_latencies_per_prio_per_node = all_raw_metric_data["pair_latencies_per_prio_per_node"]
    req_latencies_per_prio_per_node = all_raw_metric_data["req_latencies_per_prio_per_node"]
    scaled_req_latencies_per_prio_per_node = all_raw_metric_data["scaled_req_latencies_per_prio_per_node"]
    throughputs_per_prio = all_raw_metric_data["throughputs_per_prio"]

    ##############
    # Throughput #
    ##############
    metric_throughput_per_prio = {}
    for prio, throughputs in throughputs_per_prio.items():
        tps = [tp[1] * 1e9 for tp in throughputs]
        metric_throughput_per_prio[prio] = get_avg_std_num(tps)

    ##########
    # Errors #
    ##########

    errors_data = parse_table_data_from_sql(filename, "EGP_Errors")
    num_errors = len(errors_data)
    num_errors_per_code = defaultdict(int)
    for e in errors_data:
        error_datapoint = EGPErrorDataPoint(e)
        num_errors_per_code[error_datapoint.error_code] += 1

    metric_fid_per_prio = {priority: get_avg_std_num(fids) for priority, fids in fids_per_prio.items()}

    metric_qber_per_prio = {}
    for priority, qbersxyz in qber_per_prio.items():
        metric_qber_per_prio[priority] = {basis: get_avg_std_num(qbers) if len(qbers) > 0 else (0, 0, 0)
                                          for basis, qbers in qbersxyz.items()}
        metric_qber_per_prio[priority]["fid"] = 1 - sum([qber[0]
                                                         for qber in metric_qber_per_prio[priority].values()]) / 2

    # Pair latency
    metric_pair_latencies_per_prio_per_node = {}
    for priority, latencies_per_node in pair_latencies_per_prio_per_node.items():
        metric_pair_latencies_per_prio_per_node[priority] = {}
        for node_id, latencies in latencies_per_node.items():
            latencies = [l[1] * 1e-9 for l in latencies]
            metric_pair_latencies_per_prio_per_node[priority][node_id] = get_avg_std_num(latencies)
    # Request latency
    metric_req_latencies_per_prio_per_node = {}
    for priority, latencies_per_node in req_latencies_per_prio_per_node.items():
        metric_req_latencies_per_prio_per_node[priority] = {}
        for node_id, latencies in latencies_per_node.items():
            latencies = [l[1] * 1e-9 for l in latencies]
            metric_req_latencies_per_prio_per_node[priority][node_id] = get_avg_std_num(latencies)
    # Scaled request Latency
    metric_scaled_req_latencies_per_prio_per_node = {}
    for priority, latencies_per_node in scaled_req_latencies_per_prio_per_node.items():
        metric_scaled_req_latencies_per_prio_per_node[priority] = {}
        for node_id, latencies in latencies_per_node.items():
            latencies = [l[1] * 1e-9 for l in latencies]
            metric_scaled_req_latencies_per_prio_per_node[priority][node_id] = get_avg_std_num(latencies)

    avg_cycles_per_attempt_per_prio = {priority: sum(c_p_a) / len(c_p_a) if len(c_p_a) > 0 else None
                                       for priority, c_p_a in cycles_per_attempt_per_prio.items()}

    #################
    # Queue Lengths #
    #################
    queue_ids = range(3)

    raw_all_queue_data = {qid: parse_table_data_from_sql(filename,
                                                         "EGP_Local_Queue_A_{}".format(qid)) for qid in queue_ids}
    all_queue_lengths = {}
    times_non_idle = {}
    for qid, raw_queue_data in raw_all_queue_data.items():
        queue_data = parse_raw_queue_data(raw_queue_data)
        all_queue_lengths[qid] = queue_data[0]
        times_non_idle[qid] = queue_data[-1]
    all_queue_lengths = {qid: parse_raw_queue_data(raw_queue_data)[0]
                         for qid, raw_queue_data in raw_all_queue_data.items()}
    all_avg_queue_lengths = {qid: sum(queue_lengths) / len(queue_lengths)
                             for qid, queue_lengths in all_queue_lengths.items()}

    ###############
    # Matrix time #
    ###############

    # Simulation time
    additional_data_filename = filename[:-3] + "_additional_data.json"
    with open(additional_data_filename, 'r') as f:
        try:
            additional_data = json.load(f)
        except json.decoder.JSONDecodeError:
            total_matrix_time = 0
        else:
            total_matrix_time = additional_data["total_real_time"]

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
        add_metric_data(metrics, "Throughp_Prio{} (1/s)".format(prio_name), metric_throughput_per_prio[priority])
        metrics["AvgCyc_per_Att_Prio{}".format(prio_name)] = avg_cycles_per_attempt_per_prio[priority]

        for lat_name, lat_data in zip(["Pair", "Req", "ScaledReq"], [metric_pair_latencies_per_prio_per_node,
                                                                     metric_req_latencies_per_prio_per_node,
                                                                     metric_scaled_req_latencies_per_prio_per_node]):
            try:
                metric_latencies_per_node = lat_data[priority]
            except KeyError:
                metric_latencies_per_node = {}
            for node_id in range(2):
                try:
                    add_metric_data(metrics, "{}Laten_Prio{}_NodeID{} (s)".format(lat_name, prio_name, node_id),
                                    metric_latencies_per_node[node_id])
                except KeyError:
                    add_metric_data(metrics, "{}Laten_Prio{}_NodeID{} (s)".format(lat_name, prio_name, node_id),
                                    [None] * 3)

        if priority < 2:
            try:
                add_metric_data(metrics, "Fid_Prio{}".format(prio_name), metric_fid_per_prio[priority])
            except KeyError:
                add_metric_data(metrics, "Fid_Prio{}".format(prio_name), [None] * 3)
        else:
            try:
                metrics["AvgFid_Prio{}".format(prio_name)] = metric_qber_per_prio[priority]["fid"]
                metrics["StdFid_Prio{}".format(prio_name)] = None
                metrics["NumFid_Prio{}".format(prio_name)] = None
            except KeyError:
                add_metric_data(metrics, "Fid_Prio{}".format(prio_name), [None] * 3)
            for basis in ["X", "Y", "Z"]:
                try:
                    add_metric_data(metrics, "QBER{}_Prio{}".format(basis, prio_name),
                                    metric_qber_per_prio[priority]["{}".format(basis)])
                except KeyError:
                    add_metric_data(metrics, "QBER{}_Prio{}".format(basis, prio_name), [None] * 3)

    metrics["NumErrors"] = num_errors

    metrics["ErrorCodes"] = "".join(["{}({}), ".format(error_code, number)
                                     for error_code, number in num_errors_per_code.items()])[:-2]

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
        metrics["AvgQueueLen_QID{}".format(qid)] = avg_queue_length
        metrics["QueueIdle_QID{}".format(qid)] = time_idle

    return metrics


def main(results_folder):
    all_metrics = []
    for entry in sorted(os.listdir(results_folder)):
        if entry.endswith(".db"):
            scenario_key = entry.split("_key_")[1].split("_run_")[0]
            run_index = entry.split("_run_")[1].split(".")[0]
            run_ID = scenario_key + run_index
            print(run_ID)
            metrics = get_metrics_from_single_file(os.path.join(results_folder, entry))
            metrics["Name"] = run_ID
            all_metrics.append(metrics)

    if len(all_metrics) > 0:
        csv_folder = os.path.join(results_folder, "metrics")
        if os.path.exists(csv_folder):
            for f in os.listdir(csv_folder):
                f_path = os.path.join(csv_folder, f)
                if os.path.isfile(f_path):
                    if not f.startswith("."):
                        os.remove(f_path)
        else:
            os.mkdir(csv_folder)

        prio_names = ["NL", "CK", "MD"]
        tps = ["Avg", "Std", "Num"]
        fieldnames_per_file = {
            "Times": (["Name", "TotalMatrixT (s)"] + ["QueueIdle_QID{}".format(qid) for qid in range(3)] +
                      ["AvgCyc_per_Att_Prio{}".format(p) for p in prio_names]),
            "Number": (["Name", "NumErrors", "ErrorCodes"] +
                       sum([["Nr{}_Prio{}".format(what_nr, p) for p in prio_names]
                            for what_nr in ["Reqs", "OKs", "RemReq", "RemPairs"]], [])),
            "Throughput": (["Name"] + sum([["{}Throughp_Prio{} (1/s)".format(tp, p)
                                            for p in prio_names] for tp in tps], [])),
            "PairLatency": (["Name"] + sum([sum([["{}PairLaten_Prio{}_NodeID{} (s)".format(tp, p, node)
                                                  for p in prio_names] for node in range(2)], []) for tp in tps], [])),
            "ReqLatency": (["Name"] + sum([sum([["{}ReqLaten_Prio{}_NodeID{} (s)".format(tp, p, node)
                                                 for p in prio_names] for node in range(2)], []) for tp in tps], [])),
            "ScaledReqLatency": (["Name"] + sum([sum([["{}ScaledReqLaten_Prio{}_NodeID{} (s)".format(tp, p, node)
                                                       for p in prio_names]
                                                      for node in range(2)], []) for tp in tps], [])),
            "Fidelity": (["Name"] + sum([["{}Fid_Prio{}".format(tp, p)
                                          for p in prio_names] + ["{}QBER{}_PrioMD".format(tp, basis)
                                                                  for basis in ["X", "Y", "Z"]] for tp in tps], [])),
            "QueueLens": ["Name"] + ["AvgQueueLen_QID{}".format(qid) for qid in range(3)]
        }

        # Check that no metrics are missing
        all_fieldnames = sum(fieldnames_per_file.values(), [])
        remaining_fieldnames = [fieldname for fieldname in all_metrics[0].keys() if fieldname not in all_fieldnames]
        if not len(remaining_fieldnames) == 0:
            print(remaining_fieldnames)

        for sheet, fieldnames in fieldnames_per_file.items():
            csv_filename = os.path.join(csv_folder, sheet) + ".csv"
            with open(csv_filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                writer.writeheader()
                sheet_metrics = [{key: value for key, value in metr.items() if key in fieldnames}
                                 for metr in all_metrics]
                writer.writerows(sheet_metrics)

        # Make excel file
        create_excel_file(csv_folder)


def create_excel_file(csv_folder):
    workbook = Workbook(os.path.join(csv_folder, "metrics.xlsx"))
    for csv_file_path in glob.glob(os.path.join(csv_folder, '*.csv')):
        csv_filename = os.path.basename(csv_file_path)
        worksheet = workbook.add_worksheet(csv_filename[:-4])
        with open(csv_file_path, 'rt', encoding='utf8') as f:
            reader = csv.reader(f)
            for r, row in enumerate(reader):
                for c, col in enumerate(row):
                    try:
                        int(col)
                    except ValueError:
                        try:
                            float(col)
                        except ValueError:
                            worksheet.write(r, c, col)
                        else:
                            worksheet.write_number(r, c, float(col))
                    else:
                        worksheet.write_number(r, c, int(col))
    workbook.close()


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--results_folder', required=True, type=str,
                        help="The path to the results folder to consider.")

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(**vars(args))
