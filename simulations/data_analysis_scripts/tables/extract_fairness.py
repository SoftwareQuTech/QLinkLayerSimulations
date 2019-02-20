#######################################################################################################################
# NOTE: This is very similar to simulations.generate_metrics_file with the difference that metrics are seperated      #
# depending on the requesting node the be able to extract fairness metrics. Not recommended to be used for anything   #
# else than this.                                                                                                     #
#######################################################################################################################

import os
import json
import numpy as np
from argparse import ArgumentParser
from collections import defaultdict

from easysquid.toolbox import logger
from simulations.analysis_sql_data import parse_table_data_from_sql, calc_fidelity, parse_raw_queue_data,\
    get_datacollection_version
from qlinklayer.datacollection import EGPErrorDataPoint
from simulations.generate_metrics_file import sort_data_by_request, get_avg_std_num, add_metric_data


def parse_thoughput(creates_and_oks_by_create_id, max_time, num_points=10000, time_window=1e9, min_time=0, in_seconds=False):
    priorities = list(range(3))
    nodes = ["A", "B"]
    timestamps_per_prio = {node: {p: [] for p in priorities} for node in nodes}
    for create_id, create_data in creates_and_oks_by_create_id.items():
        if not create_data["expired"]:
            priority = create_data["create"].priority
            origin_id = create_data["create"].node_id
            origin = nodes[origin_id]
            for node_id, node_data in create_data["oks"].items():
                # Don't count OKs double
                if node_id == 0:
                    for mhp_seq, mhp_data in node_data.items():
                        ok = mhp_data["ok"]
                        timestamps_per_prio[origin][priority].append(ok.timestamp)

    throughputs_per_prio = {}
    for node, node_timestamps in timestamps_per_prio.items():
        throughputs_per_prio[node] = {}
        for priority, timestamps in node_timestamps.items():
            timestamps = sorted(timestamps)
            if len(timestamps) == 0:
                throughputs = [(None, 0)] * num_points
            elif len(timestamps) == 1:
                import pdb
                pdb.set_trace()
                raise RuntimeError()
            else:
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

            throughputs_per_prio[node][priority] = throughputs

    return throughputs_per_prio


def get_metrics_from_single_file(filename):
    # Get the correct datacollection version
    get_datacollection_version(filename)

    creates_and_oks_by_create_id, ok_keys_by_timestamp = sort_data_by_request(filename)

    ##########################
    # Nr OKs and outstanding #
    ##########################

    nodes = ["A", "B"]

    nr_oks_per_prio = {node: {i: 0 for i in range(3)} for node in nodes}
    nr_reqs_per_prio = {node: {i: 0 for i in range(3)} for node in nodes}
    nr_outstanding_req_per_prio = {node: {i: 0 for i in range(3)} for node in nodes}
    nr_outstanding_pairs_per_prio = {node: {i: 0 for i in range(3)} for node in nodes}
    nr_expired_req_per_prio = {node: {i: 0 for i in range(3)} for node in nodes}
    for create_id, create_data in creates_and_oks_by_create_id.items():
        priority = create_data["create"].priority
        origin_id = create_data["create"].node_id
        origin = nodes[origin_id]
        if create_data["expired"]:
            nr_expired_req_per_prio[origin][priority] += 1
        else:
            node_id = 0
            nr_reqs_per_prio[origin][priority] += 1
            if node_id in create_data["oks"]:
                oks = create_data["oks"][node_id]
                nr_oks_per_prio[origin][priority] += len(oks)

                pairs_left = create_data["create"].num_pairs - len(oks)
                if pairs_left > 0:
                    nr_outstanding_req_per_prio[origin][priority] += 1
                    nr_outstanding_pairs_per_prio[origin][priority] += pairs_left
            else:
                nr_outstanding_req_per_prio[origin][priority] += 1
                nr_outstanding_pairs_per_prio[origin][priority] += create_data["create"].num_pairs

    ##########
    # Errors #
    ##########

    errors_data = parse_table_data_from_sql(filename, "EGP_Errors")
    num_errors = len(errors_data)
    num_errors_per_code = defaultdict(int)
    for e in errors_data:
        error_datapoint = EGPErrorDataPoint(e)
        num_errors_per_code[error_datapoint.error_code] += 1

    ################################
    # Fidelities, QBER and latency #
    ################################

    fids_per_prio = {node: {} for node in nodes}
    qber_per_prio = {node: {} for node in nodes}
    pair_latencies_per_prio_per_node = {}
    req_latencies_per_prio_per_node = {}
    scaled_req_latencies_per_prio_per_node = {origin: {} for origin in nodes}
    attempts_per_prio = {i: 0 for i in range(3)}
    cycles_per_attempt_per_prio = {i: [] for i in range(3)}
    priorities = []

    for create_id, request_data in creates_and_oks_by_create_id.items():
        if not request_data["expired"]:
            create_datapoint = request_data["create"]
            priority = create_datapoint.priority
            origin_id = create_datapoint.node_id
            origin = nodes[origin_id]
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
                        if priority not in fids_per_prio[origin]:
                            fids_per_prio[origin][priority] = [fid]
                        else:
                            fids_per_prio[origin][priority].append(fid)

                    # QBER
                    if "QBER" in ok_data:
                        qber_datapoint = ok_data["QBER"]
                        qberxyz = {"X": qber_datapoint.x_err, "Y": qber_datapoint.y_err, "Z": qber_datapoint.z_err}
                        if priority not in qber_per_prio[origin]:
                            qber_per_prio[origin][priority] = {"X": [], "Y": [], "Z": []}
                        for basis, qber in qberxyz.items():
                            if qber != -1:
                                qber_per_prio[origin][priority][basis].append(qber)

                    # Latency
                    create_time = create_datapoint.create_time
                    ok_time = ok_data["ok"].timestamp
                    latency = (ok_time - create_time)
                    if latency > max_latency:
                        max_latency = latency
                    if priority not in pair_latencies_per_prio_per_node:
                        pair_latencies_per_prio_per_node[priority] = {}
                    if node_id not in pair_latencies_per_prio_per_node[priority]:
                        pair_latencies_per_prio_per_node[priority][node_id] = []
                    pair_latencies_per_prio_per_node[priority][node_id].append(latency)

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
                    if priority not in scaled_req_latencies_per_prio_per_node[origin]:
                        scaled_req_latencies_per_prio_per_node[origin][priority] = {}
                    if node_id not in req_latencies_per_prio_per_node[priority]:
                        req_latencies_per_prio_per_node[priority][node_id] = []
                    if node_id not in scaled_req_latencies_per_prio_per_node[origin][priority]:
                        scaled_req_latencies_per_prio_per_node[origin][priority][node_id] = []
                    req_latencies_per_prio_per_node[priority][node_id].append(max_latency)
                    scaled_req_latencies_per_prio_per_node[origin][priority][node_id].append(max_latency / num_pairs)

    metric_fid_per_prio = {node: {priority: get_avg_std_num(fids) for priority, fids in fids_per_prio[node].items()} for node in nodes}

    metric_qber_per_prio = {}
    for node, qber_per_prio_only in qber_per_prio.items():
        metric_qber_per_prio[node] = {}
        for priority, qbersxyz in qber_per_prio_only.items():
            metric_qber_per_prio[node][priority] = {basis: get_avg_std_num(qbers) if len(qbers) > 0 else 0 for basis, qbers in qbersxyz.items()}
            metric_qber_per_prio[node][priority]["fid"] = 1 - sum([qber[0] for qber in metric_qber_per_prio[node][priority].values()]) / 2

    # Pair latency
    metric_pair_latencies_per_prio_per_node = {}
    for priority, latencies_per_node in pair_latencies_per_prio_per_node.items():
        metric_pair_latencies_per_prio_per_node[priority] = {}
        for node_id, latencies in latencies_per_node.items():
            latencies = [l * 1e-9 for l in latencies]
            metric_pair_latencies_per_prio_per_node[priority][node_id] = get_avg_std_num(latencies)
    # Request latency
    metric_req_latencies_per_prio_per_node = {}
    for priority, latencies_per_node in req_latencies_per_prio_per_node.items():
        metric_req_latencies_per_prio_per_node[priority] = {}
        for node_id, latencies in latencies_per_node.items():
            latencies = [l * 1e-9 for l in latencies]
            metric_req_latencies_per_prio_per_node[priority][node_id] = get_avg_std_num(latencies)
    # Scaled request Latency
    metric_scaled_req_latencies_per_prio_per_node = {origin: {} for origin in nodes}
    for origin, node_latencies_per_node in scaled_req_latencies_per_prio_per_node.items():
        for priority, latencies_per_node in node_latencies_per_node.items():
            metric_scaled_req_latencies_per_prio_per_node[origin][priority] = {}
            for node_id, latencies in latencies_per_node.items():
                latencies = [l * 1e-9 for l in latencies]
                metric_scaled_req_latencies_per_prio_per_node[origin][priority][node_id] = get_avg_std_num(latencies)

    avg_cycles_per_attempt_per_prio = {priority: sum(c_p_a) / len(c_p_a) if len(c_p_a) > 0 else None for priority, c_p_a in cycles_per_attempt_per_prio.items()}

    #################
    # Queue Lengths #
    #################
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

    ###############
    # Matrix time #
    ###############

    # Simulation time
    additional_data_filename = filename[:-3] + "_additional_data.json"
    with open(additional_data_filename, 'r') as f:
        additional_data = json.load(f)
    total_matrix_time = additional_data["total_real_time"]

    ##############
    # Throughput #
    ##############

    throughputs_per_prio = parse_thoughput(creates_and_oks_by_create_id, total_matrix_time)
    metric_throughput_per_prio = {}
    for node, node_throughputs in throughputs_per_prio.items():
        metric_throughput_per_prio[node] = {}
        for prio, throughputs in node_throughputs.items():
            tps = [tp[1] for tp in throughputs]
            metric_throughput_per_prio[node][prio] = get_avg_std_num(tps)

    ########################
    # Outstanding requests #
    ########################

    #############################
    # Construct dict of metrics #
    #############################

    metrics = {}
    for origin in nodes:
        for priority in range(3):
            if priority == 0:
                prio_name = "NL"
            elif priority == 1:
                prio_name = "CK"
            elif priority == 2:
                prio_name = "MD"
            else:
                raise RuntimeError("Unkown priority {}".format(priority))

            metrics["NrReqs_Prio{}_Origin{}".format(prio_name, origin)] = nr_reqs_per_prio[origin][priority]
            metrics["NrOKs_Prio{}_Origin{}".format(prio_name, origin)] = nr_oks_per_prio[origin][priority]
            metrics["NrRemReq_Prio{}_Origin{}".format(prio_name, origin)] = nr_outstanding_req_per_prio[origin][priority]
            metrics["NrRemPairs_Prio{}_Origin{}".format(prio_name, origin)] = nr_outstanding_pairs_per_prio[origin][priority]
            add_metric_data(metrics, "Throughp_Prio{}_Origin{} (1/s)".format(prio_name, origin), metric_throughput_per_prio[origin][priority])
            metrics["AvgCyc_per_Att_Prio{}".format(prio_name)] = avg_cycles_per_attempt_per_prio[priority]

            for lat_name, lat_data in zip(["Pair", "Req"], [metric_pair_latencies_per_prio_per_node, metric_req_latencies_per_prio_per_node]):
                try:
                    metric_latencies_per_node = lat_data[priority]
                except KeyError:
                    metric_latencies_per_node = {}
                for node_id in range(2):
                    try:
                        add_metric_data(metrics, "{}Laten_Prio{}_NodeID{} (s)".format(lat_name, prio_name, node_id), metric_latencies_per_node[node_id])
                    except KeyError:
                        add_metric_data(metrics, "{}Laten_Prio{}_NodeID{} (s)".format(lat_name, prio_name, node_id), [None] * 3)

            for node_id in range(2):
                try:
                    add_metric_data(metrics, "ScaledReqLaten_Prio{}_NodeID{}_Origin{} (s)".format(prio_name, node_id, origin), metric_scaled_req_latencies_per_prio_per_node[origin][priority][node_id])
                except KeyError:
                    add_metric_data(metrics, "ScaledReqLaten_Prio{}_NodeID{}_Origin{} (s)".format(prio_name, node_id, origin), [None] * 3)

            if priority < 2:
                try:
                    add_metric_data(metrics, "Fid_Prio{}_Origin{}".format(prio_name, origin), metric_fid_per_prio[origin][priority])
                except KeyError:
                    add_metric_data(metrics, "Fid_Prio{}_Origin{}".format(prio_name, origin), [None] * 3)
            else:
                    try:
                        metrics["AvgFid_Prio{}_Origin{}".format(prio_name, origin)] = metric_qber_per_prio[origin][priority]["fid"]
                        metrics["StdFid_Prio{}_Origin{}".format(prio_name, origin)] = None
                        metrics["NumFid_Prio{}_Origin{}".format(prio_name, origin)] = None
                    except KeyError:
                        add_metric_data(metrics, "Fid_Prio{}_Origin{}".format(prio_name, origin), [None] * 3)
                    for basis in ["X", "Y", "Z"]:
                        try:
                            add_metric_data(metrics, "QBER{}_Prio{}_Origin{}".format(basis, prio_name, origin), metric_qber_per_prio[origin][priority]["{}".format(basis)])
                        except KeyError:
                            add_metric_data(metrics, "QBER{}_Prio{}_Origin{}".format(basis, prio_name, origin), [None] * 3)

    metrics["NumErrors"] = num_errors

    metrics["ErrorCodes"] = "".join(["{}({}), ".format(error_code, number) for error_code, number in num_errors_per_code.items()])[:-2]

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
    req_freqs = ["Low", "High", "Ultra", "Mixed"]
    max_fid_diff = {r: (-1, None) for r in req_freqs}
    max_throughput_diff = {r: (-1, None) for r in req_freqs}
    max_latency_diff = {r: (-1, None) for r in req_freqs}
    max_oks_diff = {r: (-1, None) for r in req_freqs}
    counter = 0
    for entry in sorted(os.listdir(results_folder)):
        if entry.endswith(".db"):
            try:
                scenario_key = entry.split("_key_")[1].split("_run_")[0]
            except IndexError:
                import pdb
                pdb.set_trace()
            # if scenario_key == "LAB_NC_NC_MD_max3_req_frac_high_origin_originA_weights_FIFO":
            # if (("originAB" in scenario_key) or ("mix" in scenario_key)) and "max255" not in scenario_key:
            if True:
                counter += 1
                if counter > 10:
                    break
                if "req_frac_low" in scenario_key:
                    req_freq = "Low"
                elif "req_frac_high" in scenario_key:
                    req_freq = "High"
                elif "req_frac_ultra" in scenario_key:
                    req_freq = "Ultra"
                elif "mix" in scenario_key:
                    req_freq = "Mixed"
                else:
                    raise RuntimeError()
                print(scenario_key)
                metrics = get_metrics_from_single_file(os.path.join(results_folder, entry))

                prio_names = ["NL", "CK", "MD"]
                fidelity_keys = ["AvgFid_Prio{}_Origin".format(p) for p in prio_names]
                for k in fidelity_keys:
                    try:
                        f_A = metrics[k + "A"]
                    except KeyError:
                        f_A = None
                    try:
                        f_B = metrics[k + "B"]
                    except KeyError:
                        f_B = None
                    if (f_A is None) and (f_B is None):
                        pass
                    elif (f_A is not None) and (f_B is not None):
                        if f_A * f_B == 0:
                            rel_diff = 0
                        else:
                            rel_diff = np.abs((f_A - f_B) / max(f_A, f_B))
                        if rel_diff > max_fid_diff[req_freq][0]:
                            max_fid_diff[req_freq] = rel_diff, scenario_key
                    else:
                        import pdb
                        pdb.set_trace()
                        raise RuntimeError("one f is None")

                throughput_keys = ["AvgThroughp_Prio{}_Origin".format(p) for p in prio_names]
                for k in throughput_keys:
                    try:
                        tp_A = metrics[k + "A (1/s)"]
                    except KeyError:
                        tp_A = None
                    try:
                        tp_B = metrics[k + "B (1/s)"]
                    except KeyError:
                        tp_B = None
                    if (tp_A is None) and (tp_B is None):
                        pass
                    elif (tp_A is not None) and (tp_B is not None):
                        if tp_A * tp_B == 0:
                            rel_diff = 0
                        else:
                            rel_diff = np.abs((tp_A - tp_B) / max(tp_A, tp_B))
                        if rel_diff > max_throughput_diff[req_freq][0]:
                            max_throughput_diff[req_freq] = rel_diff, scenario_key
                    else:
                        raise RuntimeError("one tp is None")

                num_OKs_keys = ["NrOKs_Prio{}_Origin".format(p) for p in prio_names]
                for k in num_OKs_keys:
                    try:
                        oks_A = metrics[k + "A"]
                    except:
                        oks_A = None
                    try:
                        oks_B = metrics[k + "B"]
                    except KeyError:
                        oks_B = None
                    if (oks_A is None) and (oks_B is None):
                        pass
                    elif (oks_A is not None) and (oks_B is not None):
                        if oks_A * oks_B == 0:
                            rel_diff = 0
                        else:
                            rel_diff = np.abs((oks_A - oks_B) / max(oks_A, oks_B))
                        if rel_diff > max_oks_diff[req_freq][0]:
                            max_oks_diff[req_freq] = rel_diff, scenario_key
                    else:
                        raise RuntimeError("one oks is None")

                num_lat_keys = ["AvgScaledReqLaten_Prio{}_NodeID{}_Origin".format(p, node_id) for p in prio_names for node_id in range(2)]
                for k in num_lat_keys:
                    try:
                        lat_A = metrics[k + "A (s)"]
                    except KeyError:
                        lat_A = None
                    try:
                        lat_B = metrics[k + "B (s)"]
                    except KeyError:
                        lat_B = None
                    if (lat_A is None) and (lat_B is None):
                        pass
                    elif (lat_A is not None) and (lat_B is not None):
                        if lat_A * lat_B == 0:
                            rel_diff = 0
                        else:
                            rel_diff = np.abs((lat_A - lat_B) / max(lat_A, lat_B))
                        if rel_diff > max_latency_diff[req_freq][0]:
                            max_latency_diff[req_freq] = rel_diff, scenario_key
                    else:
                        raise RuntimeError("one oks is None")

                metrics["Name"] = scenario_key
                all_metrics.append(metrics)

    for r in req_freqs:
        print("Req freq: {}".format(r))
        print("     Max Fidelity diff: {}".format(max_fid_diff[r]))
        print("     Max Throughput diff: {}".format(max_throughput_diff[r]))
        print("     Max NumOKs diff: {}".format(max_oks_diff[r]))
        print("     Max Latency diff: {}".format(max_latency_diff[r]))
        print("")
    return


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--results_folder', required=True, type=str,
                        help="The path to the results folder to consider.")

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(**vars(args))

