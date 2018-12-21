import sqlite3
from argparse import ArgumentParser
from collections import defaultdict
import numpy as np
from easysquid.toolbox import logger
from qlinklayer.datacollection import EGPCreateDataPoint, EGPOKDataPoint, EGPStateDataPoint, \
    EGPQubErrDataPoint, EGPLocalQueueDataPoint
from netsquid.simutil import SECOND
import json
import math
import matplotlib.pyplot as plt
import os
import sys


class printer:
    def __init__(self, results_path, save_output=False, analysis_folder=None):
        """
        Class used to either print data information from simulation or save to file.
        :param results_path: str
            Path to results folder
        :param save_output: bool
            Whether output should be saved to file or printed to console
        :param analysis_folder: str
            Optional, specify path of analysis folder
        """
        self._results_path = results_path
        self._save_output = save_output
        self._analysis_folder = analysis_folder

    def print(self, to_print):
        """
        Print or save to file
        :param to_print: str
            What to output
        :return: None
        """
        if not self._save_output:
            print(to_print)
        else:
            if self._analysis_folder is not None:
                if not os.path.exists(self._analysis_folder):
                    os.makedirs(self._analysis_folder)
                with open(self._analysis_folder + "/analysis_output.txt", 'a') as out_file:
                    out_file.write(to_print + "\n")
            else:
                with open(self._results_path[:-3] + "_analysis_output.txt", 'a') as out_file:
                    out_file.write(to_print + "\n")


def _check_table_name(table_name, base_table_name):
    """
    Checks if (table_name == (base_table_name + "n")) where n is an integer
    :param table_name: str
    :param base_table_name: str
    :return: bool
    """
    # Split of last part of table_name
    split_table_name = table_name.split('_')
    table = "_".join(split_table_name[:-1])
    number = split_table_name[-1]
    try:
        int(number)
        is_number = True
    except ValueError:
        is_number = False
    return (table == base_table_name) and is_number


def parse_table_data_from_sql(results_path, base_table_name, max_real_time=None):
    """
    Parses data from all tables with name (base_table_name + "n") where n is an integer.
    If base_table_name is a list of str then data from each table is put in a dictionary with keys being
    the base table names.
    :param results_path: str
        Path to the sql-file (file.db)
    :param base_table_name: str or list of str
        Name of the tables
    :param max_real_time: float or None
        If specified, don't include any entries after max_real_time
    :return: list of any or dict of list of any
        Concatenated list of all entries in the corresponding tables
    """
    if isinstance(base_table_name, list):
        data_dict = {}
        for btn in base_table_name:
            data_dict[btn] = parse_table_data_from_sql(results_path, btn)
        return data_dict
    else:
        try:
            conn = sqlite3.connect(results_path)
        except sqlite3.OperationalError:
            logger.error("sqlite3 could not open the file {}, check that the path is correct".format(results_path))
            return None
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        all_tables = [t[0] for t in c.fetchall()]

        # Get the relevant table_names
        table_names = list(filter(lambda table_name: _check_table_name(table_name, base_table_name), all_tables))

        table_data = []

        for table_name in table_names:
            c.execute("SELECT * FROM {}".format(table_name))
            table_data += c.fetchall()

        if max_real_time is not None:
            table_data = [entry for entry in table_data if (entry[0] < max_real_time)]
        return table_data


def parse_request_data_from_sql(results_path, max_real_time=None):
    """
    Parses collected request/ok data points from the sql database
    :param results_path: str
        Path to directory containing request.log
    :param max_real_time: float or None
        If specified, don't include any entries after max_real_time
    :return: dict, list, dict, list, int
        requests - dict of key (createID, sourceID, otherID)
                           value [sourceID, otherID, numPairs, createID, createTime,maxTime]
                 These are successfully submitted request
        rejected_requests - list of [sourceID, otherID, numPairs, timeStamp] of rejected requests
        gens - dict of key nodeID
                    value dict of key (createID, sourceID, otherID)
                       value list of [nodeID, createID, sourceID, otherID, mhpSeq, createTime, attempts]
                 Generations corresponding to successfully submitted requests
        all_gens - dict of key nodeID
                        value list of (nodeID, createTime, attempts, (createID, sourceID, otherID, mhpSeq))
        total_requested_pairs - Number of pairs requested (successful + unsuccessful requests)
    """
    # Get the create and ok data
    data_dct = parse_table_data_from_sql(results_path, ["EGP_Creates", "EGP_OKs"], max_real_time=max_real_time)
    creates_data = data_dct["EGP_Creates"]
    oks_data = data_dct["EGP_OKs"]

    # Parse the create data
    requests = {}
    rejected_requests = []
    total_requested_pairs = 0
    for entry in creates_data:
        data_point = EGPCreateDataPoint(entry)

        total_requested_pairs += data_point.num_pairs

        if data_point.create_id is not None and data_point.create_time is not None:
            req_id = data_point.create_id, data_point.node_id, data_point.other_id
            req_data = [data_point.node_id, data_point.other_id, data_point.num_pairs, data_point.create_id,
                        data_point.create_time, data_point.max_time]
            requests[req_id] = req_data
        else:
            req_data = [data_point.node_id, data_point.other_id, data_point.num_pairs, data_point.timestamp]
            rejected_requests.append(req_data)

    # Parse the ok data
    gens = {}
    all_gens = {}
    # recorded_mhp_seqs = []
    for entry in oks_data:
        data_point = EGPOKDataPoint(entry)
        node_id = data_point.node_id
        req_id = data_point.create_id, data_point.origin_id, data_point.other_id
        mhp_seq = data_point.mhp_seq
        gen_info = data_point.create_time, data_point.attempts

        if node_id in gens:
            gens[node_id][req_id].append((node_id,) + req_id + (mhp_seq,) + gen_info)
        else:
            gens[node_id] = defaultdict(list)
            gens[node_id][req_id].append((node_id,) + req_id + (mhp_seq,) + gen_info)
        if node_id in all_gens:
            all_gens[node_id].append((node_id,) + gen_info + req_id + (mhp_seq,))
        else:
            all_gens[node_id] = []
            all_gens[node_id].append((node_id,) + gen_info + req_id + (mhp_seq,))

    # For consistent output, create empty dict if no gens
    if len(gens) == 0:
        gens = {0: defaultdict(list), 1: defaultdict(list)}
    if len(all_gens) == 0:
        all_gens = {0: [], 1: []}

    return (requests, rejected_requests), (gens, all_gens), total_requested_pairs


def get_attempt_data(all_gens):
    """
    Extracts the attempt data of succesful generations
        all_gens - dict of key nodeID
                        value list of (nodeID, createTime, attempts, (createID, sourceID, otherID, mhpSeq))
    :return: dict of dicts
        A dict with keys being nodeIDs and values being dictinoaries of the form
                    {Entanglement ID: Number of generation attempts}
    """
    gen_attempts = {}
    for nodeID, all_node_gens in all_gens.items():
        for gen in all_node_gens:
            attempts = gen[2]
            ent_id = gen[-3:]
            key = "{}_{}_{}".format(*ent_id)

            if nodeID in gen_attempts:
                gen_attempts[nodeID][key] = attempts
            else:
                gen_attempts[nodeID] = {key: attempts}

    # For consistent output, create empty dict if no gens
    if len(gen_attempts) == 0:
        gen_attempts = {0: {}, 1: {}}
    if len(gen_attempts) == 1:
        if list(gen_attempts.keys())[0] == 0:
            gen_attempts[1] = {}
        else:
            gen_attempts[0] = {}

    return gen_attempts


def parse_fidelities_from_sql(results_path, max_real_time=None):
    """
    Parses quantum states from SQL file and computes fidelities to the state 1/sqrt(2)(|01>+|10>)
    :param results_path: The path to the SQL file
    :type results_path: str
    :param max_real_time: float or None
        If specified, don't include any entries after max_real_time
    :return: Fidelities of the generated states
    :rtype: list of float
    """
    # Get the states_data
    states_data = parse_table_data_from_sql(results_path, "EGP_Qubit_States", max_real_time=max_real_time)

    # Parse the states data to compute fidelities
    fidelities = []
    ts = []  # time associated to the fidelity (used for plotting)
    for entry in states_data:
        data_point = EGPStateDataPoint(entry)
        timestamp = data_point.timestamp
        ts.append(timestamp)
        density_matrix = data_point.density_matrix
        # print("dm: {}".format(density_matrix))
        # print("fid: {}".format(calc_fidelity(density_matrix)))
        # print("")
        fidelities.append(calc_fidelity(density_matrix))

    return fidelities


def calc_fidelity(d_matrix):
    """
    Computes fidelity to the state 1/sqrt(2)(|01>+|10>)
    :param d_matrix: Density matrix
    :type d_matrix: :obj:`numpy.matrix`
    :return: The fidelity
    :rtype: float
    """
    psi = np.matrix([[0, 1, 1, 0]]).transpose() / np.sqrt(2)
    return np.real((psi.H * d_matrix * psi)[0, 0])


def parse_quberr_from_sql(results_path, max_real_time=None):
    """
    Parses quberrfrom SQL file.
    :param results_path: The path to the SQL file
    :type results_path: str
    :param max_real_time: float or None
        If specified, don't include any entries after max_real_time
    :return: Average QubErr
    :rtype: tuple of tuples of floats
    """
    # Get the states_data
    quberr_data = parse_table_data_from_sql(results_path, "EGP_QubErr", max_real_time=max_real_time)

    # Parse the qubit error data
    Z_err = []
    X_err = []
    Y_err = []
    Z_data_points = 0
    X_data_points = 0
    Y_data_points = 0
    for entry in quberr_data:
        data_point = EGPQubErrDataPoint(entry)
        z_err = data_point.z_err
        x_err = data_point.x_err
        y_err = data_point.y_err
        if z_err in [0, 1]:
            Z_err.append(z_err)
            Z_data_points += 1
        if x_err in [0, 1]:
            X_err.append(x_err)
            X_data_points += 1
        if y_err in [0, 1]:
            Y_err.append(y_err)
            Y_data_points += 1
    if Z_data_points > 0:
        avg_Z_err = sum(Z_err) / Z_data_points
    else:
        avg_Z_err = None
    if X_data_points > 0:
        avg_X_err = sum(X_err) / X_data_points
    else:
        avg_X_err = None
    if Y_data_points > 0:
        avg_Y_err = sum(Y_err) / Y_data_points
    else:
        avg_Y_err = None

    return (avg_Z_err, Z_data_points), (avg_X_err, X_data_points), (avg_Y_err, Y_data_points)


def calc_throughput(all_gens, window=1):
    """
    Computes the instantaneous throughput of entanglement generation as a function of time over the
    duration of the simulation
    :param all_gens: dict of key nodeID
                        value list of (nodeID, createTime, attempts, (createID, sourceID, otherID, mhpSeq))
        Contains the create time of the generations
    :param window: The window size in seconds
    :return: A tuple with list of times and throughputs that can be used for plotting
    """
    window_size = window * SECOND
    t_actions = []
    throughput = [(0, 0)]

    # TODO assuming gens are consistent between the nodes
    all_node_gens = list(all_gens.values())[0]
    for gen in all_node_gens:
        gen_time = gen[1]
        t_actions.append((gen_time, 1))
        t_actions.append((gen_time + window_size, -1))

    t_actions = sorted(t_actions, key=lambda t: t[0])

    for timestamp, action in t_actions:
        inst_throughput = throughput[-1][1] + action
        throughput.append((timestamp - 1, throughput[-1][1]))
        throughput.append((timestamp, inst_throughput))

    throughput = sorted(throughput, key=lambda p: p[0])
    s = [t[1] for t in throughput]
    t = [t[0] / SECOND for t in throughput]
    return t, s


def extract_successful_unsuccessful_creates(requests, gens):
    """
    Discovers which requests were successfully completed
    :param requests: dict of key (createID, sourceID, otherID)
                           value [sourceID, otherID, numPairs, createID, createTime,maxTime]
        These are successfully submitted request
    :param gens: dict of key nodeID
                    value dict of key (createID, sourceID, otherID)
                       value list of [nodeID, createID, sourceID, otherID, mhpSeq, createTime, attempts]
                 Generations corresponding to successfully submitted requests
    :return: list, list
        satisfied_creates - list of [sourceID, otherID, numPairs, createID, createTime, maxTime] corresponding to
                            requests that had all pairs completed
        unsatisfied_creates - list of [sourceID, otherID, numPairs, createID, createTime, maxTime] corresponding to
                            requests that were not satisfied
    """
    # TODO assuming gens are consistent between the nodes
    node_gens = list(gens.values())[0]

    satisfied_creates = []
    unsatisfied_creates = []
    for requestID, request in requests.items():
        genlist = node_gens[requestID]
        if len(genlist) == request[2]:
            satisfied_creates.append(request)
        else:
            unsatisfied_creates.append(request)

    return satisfied_creates, unsatisfied_creates


def get_request_latencies(satisfied_creates, gens):
    """
    Extracts the latencies of the satisfied requests
    :param satisfied_creates: list of [sourceID, otherID, numPairs, createID, createTime, maxTime]
        Corresponds to requests that had all pairs completed
    :param gens: dict of key (createID, sourceID, otherID)
                       value list of [nodeID, createID, sourceID, otherID, mhpSeq, createTime, attempts]
        Generations corresponding to successfully submitted requests
    :return: list of floats
        Latencies of satisfied requests
    """
    # TODO assuming gens are consistent between the nodes
    node_gens = list(gens.values())[0]

    request_times = []
    for request in satisfied_creates:
        requestID = (request[3], request[0], request[1])
        genlist = node_gens[requestID]
        request_time = genlist[-1][5] - request[5]
        request_times.append(request_time)

    return request_times


def get_gen_latencies(requests, gens):
    """
    Extracts the latencies of successful generations
    :param requests: dict of key (createID, sourceID, otherID)
                           value [sourceID, otherID, numPairs, createID, createTime,maxTime]
        These are successfully submitted request
    :param gens: dict of key (createID, sourceID, otherID)
                       value list of [nodeID, createID, sourceID, otherID, mhpSeq, createTime, attempts]
        Generations corresponding to successfully submitted requests
    :return: dict, list
        gen_starts - dict of key (createID, sourceID, otherID)
                             value float
            Specifying the start time of a particular generation
        gen_times - list of floats
            The amount of time per generation
    """
    # TODO assuming gens are consistent between the nodes
    node_gens = list(gens.values())[0]

    gen_starts = {}
    gen_times = []
    for requestID, requestInfo in requests.items():
        start_time = requestInfo[4]
        for gen in node_gens[requestID]:
            gen_time = (gen[5] - start_time)
            gen_starts[tuple(gen[1:5])] = start_time
            gen_times.append(gen_time)
            start_time = gen[5]

    return gen_starts, gen_times


def parse_raw_queue_data(raw_queue_data, max_real_time=None):
    """
    Computes average and max queue length and total time items spent in queue
    given data from the sqlite file
    :param raw_queue_data: list
        data extracted from the sqlite file
    :param max_real_time: float or None
        If specified, don't include any entries after max_real_time
    :return: tuple
        (queue_lens, times, max_queue_len, avg_queue_len, tot_time_in_queue)
        queue_lens: List of queue lengths at times in 'times
        times: Times where corresponding to the queue lengths data points
        max_queue_len: Max queue length
        avg_queue_len: Average queue length
        tot_time_in_queue: Total time items spent in queue.
    """
    tot_time_in_queue = 0
    queue_lens = [0]
    times = [0]
    for entry in raw_queue_data:
        data_point = EGPLocalQueueDataPoint(entry)
        time = data_point.timestamp
        change = data_point.change
        time_diff = time - times[-1]
        tot_time_in_queue += time_diff * queue_lens[-1]
        queue_lens.append(queue_lens[-1] + change)
        times.append(time)
    if max_real_time is None:
        tot_time_diff = times[-1] - times[1]
    else:
        if queue_lens[-1] > 0:  # Non-empty queue by max_real_time
            tot_time_diff = max_real_time - times[1]

            # Since there are still items in the queue by max_real_time
            # we should add this to the tot_time_in_queue
            tot_time_in_queue += (max_real_time - times[-1]) * queue_lens[-1]

            # Add also last point to queue lens
            queue_lens.append(queue_lens[-1])
            times.append(max_real_time)
        else:  # Empty queue by max_real_time, stop when last item popped
            tot_time_diff = times[-1] - times[1]
    if min(queue_lens) < 0:
        raise RuntimeError("Something went wrong, negative queue length")
    if tot_time_diff == 0:
        return queue_lens, times, max(queue_lens), float('inf'), tot_time_in_queue
    else:
        return queue_lens, times, max(queue_lens), tot_time_in_queue / tot_time_diff, tot_time_in_queue


def plot_single_queue_data(queue_lens, times, color=None, label=None, clear_figure=True):
    """
    Plots the queue length over time from data extracted using 'parse_raw_queue_data'
    :param queue_lens: list of int
    :param times: list of float
    :param path_to_folder: str
        Path to the results folder, used to save the fig
    :param no_plot: bool
        Whether to show the plot or not
    :param save_figs: bool
        Whether to save the plot or not
    :param clear_figure: bool
        Whether to clear the plot before plotting
    :return: None
    """
    if clear_figure:
        plt.clf()
    x_points = []
    y_points = []
    for j in range(len(queue_lens)):
        # Make points to make straight lines when queue len is not changing
        x_points.append(times[j])
        y_points.append(queue_lens[j])

        if (j + 1) < len(times):
            x_points.append(times[j + 1])
            y_points.append(queue_lens[j])

    if color:
        if label:
            plt.plot(x_points, y_points, color=color, label=label)
        else:
            plt.plot(x_points, y_points, color=color)
    else:
        if label:
            plt.plot(x_points, y_points, label=label)
        else:
            plt.plot(x_points, y_points)


def plot_queue_data(queue_lens, times, results_path, no_plot=False, save_figs=False, analysis_folder=None,
                    clear_figure=True):
    colors = ['red', 'green']
    labels = ['Node A', 'Node B']
    if clear_figure:
        plt.clf()
    for i in range(len(queue_lens)):
        qls = queue_lens[i]
        ts = times[i]
        plot_single_queue_data(qls, ts, color=colors[i], label=labels[i], clear_figure=False)
    plt.ylabel("Queue lengths")
    plt.xlabel("Real time (s)")
    plt.legend(loc='upper right')
    if save_figs:
        save_plot("queue_lens.pdf", results_path, analysis_folder=analysis_folder)
    if not no_plot:
        plt.show()


def plot_gen_attempts(gen_attempts, results_path, no_plot=False, save_figs=False, analysis_folder=None, plot_dist=False,
                      clear_figure=True):
    """
    Plots a histogram and a distribution of the number of attempts for generations in the simulation
    :param gen_attempts: dict of key nodeID
                         value dict of key (createID, sourceID, otherID, mhpSeq)
                                  value int
            Containing the number of attempts made for the generation for each node
    :param path_to_folder: str
        Path to the results folder, used to save the fig
    :param no_plot: bool
        Whether to show the plot or not
    :param save_figs: bool
        Whether to save the plot or not
    :param plot_dist: bool
        Whether to plot the distribution along the histogram
    :param clear_figure: bool
        Whether to clear the plot before plotting
    """
    if no_plot and (not save_figs):
        return

    if clear_figure:
        plt.clf()
    # TODO assuming node attempts are equal for the two nodes
    node_gen_attempts = list(gen_attempts.values())[0]

    plt.hist(node_gen_attempts.values(), 50)

    plt.xlabel('Attempts')
    plt.ylabel('Generation Count')
    plt.title('Attempt Histogram')
    plt.grid(True)
    if save_figs:
        save_plot("attempt_histogram.pdf", results_path, analysis_folder=analysis_folder)
    if not no_plot:
        plt.show()

    if plot_dist:
        if clear_figure:
            plt.clf()
        t = node_gen_attempts.values()
        s = [0] * len(t)
        plt.plot(t, s, '.')

        plt.xlabel('time (s)')
        plt.title('Generation Latency Distribution')
        plt.grid(True)
        if save_figs:
            save_plot("gen_latency_dist.pdf", results_path, analysis_folder=analysis_folder)
        if not no_plot:
            plt.show()


def plot_gen_times(gen_times, results_path, no_plot=False, save_figs=False, analysis_folder=None, plot_dist=False,
                   clear_figure=True):
    """
    Plots a histogram and a distribution of the amount of time for generations in the simulation
    :param gen_times: list of floats
        The amount of time per generation
    :param path_to_folder: str
        Path to the results folder, used to save the fig
    :param no_plot: bool
        Whether to show the plot or not
    :param save_figs: bool
        Whether to save the plot or not
    :param plot_dist: bool
        Whether to plot the distribution along the histogram
    :param clear_figure: bool
        Whether to clear the plot before plotting
    """
    if no_plot and (not save_figs):
        return

    if clear_figure:
        plt.clf()
    gen_times = [t / SECOND for t in gen_times]
    plt.hist(gen_times, 50)

    plt.xlabel('Generation Latency')
    plt.ylabel('Generation Count')
    plt.title('Generation Latency Histogram')
    plt.grid(True)
    if save_figs:
        save_plot("gen_latency_histogram.pdf", results_path, analysis_folder=analysis_folder)
    if not no_plot:
        plt.show()

    if plot_dist:
        if clear_figure:
            plt.clf()
        t = gen_times
        s = [0] * len(t)
        plt.plot(t, s, '.')

        plt.xlabel('attempts')
        plt.title('Generation Attempt Count Distribution')
        plt.grid(True)
        if save_figs:
            save_plot("gen_attempt_count_dist.pdf", results_path, analysis_folder=analysis_folder)
        if not no_plot:
            plt.show()


def plot_throughput(all_gens, results_path, no_plot=False, save_figs=False, analysis_folder=None, clear_figure=True):
    """
    Plots the instantaneous throughput of entanglement generation as a function of time over the
    duration of the simulation
    :param all_gens: list of (nodeID, createTime, attempts, (createID, sourceID, otherID, mhpSeq))
        Contains the create time of the generations
    :param path_to_folder: str
        Path to the results folder, used to save the fig
    :param no_plot: bool
        Whether to show the plot or not
    :param save_figs: bool
        Whether to save the plot or not
    :param clear_figure: bool
        Whether to clear the plot before plotting
    """
    # TODO assuming gens are consistent between the nodes
    node_all_gens = list(all_gens.values())[0]

    if no_plot and (not save_figs):
        return

    if clear_figure:
        plt.clf()
    window_size = SECOND
    t_actions = []
    throughput = [(0, 0)]

    for gen in node_all_gens:
        gen_time = gen[1]
        t_actions.append((gen_time, 1))
        t_actions.append((gen_time + window_size, -1))

    t_actions = sorted(t_actions, key=lambda t: t[0])

    for timestamp, action in t_actions:
        inst_throughput = throughput[-1][1] + action
        throughput.append((timestamp - 1, throughput[-1][1]))
        throughput.append((timestamp, inst_throughput))

    throughput = sorted(throughput, key=lambda p: p[0])
    s = [t[1] for t in throughput]
    t = [t[0] / SECOND for t in throughput]
    plt.plot(t, s, '.-')

    plt.xlabel('time (s)')
    plt.ylabel('Throughput (gen/s)')
    plt.title('Instantaneous throughput of generation')
    plt.grid(True)

    if save_figs:
        save_plot("throughput.pdf", results_path, analysis_folder=analysis_folder)
    if not no_plot:
        plt.show()


def get_key_and_run_from_path(results_path):
    """
    results_path is assumed to be of the form "path/timestamp_key_i_run_j.db".
    This function returns i and j as s tuple (i,j)
    :param results_path:
    :return: (str, str)
    """
    start_of_key = results_path.find("key")
    start_of_run = results_path.find("run")

    key_str = results_path[(start_of_key + 4):(start_of_run - 1)]
    run_str = results_path[(start_of_run + 4):]

    # strip of possible filetype from run
    run_str = run_str.split('.')[0]

    return key_str, run_str


def save_plot(fig_name, results_path, analysis_folder=None):
    if analysis_folder is not None:
        if not os.path.exists(analysis_folder):
            os.makedirs(analysis_folder)
        plt.savefig(analysis_folder + "/" + fig_name)
    else:
        plt.savefig(results_path[-3] + "_" + fig_name)


def get_data_from_single_file(path_to_file, max_real_time=None):
    """
    Returns a dictionary of all the data from the simulation(s)
    :param results_path: str
        Path to a sqlite-file
    :param max_real_time:
    :return: dct
    """
    data_dct = {}
    # Check if there is an additional data file
    try:
        with open(path_to_file[:-3] + "_additional_data.json", 'r') as json_file:
            additional_data = json.load(json_file)
            data_dct["additional_data"] = additional_data
    except FileNotFoundError:
        additional_data = {}

    # If max_real_time is not set, check if there is a total_real_time in the data
    if max_real_time is None:
        try:
            max_real_time = additional_data["total_real_time"]
        except KeyError:
            pass

    # Get create and ok data from sql file to compute latencies and throughput
    (requests, rejected_requests), (gens, all_gens), total_requested_pairs = \
        parse_request_data_from_sql(path_to_file, max_real_time=max_real_time)
    # Compute latencies for generating a single pair
    gen_starts, gen_times = get_gen_latencies(requests, gens)

    # Get attempts data to compute number of attempts per generation and generation probability
    gen_attempts = get_attempt_data(all_gens)

    # Get fidelities
    fidelities = parse_fidelities_from_sql(path_to_file, max_real_time=max_real_time)

    # Get QubErr
    Z_data, X_data, Y_data = parse_quberr_from_sql(path_to_file, max_real_time=max_real_time)

    # Get queue data
    raw_queue_dataA = parse_table_data_from_sql(path_to_file, "EGP_Local_Queue_A", max_real_time=max_real_time)
    raw_queue_dataB = parse_table_data_from_sql(path_to_file, "EGP_Local_Queue_B", max_real_time=max_real_time)

    if all_gens:
        data_dct["all_gens"] = all_gens
    if gen_times:
        data_dct["gen_times"] = gen_times
    if gen_attempts:
        data_dct["gen_attempts"] = gen_attempts
    if fidelities:
        data_dct["fidelities"] = fidelities
    if Z_data:
        data_dct["Z_data"] = Z_data
    if X_data:
        data_dct["X_data"] = X_data
    if Y_data:
        data_dct["Y_data"] = Y_data
    if raw_queue_dataA:
        data_dct["raw_queue_dataA"] = raw_queue_dataA
    if raw_queue_dataB:
        data_dct["raw_queue_dataB"] = raw_queue_dataB

    return data_dct


def get_data(results_path, max_real_time=None):
    """
    Returns a dictionary of all the data from the simulation(s)
    :param results_path: str or list of str
        Path to a sqlite-file, folder containing sqlite-files or a list of paths to sqlite-files.
    :param max_real_time:
    :return: dct
    """
    if isinstance(results_path, list):  # list of paths
        data_dct = {}
        for path in results_path:
            data_dct[path] = get_data(path, max_real_time)
        return data_dct
    elif results_path.endswith(".db"):  # single data file
        return get_data_from_single_file(results_path, max_real_time)
    else:  # path to folder
        if results_path.endswith('/'):
            results_path = results_path[:-1]
        if not os.path.isdir(results_path):
            raise ValueError("'results_path={} is not in correct format".format(results_path))
        data_dct = {}
        for entry in os.listdir(results_path):
            if entry.endswith(".db"):
                data_dct[entry] = get_data(os.path.join(results_path, entry), max_real_time)
        return data_dct


def analyse_single_file(results_path, no_plot=False, max_real_time=None, save_figs=False, save_output=False,
                        analysis_folder=None):
    # Initialize the printer
    prnt = printer(results_path=results_path, save_output=save_output, analysis_folder=analysis_folder)
    prnt.print("results_path: {}".format(results_path))

    # Check if there is an additional data file
    try:
        with open(results_path[:-3] + "_additional_data.json", 'r') as json_file:
            additional_data = json.load(json_file)
    except FileNotFoundError:
        additional_data = {}

    # If max_real_time is not set, check if there is a total_real_time in the data
    if max_real_time is None:
        try:
            max_real_time = additional_data["total_real_time"]
        except KeyError:
            pass

    # Get create and ok data from sql file to compute latencies and throughput
    (requests, rejected_requests), (gens, all_gens), total_requested_pairs = \
        parse_request_data_from_sql(results_path, max_real_time=max_real_time)
    # Check (u)successful creates
    satisfied_creates, unsatisfied_creates = extract_successful_unsuccessful_creates(requests, gens)
    # Compute latencies for requests, i.e. possibly more than one pair/request
    request_times = get_request_latencies(satisfied_creates, gens)
    # Compute latencies for generating a single pair
    gen_starts, gen_times = get_gen_latencies(requests, gens)

    # Get attempts data to compute number of attempts per generation and generation probability
    gen_attempts = get_attempt_data(all_gens)
    gen_attemptsA, gen_attemptsB = gen_attempts.values()

    # Get fidelities
    fidelities = parse_fidelities_from_sql(results_path, max_real_time=max_real_time)

    # Get QubErr
    Z_data, X_data, Y_data = parse_quberr_from_sql(results_path, max_real_time=max_real_time)
    avg_Z_err, Z_data_points = Z_data
    avg_X_err, X_data_points = X_data
    avg_Y_err, Y_data_points = Y_data

    # Get queue data
    # TODO Currently only local queue with id 0
    raw_queue_dataA = parse_table_data_from_sql(results_path, "EGP_Local_Queue_A_0", max_real_time=max_real_time)
    raw_queue_dataB = parse_table_data_from_sql(results_path, "EGP_Local_Queue_B_0", max_real_time=max_real_time)

    prnt.print("-------------------")
    prnt.print("|Simulation data: |")
    prnt.print("-------------------")
    prnt.print("Analysing data in file {}".format(results_path))
    key_str, run_str = get_key_and_run_from_path(results_path)
    path_to_folder = "/".join(results_path.split('/')[:-1])
    prnt.print("path_to_folder: {}".format(path_to_folder))
    prnt.print("key_str: {}".format(key_str))
    prnt.print("run_str: {}".format(run_str))
    with open(path_to_folder + "/paramcombinations.json") as json_file:
        arguments = json.load(json_file)[key_str]
    prnt.print("Arguments in paramcombinations.py for this simulation was:")
    for arg_name, arg in arguments.items():
        prnt.print("    {}={}".format(arg_name, arg))
    prnt.print("")
    try:
        prnt.print("Total 'real time' was {} ns".format(additional_data["total_real_time"]))
    except KeyError:
        pass
    try:
        prnt.print("Total 'wall time' was {} s".format(additional_data["total_wall_time"]))
    except KeyError:
        pass
    try:
        prnt.print("Bright state population used was: alphaA={}, alphaB={}".format(additional_data["alphaA"],
                                                                                   additional_data["alphaB"]))
    except KeyError:
        pass
    prnt.print("")

    prnt.print("-----------------------")
    prnt.print("|Performance metrics: |")
    prnt.print("-----------------------")
    prnt.print("Number of satisfied CREATE requests: {} out of total {}".format(len(satisfied_creates), len(requests)))
    prnt.print("")

    if request_times:
        prnt.print("Average request latency: {} s".format(sum(request_times) / len(request_times) / SECOND))
        prnt.print("Minimum request latency: {} s".format(min(request_times) / SECOND))
        prnt.print("Maximum request latency: {} s".format(max(request_times) / SECOND))
        prnt.print("")

    if gen_times:
        prnt.print("Average generation time: {} s".format(sum(gen_times) / len(all_gens) / SECOND))
        prnt.print("Minimum generation time: {} s".format(min(gen_times) / SECOND))
        prnt.print("Maximum generation time: {} s".format(max(gen_times) / SECOND))
        prnt.print("")

    if fidelities:
        print("fidelities: {}".format(fidelities))
        prnt.print("Average fidelity: {} s".format(sum(fidelities) / len(fidelities)))
        prnt.print("Minimum fidelity: {} s".format(min(fidelities)))
        prnt.print("Maximum fidelity: {} s".format(max(fidelities)))
        prnt.print("")

    if avg_Z_err:
        prnt.print("Average QubErr in Z-basis {} (from {} data points)".format(avg_Z_err, Z_data_points))
    else:
        prnt.print("Average QubErr in Z-basis NO_DATA")
    if avg_X_err:
        prnt.print("Average QubErr in X-basis {} (from {} data points)".format(avg_X_err, X_data_points))
    else:
        prnt.print("Average QubErr in X-basis NO_DATA")
    if avg_Y_err:
        prnt.print("Average QubErr in Y-basis {} (from {} data points)".format(avg_Y_err, Y_data_points))
        prnt.print("")
    else:
        prnt.print("Average QubErr in Y-basis NO_DATA")
        prnt.print("")

    if gen_attempts:
        are_equal = True
        if not len(gen_attemptsA) == len(gen_attemptsB):
            prnt.print("Number of started generations are different between the nodes, A has {} and B has {}".format(
                len(gen_attemptsA), len(gen_attemptsB)))
        else:
            for key in gen_attemptsA:
                if not gen_attemptsA[key] == gen_attemptsB[key]:
                    are_equal = False
                    break
            prnt.print("Number of attempts equal for the two nodes, for each generation: {}".format(are_equal))
        # TODO assuming that node attempts are equal for the two nodes
        nr_of_gens = len(list(all_gens.values())[0])
        if nr_of_gens > 0:
            avg_attempt_per_gen = sum(gen_attemptsA.values()) / nr_of_gens
            prnt.print("Minimum number of attempts for a generation: {}".format(min(gen_attemptsA.values())))
            prnt.print("Maximum number of attempts for a generation: {}".format(max(gen_attemptsA.values())))
        else:
            avg_attempt_per_gen = None
        prnt.print("Average number of attempts per successful generation: {}".format(avg_attempt_per_gen))
        prnt.print("")

        prnt.print(
            "Total number of generated pairs: {} of total requested {}".format(nr_of_gens, total_requested_pairs))
    prnt.print(
        "Total number of entanglement attempts for successful generations: {}".format(sum(gen_attemptsA.values())))

    prnt.print("")
    prnt.print("----------------------------------")
    prnt.print("|Data useful for queuing theory: |")
    prnt.print("----------------------------------")

    # Check mhp_t_cycle and request_cycle
    try:
        mhp_t_cycle = additional_data["mhp_t_cycle"]
        request_t_cycle = additional_data["request_t_cycle"]
        prnt.print("The time cycle for MHP was {} ns and for the scheduled requests {} ns".format(mhp_t_cycle,
                                                                                                  request_t_cycle))

        # Compute the total number of MHP cycles
        total_real_time = additional_data["total_real_time"]
        number_mhp_cycles = math.floor(total_real_time / mhp_t_cycle)
        prnt.print("Total number of complete MHP cycles was {}".format(number_mhp_cycles))
        if number_mhp_cycles > 0:
            fractionA = sum(gen_attemptsA.values()) / number_mhp_cycles
        else:
            fractionA = float('inf')
        # fractionB = node_attempts[1]/ number_mhp_cycles
        # TODO ASSUMING THAT NUMBER OF ATTEMPTS ARE EQUAL FOR A AND B
        prnt.print("Number of attempted entanglement generations / Number of MHP cycles = {}".format(fractionA))
        prnt.print("")
        if gen_attempts:
            if avg_attempt_per_gen is None:
                avg_prob_per_attempt = None
                avg_prob_per_MHP_cycle = None
            else:
                avg_prob_per_attempt = 1 / avg_attempt_per_gen
                avg_prob_per_MHP_cycle = avg_prob_per_attempt * fractionA

            prnt.print(
                "Average probability of generating entanglement per attempt: {}".format(avg_prob_per_attempt))
            prnt.print("Average probability of generating entanglement per MHP cycle: {}".format(
                avg_prob_per_MHP_cycle))
        try:
            prnt.print("Probability of midpoint declaring success: {}".format(additional_data["p_succ"]))
        except KeyError:
            pass

    except KeyError:
        pass

    try:
        prnt.print("")
        prnt.print("Request params for node A was {}".format(additional_data["request_paramsA"]))
        prnt.print("Request params for node B was {}".format(additional_data["request_paramsB"]))
        prnt.print(
            "Probability that a scheduled request was on A {}".format(additional_data["create_request_origin_bias"]))
    except KeyError:
        pass

    # Extract data from raw queue data
    if raw_queue_dataA:
        queue_lensA, qtimesA, max_queue_lenA, avg_queue_lenA, tot_time_in_queueA = \
            parse_raw_queue_data(raw_queue_dataA, max_real_time=max_real_time)
        prnt.print("")
        prnt.print("Max queue length at A: {}".format(max_queue_lenA))
        prnt.print("Average queue length at A: {}".format(avg_queue_lenA))
        prnt.print("Total time items spent in queue at A: {} ns".format(tot_time_in_queueA))
    if raw_queue_dataB:
        queue_lensB, qtimesB, max_queue_lenB, avg_queue_lenB, tot_time_in_queueB = \
            parse_raw_queue_data(raw_queue_dataA, max_real_time=max_real_time)
        prnt.print("")
        prnt.print("Max queue length at B: {}".format(max_queue_lenB))
        prnt.print("Average queue length at B: {}".format(avg_queue_lenB))
        prnt.print("Total time items spent in queue at B: {} ns".format(tot_time_in_queueB))

    if raw_queue_dataA and raw_queue_dataB:
        plot_queue_data([queue_lensA, queue_lensB], [qtimesA, qtimesB], results_path, no_plot=no_plot,
                        save_figs=save_figs, analysis_folder=analysis_folder)

    if gen_attempts:
        plot_gen_attempts(gen_attempts, results_path, no_plot=no_plot, save_figs=save_figs,
                          analysis_folder=analysis_folder)

    if gen_times:
        plot_gen_times(gen_times, results_path, no_plot=no_plot, save_figs=save_figs, analysis_folder=analysis_folder)

    if all_gens:
        plot_throughput(all_gens, results_path, no_plot=no_plot, save_figs=save_figs, analysis_folder=analysis_folder)


def main(results_path, no_plot, max_real_time=None, save_figs=False, save_output=False, analysis_folder=None):
    if results_path is None:
        # Find the latest folder containing simulation data
        sim_dir_env = "SIMULATION_DIR"

        # Check that the simulation path is set
        if sim_dir_env not in os.environ:
            print("The environment variable {} must be set to the path to the simulation folder"
                  "before running this script, if the argument --results-path is not used!".format(sim_dir_env))
            sys.exit()
        else:
            sim_dir = os.getenv(sim_dir_env)
            if not os.path.isdir(sim_dir):
                print("The environment variable {} is not a path to a folder. This must"
                      "be the case if the argument --results-path is not used!".format(sim_dir_env))
                sys.exit()

        # Check that sim_dir ends with '/'
        if not sim_dir[-1] == '/':
            sim_dir += "/"

        def is_data_folder(folder):
            return os.path.isdir(folder) and (folder[:4] in ["2018", "2019", "2020"])

        data_folders = [entry for entry in os.listdir(sim_dir) if is_data_folder(entry)]
        results_path = sorted(data_folders, reverse=True)[0]

    # Check if results_path is a single .db file or a folder containing such
    if results_path.endswith('.db'):
        analyse_single_file(results_path, no_plot, max_real_time=max_real_time, save_figs=save_figs,
                            save_output=save_output, analysis_folder=analysis_folder)
    else:
        if results_path.endswith('/'):
            results_path = results_path[:-1]
        for entry in os.listdir(results_path):
            if entry.endswith('.db'):
                # Initialize the printer
                entry_results_path = results_path + "/" + entry
                prnt = printer(results_path=entry_results_path, save_output=save_output,
                               analysis_folder=analysis_folder)

                prnt.print("")
                prnt.print("====================================")
                analyse_single_file(entry_results_path, no_plot, max_real_time=max_real_time,
                                    save_figs=save_figs, save_output=save_output, analysis_folder=analysis_folder)
                prnt.print("====================================")
                prnt.print("")

    # Save the data to json file
    data_dct = get_data(results_path=results_path)
    if os.path.isfile(results_path):
        data_filename = results_path[:results_path.rfind(".")] + "_all_data.json"
    elif os.path.isdir(results_path):
        data_filename = os.path.join(results_path, "all_data.json")
    else:
        raise ValueError("results_path {} is not a path to a folder or file")
    with open(data_filename, 'w') as f:
        json.dump(data_dct, f, indent=4)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--results-path', required=False, type=str, default=None,
                        help="Path to the directory containing the simulation results")
    parser.add_argument('--max_real_time', required=False, type=float,
                        help="If specified, don't include data after max_real_time (ns)")
    parser.add_argument('--no-plot', default=False, action='store_true',
                        help="Whether to produce plots or not")
    parser.add_argument('--save-figs', default=False, action='store_true',
                        help="Whether to save figs (independent from --no-plot")
    parser.add_argument('--save-output', default=False, action='store_true',
                        help="Whether to save figs (independent from --no-plot")
    parser.add_argument('--analysis-folder', required=False, type=str,
                        help="Path to the directory which should hold the analysis data")

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    main(results_path=args.results_path, no_plot=args.no_plot, max_real_time=args.max_real_time,
         save_figs=args.save_figs, save_output=args.save_output, analysis_folder=args.analysis_folder)
