import sqlite3
from argparse import ArgumentParser
from collections import defaultdict
import numpy as np
from easysquid.toolbox import logger
from netsquid.simutil import SECOND
import json
import math
import matplotlib.pyplot as plt
import os


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
        except sqlite3.OperationalError as err:
            logger.error("sqlite3 could not open the file {}, check that the path is correct".format(results_path))
            raise err
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
        gens - dict of key (createID, sourceID, otherID)
                       value list of [createID, sourceID, otherID, mhpSeq, createTime]
                 Generations corresponding to successfully submitted requests
        all_gens - list of (createTime, (createID, sourceID, otherID, mhpSeq))
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
        (timestamp, nodeID, create_id, create_time, max_time, measure_directly, min_fidelity, num_pairs, otherID,
         priority, purpose_id, store, succ) = entry

        total_requested_pairs += num_pairs

        if create_id is not None and create_time is not None:
            requests[(create_id, nodeID, otherID)] = [nodeID, otherID, num_pairs, create_id, create_time, max_time]
        else:
            rejected_requests.append([nodeID, otherID, num_pairs, timestamp])

    # Parse the ok data
    gens = defaultdict(list)
    all_gens = []
    recorded_mhp_seqs = []
    for entry in oks_data:
        timestamp, createID, originID, otherID, MHPSeq, logical_id, goodness, t_goodness, t_create, succ = entry

        if MHPSeq in recorded_mhp_seqs:
            continue
        else:
            recorded_mhp_seqs.append(MHPSeq)

        gens[(createID, originID, otherID)].append([createID, originID, otherID, MHPSeq, t_create])
        all_gens.append((t_create, (createID, originID, otherID, MHPSeq)))

    return (requests, rejected_requests), (gens, all_gens), total_requested_pairs


def parse_attempt_data_from_sql(results_path, all_gens, gen_starts, max_real_time=None):
    """
    Parses collected attempt data points from the sql database
    :param results_path: str
        Path to directory containing node_attempt.log containing attempts from both nodes
    :param all_gens: list of (createTime, (createID, sourceID, otherID, mhpSeq))
        All generations that occurred during the simulation
    :param gen_starts: dict of key (createID, sourceID, otherID, mhpSeq)
                               value createTime
        Contains the completion time of each generation
    :param max_real_time: float or None
        If specified, don't include any entries after max_real_time
    :return: node_attempts, gen_attempts
        node_attempts - dict of key nodeID
                                value num_attempts
            Containing the number of attempts made by each node
        gen_attempts - dict of key (createID, sourceID, otherID, mhpSeq)
                               value int
            Containing the number of attempts made for the generation
    """
    # Get the attempt data
    attempt_data = parse_table_data_from_sql(results_path, "Node_EGP_Attempts", max_real_time=max_real_time)

    # Parse the attempt data
    gen_attempts = defaultdict(int)
    node_attempts = defaultdict(int)
    for entry in attempt_data:
        timestamp, nodeID, succ = entry

        node_attempts[nodeID] += 1

        for gen in all_gens:
            genID = gen[1]
            if gen_starts[genID] < timestamp < gen[0]:
                gen_attempts[genID] += 1
                break

    return node_attempts, gen_attempts


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
        timestamp = entry[0]
        if not (len(entry) == 35):
            raise ValueError("Unknown quantum states format in data file")
        ts.append(timestamp)
        m_data = entry[2:34]
        d_matrix = np.matrix(
            [[m_data[i] + 1j * m_data[i + 1] for i in range(k, k + 8, 2)] for k in range(0, len(m_data), 8)])
        fidelities.append(calc_fidelity(d_matrix))

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

    # Parse the states data to compute fideli    print("QubErrs:")
    Z_err = []
    X_err = []
    Z_data_points = 0
    X_data_points = 0
    for entry in quberr_data:
        if entry[1] in [0, 1]:
            Z_err.append(entry[1])
            Z_data_points += 1
        if entry[2] in [0, 1]:
            X_err.append(entry[2])
            X_data_points += 1
    if Z_data_points > 0:
        avg_Z_err = sum(Z_err) / Z_data_points
    else:
        avg_Z_err = None
    if X_data_points > 0:
        avg_X_err = sum(X_err) / X_data_points
    else:
        avg_X_err = None

    return (avg_Z_err, Z_data_points), (avg_X_err, X_data_points)


def calc_throughput(all_gens, window=1):
    """
    Computes the instantaneous throughput of entanglement generation as a function of time over the
    duration of the simulation
    :param all_gens: list of (createTime, (createID, sourceID, otherID, mhpSeq))
        Contains the create time of the generations
    :param window: The window size in seconds
    :return: A tuple with list of times and throughputs that can be used for plotting
    """
    window_size = window * SECOND
    t_actions = []
    throughput = [(0, 0)]

    for gen in all_gens:
        gen_time = gen[0]
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
    :param gens: dict of key (createID, sourceID, otherID)
                       value list of [createID, sourceID, otherID, mhpSeq, createTime]
        Generations corresponding to successfully submitted requests
    :return: list, list
        satisfied_creates - list of [sourceID, otherID, numPairs, createID, createTime, maxTime] corresponding to
                            requests that had all pairs completed
        unsatisfied_creates - list of [sourceID, otherID, numPairs, createID, createTime, maxTime] corresponding to
                            requests that were not satisfied
    """
    satisfied_creates = []
    unsatisfied_creates = []
    for requestID, request in requests.items():
        genlist = gens[requestID]
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
                       value list of [createID, sourceID, otherID, mhpSeq, createTime]
        Generations corresponding to successfully submitted requests
    :return: list of floats
        Latencies of satisfied requests
    """
    request_times = []
    for request in satisfied_creates:
        requestID = (request[3], request[0], request[1])
        genlist = gens[requestID]
        request_time = genlist[-1][4] - request[4]
        request_times.append(request_time)

    return request_times


def get_gen_latencies(requests, gens):
    """
    Extracts the latencies of successful generations
    :param requests: dict of key (createID, sourceID, otherID)
                           value [sourceID, otherID, numPairs, createID, createTime,maxTime]
        These are successfully submitted request
    :param gens: dict of key (createID, sourceID, otherID)
                       value list of [createID, sourceID, otherID, mhpSeq, createTime]
        Generations corresponding to successfully submitted requests
    :return: dict, list
        gen_starts - dict of key (createID, sourceID, otherID)
                             value float
            Specifying the start time of a particular generation
        gen_times - list of floats
            The amount of time per generation
    """
    gen_starts = {}
    gen_times = []
    for requestID, requestInfo in requests.items():
        start_time = requestInfo[4]
        for gen in gens[requestID]:
            gen_time = (gen[4] - start_time)
            gen_starts[tuple(gen[:4])] = start_time
            gen_times.append(gen_time)
            start_time = gen[4]

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
        time, change, _, _, _ = entry
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
        else:  # Empty queue by max_real_time, stop when last item popped
            tot_time_diff = times[-1] - times[1]
    if min(queue_lens) < 0:
        raise RuntimeError("Something went wrong, negative queue length")
    if tot_time_diff == 0:
        return queue_lens, times, max(queue_lens), float('inf'), tot_time_in_queue
    else:
        return queue_lens, times, max(queue_lens), tot_time_in_queue / tot_time_diff, tot_time_in_queue


def plot_queue_data(queue_lens, times, results_path, no_plot=False, save_figs=False, analysis_folder=None):
    """
    Plots the queue length over time from data extracted using 'parse_raw_queue_data'
    :param queue_lens: [queue_lensA, queue_lensB]
    :param times: [qtimesA, qtimesB]
    :param path_to_folder: str
        Path to the results folder, used to save the fig
    :param no_plot: bool
        Whether to show the plot or not
    :param save_figs: bool
        Whether to save the plot or not
    :return: None
    """
    if no_plot and (not save_figs):
        return

    colors = ['red', 'green']
    labels = ['Node A', 'Node B']
    for i in range(2):
        x_points = []
        y_points = []
        for j in range(len(queue_lens[i])):
            # Make points to make straight lines when queue len is not changing
            x_points.append(times[i][j])
            y_points.append(queue_lens[i][j])

            if (j + 1) < len(times[i]):
                x_points.append(times[i][j + 1])
                y_points.append(queue_lens[i][j])

        plt.plot(x_points, y_points, color=colors[i], label=labels[i])

    plt.ylabel("Queue lengths")
    plt.xlabel("Real time (s)")
    plt.legend(loc='upper right')
    if save_figs:
        save_plot("queue_lens.pdf", results_path, analysis_folder=analysis_folder)
    if not no_plot:
        plt.show()


def plot_gen_attempts(gen_attempts, results_path, no_plot=False, save_figs=False, analysis_folder=None):
    """
    Plots a histogram and a distribution of the number of attempts for generations in the simulation
    :param gen_attempts: dict of key (createID, sourceID, otherID, mhpSeq)
                                 value int
        Containing the number of attempts made for the generation
    :param path_to_folder: str
        Path to the results folder, used to save the fig
    :param no_plot: bool
        Whether to show the plot or not
    :param save_figs: bool
        Whether to save the plot or not
    """
    if no_plot and (not save_figs):
        return

    plt.hist(gen_attempts.values(), 50)

    plt.xlabel('Attempts')
    plt.ylabel('Generation Count')
    plt.title('Attempt Histogram')
    plt.grid(True)
    if save_figs:
        save_plot("attempt_histogram.pdf", results_path, analysis_folder=analysis_folder)
    if not no_plot:
        plt.show()

    t = gen_attempts.values()
    s = [0] * len(t)
    fig, ax = plt.subplots()
    ax.plot(t, s, '.')

    ax.set(xlabel='time (s)', title='Generation Latency Distribution')
    ax.grid()
    if save_figs:
        save_plot("gen_latency_dist.pdf", results_path, analysis_folder=analysis_folder)
    if not no_plot:
        plt.show()


def plot_gen_times(gen_times, results_path, no_plot=False, save_figs=False, analysis_folder=None):
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
    """
    if no_plot and (not save_figs):
        return

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

    t = gen_times
    s = [0] * len(t)
    fig, ax = plt.subplots()
    ax.plot(t, s, '.')

    ax.set(xlabel='attempts', title='Generation Attempt Count Distribution')
    ax.grid()
    if save_figs:
        save_plot("gen_attempt_count_dist.pdf", results_path, analysis_folder=analysis_folder)
    if not no_plot:
        plt.show()


def plot_throughput(all_gens, results_path, no_plot=False, save_figs=False, analysis_folder=None):
    """
    Plots the instantaneous throughput of entanglement generation as a function of time over the
    duration of the simulation
    :param all_gens: list of (createTime, (createID, sourceID, otherID, mhpSeq))
        Contains the create time of the generations
    :param path_to_folder: str
        Path to the results folder, used to save the fig
    :param no_plot: bool
        Whether to show the plot or not
    :param save_figs: bool
        Whether to save the plot or not
    """
    if no_plot and (not save_figs):
        return

    window_size = SECOND
    t_actions = []
    throughput = [(0, 0)]

    for gen in all_gens:
        gen_time = gen[0]
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
    fig, ax = plt.subplots()
    ax.plot(t, s, '.-')

    ax.set(xlabel='time (s)', ylabel='Throughput (gen/s)',
           title='Instantaneous throughput of generation')
    ax.grid()

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
    substrings = results_path.split('_')
    for i in range(len(substrings)):
        if substrings[i] == "key":
            break
    key_str = substrings[i + 1]
    run_str = substrings[i + 3]

    # strip of possible filetype from run
    run_str = run_str.split('.')[0]

    return key_str, run_str


def output_data(data, results_path, save_output=False, analysis_folder=None):
    if not save_output:
        print(data)
    else:
        if analysis_folder is not None:
            if not os.path.exists(analysis_folder):
                os.makedirs(analysis_folder)
            with open(analysis_folder + "/analysis_output.txt", 'a') as out_file:
                out_file.write(data + "\n")
        else:
            with open(results_path[-3] + "_analysis_output.txt", 'a') as out_file:
                out_file.write(data + "\n")


def save_plot(fig_name, results_path, analysis_folder=None):
    if analysis_folder is not None:
        if not os.path.exists(analysis_folder):
            os.makedirs(analysis_folder)
        plt.savefig(analysis_folder + "/" + fig_name)
    else:
        plt.savefig(results_path[-3] + "_" + fig_name)


def analyse_single_file(results_path, no_plot=False, max_real_time=None, save_figs=False, save_output=False,
                        analysis_folder=None):
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
    (requests, rejected_requests), (gens, all_gens), total_requested_pairs =\
        parse_request_data_from_sql(results_path, max_real_time=max_real_time)
    # Check (u)successful creates
    satisfied_creates, unsatisfied_creates = extract_successful_unsuccessful_creates(requests, gens)
    # Compute latencies for requests, i.e. possibly more than one pair/request
    request_times = get_request_latencies(satisfied_creates, gens)
    # Compute latencies for generating a single pair
    gen_starts, gen_times = get_gen_latencies(requests, gens)

    # Get attempts data to compute number of attempts per generation and generation probability
    node_attempts, gen_attempts = parse_attempt_data_from_sql(results_path, all_gens, gen_starts,
                                                              max_real_time=max_real_time)

    # Get fidelities
    fidelities = parse_fidelities_from_sql(results_path, max_real_time=max_real_time)

    # Get QubErr
    Z_data, X_data = parse_quberr_from_sql(results_path, max_real_time=max_real_time)
    avg_Z_err, Z_data_points = Z_data
    avg_X_err, X_data_points = X_data

    # Get queue data
    raw_queue_dataA = parse_table_data_from_sql(results_path, "EGP_Local_Queue_A", max_real_time=max_real_time)
    raw_queue_dataB = parse_table_data_from_sql(results_path, "EGP_Local_Queue_B", max_real_time=max_real_time)

    output_data("-------------------", results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("|Simulation data: |", results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("-------------------", results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("Analysing data in file {}".format(results_path), results_path, save_output=save_output,
                analysis_folder=analysis_folder)
    key_str, run_str = get_key_and_run_from_path(results_path)
    path_to_folder = "/".join(results_path.split('/')[:-1])
    with open(path_to_folder + "/paramcombinations.json") as json_file:
        arguments = json.load(json_file)[key_str]
    output_data("Arguments in paramcombinations.py for this simulation was:", results_path, save_output=save_output,
                analysis_folder=analysis_folder)
    for arg_name, arg in arguments.items():
        output_data("    {}={}".format(arg_name, arg), results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
    output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)
    try:
        output_data("Total 'real time' was {} ns".format(additional_data["total_real_time"]), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
    except KeyError:
        pass
    try:
        output_data("Total 'wall time' was {} s".format(additional_data["total_wall_time"]), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
    except KeyError:
        pass
    try:
        output_data("Bright state population used was: alphaA={}, alphaB={}".format(additional_data["alphaA"],
                                                                                    additional_data["alphaB"]),
                    results_path, save_output=save_output, analysis_folder=analysis_folder)
    except KeyError:
        pass
    output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)

    output_data("-----------------------", results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("|Performance metrics: |", results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("-----------------------", results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("Number of satisfied CREATE requests: {} out of total {}".format(len(satisfied_creates), len(requests)),
                results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)

    if request_times:
        output_data("Average request latency: {} s".format(sum(request_times) / len(request_times) / SECOND),
                    results_path, save_output=save_output, analysis_folder=analysis_folder)
        output_data("Minimum request latency: {} s".format(min(request_times) / SECOND), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("Maximum request latency: {} s".format(max(request_times) / SECOND), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)

    if gen_times:
        output_data("Average generation time: {} s".format(sum(gen_times) / len(all_gens) / SECOND), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("Minimum generation time: {} s".format(min(gen_times) / SECOND), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("Maximum generation time: {} s".format(max(gen_times) / SECOND), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)

    if fidelities:
        output_data("Average fidelity: {} s".format(sum(fidelities) / len(fidelities)), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("Minimum fidelity: {} s".format(min(fidelities)), results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
        output_data("Maximum fidelity: {} s".format(max(fidelities)), results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)

    if avg_Z_err:
        output_data("Average QubErr in Z-basis {} (from {} data points)".format(avg_Z_err, Z_data_points), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
    else:
        output_data("Average QubErr in Z-basis NO_DATA", results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
    if avg_X_err:
        output_data("Average QubErr in X-basis {} (from {} data points)".format(avg_X_err, X_data_points), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)
    else:
        output_data("Average QubErr in X-basis NO_DATA", results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)

    if gen_attempts:
        avg_attempt_per_gen = sum(gen_attempts.values()) / 2 / len(all_gens)
        output_data("Average number of attempts per successful generation: {}".format(avg_attempt_per_gen),
                    results_path, save_output=save_output, analysis_folder=analysis_folder)
        output_data("Minimum number of attempts for a generation: {}".format(min(gen_attempts.values())), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("Maximum number of attempts for a generation: {}".format(max(gen_attempts.values())), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)

    output_data(
        "Total number of generated pairs: {} of total requested {}".format(len(all_gens), total_requested_pairs),
        results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data(
        "Total number of entanglement attempts for successful generations: {}".format(sum(gen_attempts.values()) / 2),
        results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("Total node attempts during simulation: " + "".join(
                ["Node {}: {}, ".format(node, attempts) for node, attempts in node_attempts.items()]), results_path,
                save_output=save_output, analysis_folder=analysis_folder)
    output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)
    output_data("----------------------------------", results_path, save_output=save_output,
                analysis_folder=analysis_folder)
    output_data("|Data useful for queuing theory: |", results_path, save_output=save_output,
                analysis_folder=analysis_folder)
    output_data("----------------------------------", results_path, save_output=save_output,
                analysis_folder=analysis_folder)

    # Check mhp_t_cycle and request_cycle
    try:
        mhp_t_cycle = additional_data["mhp_t_cycle"]
        request_t_cycle = additional_data["request_t_cycle"]
        output_data("The time cycle for MHP was {} ns and for the scheduled requests {} ns".format(mhp_t_cycle,
                                                                                                   request_t_cycle),
                    results_path, save_output=save_output, analysis_folder=analysis_folder)

        # Compute the total number of MHP cycles
        total_real_time = additional_data["total_real_time"]
        number_mhp_cycles = math.floor(total_real_time / mhp_t_cycle)
        output_data("Total number of complete MHP cycles was {}".format(number_mhp_cycles), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
        if number_mhp_cycles > 0:
            fractionA = node_attempts[0] / number_mhp_cycles
        else:
            fractionA = float('inf')
        # fractionB = node_attempts[1]/ number_mhp_cycles
        # TODO ASSUMING THAT NUMBER OF ATTEMPTS ARE EQUAL FOR A AND B
        output_data("Number of attempted entanglement generations at / Number of MHP cycles = {}".format(fractionA),
                    results_path, save_output=save_output, analysis_folder=analysis_folder)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)
        if gen_attempts:
            output_data(
                "Average probability of generating entanglement per attempt: {}".format(1 / avg_attempt_per_gen),
                results_path, save_output=save_output, analysis_folder=analysis_folder)
            output_data("Average probability of generating entanglement per MHP cycle: {}".format(
                1 / avg_attempt_per_gen * fractionA), results_path, save_output=save_output,
                analysis_folder=analysis_folder)
        try:
            output_data("Probability of midpoint declaring success: {}".format(additional_data["p_succ"]), results_path,
                        save_output=save_output, analysis_folder=analysis_folder)
        except KeyError:
            pass
    except KeyError:
        pass
    try:
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)
        output_data("Probability of scheduling a request per request cycle was {} at node A and {} at node B".format(
            additional_data["create_request_probA"], additional_data["create_request_probB"]), results_path,
            save_output=save_output, analysis_folder=analysis_folder)
        output_data(
            "Probability that a scheduled request was on A {}".format(additional_data["create_request_origin_bias"]),
            results_path, save_output=save_output, analysis_folder=analysis_folder)
    except KeyError:
        pass

    # Extract data from raw queue data
    if raw_queue_dataA:
        queue_lensA, qtimesA, max_queue_lenA, avg_queue_lenA, tot_time_in_queueA =\
            parse_raw_queue_data(raw_queue_dataA, max_real_time=max_real_time)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)
        output_data("Max queue length at A: {}".format(max_queue_lenA), results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
        output_data("Average queue length at A: {}".format(avg_queue_lenA), results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
        output_data("Total time items spent in queue at A: {} ns".format(tot_time_in_queueA), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)
    if raw_queue_dataB:
        queue_lensB, qtimesB, max_queue_lenB, avg_queue_lenB, tot_time_in_queueB =\
            parse_raw_queue_data(raw_queue_dataA, max_real_time=max_real_time)
        output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)
        output_data("Max queue length at B: {}".format(max_queue_lenB), results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
        output_data("Average queue length at B: {}".format(avg_queue_lenB), results_path, save_output=save_output,
                    analysis_folder=analysis_folder)
        output_data("Total time items spent in queue at B: {} ns".format(tot_time_in_queueB), results_path,
                    save_output=save_output, analysis_folder=analysis_folder)

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
    # Check if results_path is a single .db file or a folder containing such
    if results_path.endswith('.db'):
        analyse_single_file(results_path, no_plot, max_real_time=max_real_time, save_figs=save_figs,
                            save_output=save_output, analysis_folder=analysis_folder)
    else:
        if results_path.endswith('/'):
            results_path = results_path[:-1]
        for entry in os.listdir(results_path):
            if entry.endswith('.db'):
                output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)
                output_data("====================================", results_path, save_output=save_output,
                            analysis_folder=analysis_folder)
                analyse_single_file(results_path + "/" + entry, no_plot, max_real_time=max_real_time,
                                    save_figs=save_figs, save_output=save_output, analysis_folder=analysis_folder)
                output_data("====================================", results_path, save_output=save_output,
                            analysis_folder=analysis_folder)
                output_data("", results_path, save_output=save_output, analysis_folder=analysis_folder)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--results-path', required=True, type=str,
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
