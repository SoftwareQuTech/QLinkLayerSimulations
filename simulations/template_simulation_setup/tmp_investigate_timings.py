import json
import matplotlib.pyplot as plt
import sqlite3
from argparse import ArgumentParser
from collections import defaultdict
import numpy as np
import os


SECOND = 1e9


def parse_request_data_from_sql(results_path):
    """
    Parses collected request/ok data points from the sql database
    :param results_path: str
        Path to directory containing request.log
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
    conn = sqlite3.connect(results_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [t[0] for t in c.fetchall()]

    create_tables = list(filter(lambda table_name: "EGP_Creates" in table_name, all_tables))
    requests = {}
    rejected_requests = []
    total_requested_pairs = 0

    for create_table in create_tables:
        c.execute("SELECT * FROM {}".format(create_table))
        for entry in c.fetchall():
            timestamp, nodeID, create_id, create_time, max_time, min_fidelity, num_pairs, otherID, priority,\
                purpose_id, store, succ = entry

            total_requested_pairs += num_pairs

            if create_id is not None and create_time is not None:
                requests[(create_id, nodeID, otherID)] = [nodeID, otherID, num_pairs, create_id, create_time, max_time]
            else:
                rejected_requests.append([nodeID, otherID, num_pairs, timestamp])

    ok_tables = filter(lambda table_name: "EGP_OKs" in table_name, all_tables)
    gens = defaultdict(list)
    all_gens = []
    recorded_mhp_seqs = []

    for ok_table in ok_tables:
        c.execute("SELECT * FROM {}".format(ok_table))
        for entry in c.fetchall():
            timestamp, createID, originID, otherID, MHPSeq, logical_id, goodness, t_goodness, t_create, succ = entry

            if MHPSeq in recorded_mhp_seqs:
                continue
            else:
                recorded_mhp_seqs.append(MHPSeq)

            gens[(createID, originID, otherID)].append([createID, originID, otherID, MHPSeq, t_create])
            all_gens.append((t_create, (createID, originID, otherID, MHPSeq)))

    return (requests, rejected_requests), (gens, all_gens), total_requested_pairs


def parse_attempt_data_from_sql(results_path, all_gens, gen_starts):
    """
    Parses collected attempt data points from the sql database
    :param results_path: str
        Path to directory containing node_attempt.log containing attempts from both nodes
    :param all_gens: list of (createTime, (createID, sourceID, otherID, mhpSeq))
        All generations that occurred during the simulation
    :param gen_starts: dict of key (createID, sourceID, otherID, mhpSeq)
                               value createTime
        Contains the completion time of each generation
    :return: node_attempts, gen_attempts
        node_attempts - dict of key nodeID
                                value num_attempts
            Containing the number of attempts made by each node
        gen_attempts - dict of key (createID, sourceID, otherID, mhpSeq)
                               value int
            Containing the number of attempts made for the generation
    """
    conn = sqlite3.connect(results_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [t[0] for t in c.fetchall()]

    node_attempt_tables = list(filter(lambda table_name: "Node_EGP_Attempts" in table_name, all_tables))
    gen_attempts = defaultdict(int)
    node_attempts = defaultdict(int)
    for attempt_table in node_attempt_tables:
        c.execute("SELECT * FROM {}".format(attempt_table))
        for entry in c.fetchall():
            timestamp, nodeID, succ = entry
            node_attempts[nodeID] += 1

            for gen in all_gens:
                genID = gen[1]
                if gen_starts[genID] < timestamp < gen[0]:
                    gen_attempts[genID] += 1
                    break

    return node_attempts, gen_attempts

def get_all_attempts_from_sql(results_path):
    """
    tmp function use above...
    """
    conn = sqlite3.connect(results_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [t[0] for t in c.fetchall()]

    node_attempt_tables = list(filter(lambda table_name: "Node_EGP_Attempts" in table_name, all_tables))
    node_attempts = []
    for attempt_table in node_attempt_tables:
        c.execute("SELECT * FROM {}".format(attempt_table))
        for entry in c.fetchall():
            node_attempts.append(entry)

    return node_attempts


def get_all_entries_from_sql(results_path, name):
    """
    tmp function use above...
    """
    conn = sqlite3.connect(results_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [t[0] for t in c.fetchall()]

    tables = list(filter(lambda table_name: name in table_name, all_tables))
    entries = []
    for table in tables:
        c.execute("SELECT * FROM {}".format(table))
        for entry in c.fetchall():
            entries.append(entry)

    return entries


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


def plot_gen_attempts(gen_attempts):
    """
    Plots a histogram and a distribution of the number of attempts for generations in the simulation
    :param gen_attempts: dict of key (createID, sourceID, otherID, mhpSeq)
                                 value int
        Containing the number of attempts made for the generation
    """
    plt.hist(gen_attempts.values(), 50)

    plt.xlabel('Attempts')
    plt.ylabel('Generation Count')
    plt.title('Attempt Histogram')
    plt.grid(True)
    plt.show()

    t = gen_attempts.values()
    s = [0] * len(t)
    fig, ax = plt.subplots()
    ax.plot(t, s, '.')

    ax.set(xlabel='time (s)', title='Generation Latency Distribution')
    ax.grid()
    plt.show()


def plot_gen_times(gen_times):
    """
    Plots a histogram and a distribution of the amount of time for generations in the simulation
    :param gen_times: list of floats
        The amount of time per generation
    """
    gen_times = [t / 1e9 for t in gen_times]
    plt.hist(gen_times, 50)

    plt.xlabel('Generation Latency')
    plt.ylabel('Generation Count')
    plt.title('Generation Latency Histogram')
    plt.grid(True)
    plt.show()

    t = gen_times
    s = [0] * len(t)
    fig, ax = plt.subplots()
    ax.plot(t, s, '.')

    ax.set(xlabel='attempts', title='Generation Attempt Count Distribution')
    ax.grid()
    plt.show()


def plot_throughput(all_gens):
    """
    Plots the instantaneous throughput of entanglement generation as a function of time over the
    duration of the simulation
    :param all_gens: list of (createTime, (createID, sourceID, otherID, mhpSeq))
        Contains the create time of the generations
    """
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
    t = [t[0] / 1e9 for t in throughput]
    fig, ax = plt.subplots()
    ax.plot(t, s, '.-')

    ax.set(xlabel='time (s)', ylabel='Throughput (gen/s)',
           title='Instantaneous throughput of generation')
    ax.grid()
    plt.show()


def main(results_path, no_plot=False):
    if results_path == "":
        folders = []
        for folder in os.listdir("./"):
            if folder[:4] == "2018":
                folders.append(folder)
        last_folder = sorted(folders)[-1]

        # Find datafile
        for file in os.listdir("./{}".format(last_folder)):
            if file[-3:] == ".db":
                break
        results_path = "./{}/{}".format(last_folder, file)
        # print("./{}/{}".format(last_folder, file))

    # node_attempts = get_all_entries_from_sql(results_path, "Node_EGP_Attempts")
    # nr_attempts = len(node_attempts)
    #
    # print("Node attempts:")
    # for i in range(nr_attempts):
    #     print("    Node attempt {}: {}".format(i + 1, node_attempts[i]))
    #
    # try:
    #     time_diff = node_attempts[2][0] - node_attempts[0][0]
    #     print("time_diff: {}".format(time_diff))
    # except IndexError:
    #     pass
    #
    # mid_attempts = get_all_entries_from_sql(results_path, "Midpoint_EGP_Attempts")
    # print("Midpoint attempts:")
    # for entry in mid_attempts:
    #     print("    Mid attempt: {}".format(entry))
    #
    Attempts = get_all_entries_from_sql(results_path, "Node_EGP_Attempts")
    print("Attemptss:")
    for entry in Attempts:
        print("    Attempt: {}".format(entry))

    OKs = get_all_entries_from_sql(results_path, "EGP_OKs")
    print("OKs:")
    for entry in OKs:
        print("    OK: {}".format(entry))
    #
    # states = get_all_entries_from_sql(results_path, "EGP_Qubit_States")
    # print("Fidelities:")
    # for entry in states:
    #     m_data = entry[2:34]
    #     m = np.matrix([[m_data[i] + 1j * m_data[i+1] for i in range(k, k + 8, 2)] for k in range(0, len(m_data), 8)])
    #     fidelity = calc_fidelity(m)
    #     print("    Fidelity: {}".format(fidelity))

    QubErrs = get_all_entries_from_sql(results_path, "EGP_QubErr")
    print("QubErrs:")
    Z_err = []
    X_err = []
    for entry in QubErrs:
        print("    QubErr: {}".format(entry))
        if entry[1] in [0, 1]:
            Z_err.append(entry[1])
        if entry[2] in [0, 1]:
            X_err.append(entry[2])
    if len(Z_err) > 0:
        print("Z_err: {}".format(sum(Z_err)/len(Z_err)))
    else:
        print("Z_err: NO DATA")
    if len(X_err) > 0:
        print("X_err: {}".format(sum(X_err)/len(X_err)))
    else:
        print("X_err: NO DATA")


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


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--results-path', required=False, default="", type=str,
                        help="Path to the directory containing the simulation results")
    parser.add_argument('--no-plot', default=False, action='store_true',
                        help="Whether to produce plots or not")

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    main(results_path=args.results_path, no_plot=args.no_plot)
