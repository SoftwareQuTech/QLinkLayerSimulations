import json
import matplotlib.pyplot as plt
import sqlite3
from argparse import ArgumentParser
from collections import defaultdict


SECOND = 1e9


def parse_request_data_from_log(results_path):
    """
    Parses collected request/ok data points for log file
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
    with open('{}/request.log'.format(results_path)) as f:
        lines = f.read().splitlines()

        requests = {}
        rejected_requests = []
        gens = defaultdict(list)
        all_gens = []
        total_requested_pairs = 0
        recorded_mhp_seqs = []

        for line in lines:
            elements = line.split(' ')
            data = elements[5:]

            timeStamp = float(data[0].strip(','))

            # Request create data
            if len(data) > 12:
                json_string = ''.join(data[3:19]).strip('),').replace('None', 'null').replace("'", '"')
                request_vars = json.loads(json_string)
                # Extract request info
                sourceID = int(data[2].strip('(,'))

                otherID = request_vars['otherID']
                numPairs = request_vars['num_pairs']
                total_requested_pairs += numPairs
                maxTime = request_vars['max_time']

                # Extract creation info
                createID = request_vars['create_id']
                createTime = request_vars['create_time']

                if createID is not None and createTime is not None:
                    requests[(createID, sourceID, otherID)] = [sourceID, otherID, numPairs, createID, createTime,
                                                               maxTime]
                else:
                    rejected_requests.append([sourceID, otherID, numPairs, timeStamp])

            # OK Response data
            else:
                createID = int(data[2].strip('(,'))
                sourceID = int(data[3].strip('(,'))
                otherID = int(data[4].strip(','))
                createTime = float(data[8].strip(','))
                mhpSeq = int(data[5].strip(','))
                if mhpSeq in recorded_mhp_seqs:
                    continue
                else:
                    recorded_mhp_seqs.append(mhpSeq)

                gens[(createID, sourceID, otherID)].append([createID, sourceID, otherID, mhpSeq, createTime])
                all_gens.append((createTime, (createID, sourceID, otherID, mhpSeq)))

    return (requests, rejected_requests), (gens, all_gens), total_requested_pairs


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
                purpose_id, succ = entry

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


def parse_attempt_data_from_log(results_path, all_gens, gen_starts):
    """
    Parses collected attempt data points for log file
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
    gen_attempts = defaultdict(int)
    node_attempts = defaultdict(int)
    with open('{}/node_attempt.log'.format(results_path)) as f:
        for line in f:
            elements = line.split(' ')
            timestamp = float(elements[5].strip(','))
            nodeID = int(elements[7].strip(','))
            node_attempts[nodeID] += 1

            for gen in all_gens:
                genID = gen[1]
                if gen_starts[genID] < timestamp < gen[0]:
                    gen_attempts[genID] += 1
                    break

    return node_attempts, gen_attempts


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


def main(results_path):
    (requests, rejected_requests), (gens, all_gens), total_requested_pairs = parse_request_data_from_sql(results_path)
    satisfied_creates, unsatisfied_creates = extract_successful_unsuccessful_creates(requests, gens)
    request_times = get_request_latencies(satisfied_creates, gens)
    gen_starts, gen_times = get_gen_latencies(requests, gens)
    node_attempts, gen_attempts = parse_attempt_data_from_sql(results_path, all_gens, gen_starts)

    print("Number of satisfied CREATE requests: {} out of total {}".format(len(satisfied_creates), len(requests)))
    print("Satisfied CREATE requests: {}".format(satisfied_creates))
    print("Unsatisfied CREATE requests: {}".format(unsatisfied_creates))
    print("Rejected CREATE requests: {}".format(rejected_requests))

    if request_times:
        print("Average request latency: {}".format(sum(request_times) / len(request_times) / 1e9))
        print("Minimum request latency: {}".format(min(request_times) / 1e9))
        print("Maximum request latency: {}".format(max(request_times) / 1e9))

    if gen_times:
        print("Average generation time: {}".format(sum(gen_times) / len(all_gens) / 1e9))
        print("Minimum generation time: {}".format(min(gen_times) / 1e9))
        print("Maximum generation time: {}".format(max(gen_times) / 1e9))

    if gen_attempts:
        avg_attempt_per_gen = sum(gen_attempts.values()) / 2 / len(all_gens)
        print("Average number of attempts per successful generation: {}".format(avg_attempt_per_gen))
        print("Minimum number of attempts for a generation: {}".format(min(gen_attempts.values())))
        print("Maximum number of attempts for a generation: {}".format(max(gen_attempts.values())))
        print("Average probability of generating entanglement per attempt: {}".format(1 / avg_attempt_per_gen))

    print("Total number of generated pairs: {} of total requested {}".format(len(all_gens), total_requested_pairs))
    print("Total number of entanglement attempts for successful generations: {}".format(sum(gen_attempts.values()) / 2))
    print("Total node attempts during simulation: {}".format(node_attempts))

    if gen_attempts:
        plot_gen_attempts(gen_attempts)

    if gen_times:
        plot_gen_times(gen_times)

    if all_gens:
        plot_throughput(all_gens)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--results-path', required=True, type=str,
                        help="Path to the directory containing the simulation results")

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    main(results_path=args.results_path)
