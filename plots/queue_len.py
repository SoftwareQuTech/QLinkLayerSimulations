import sys
import json
from matplotlib import pyplot as plt

from simulations.major_simulation.generate_metrics_file import parse_thoughput, sort_data_by_request
from simulations.analysis_sql_data import parse_table_data_from_sql, parse_raw_queue_data


def get_max_time(results_file):
    # Simulation time
    additional_data_filename = results_file[:-3] + "_additional_data.json"
    with open(additional_data_filename, 'r') as f:
        additional_data = json.load(f)
    total_matrix_time = additional_data["total_real_time"]

    return total_matrix_time


def plot_queue_lens(results_file, last_plot=False):
    queue_ids = range(3)
    raw_all_queue_data = {qid: parse_table_data_from_sql(results_file, "EGP_Local_Queue_A_{}".format(qid)) for qid in queue_ids}
    all_queue_lengths = {}
    all_times = {}
    for qid, raw_queue_data in raw_all_queue_data.items():
        queue_data = parse_raw_queue_data(raw_queue_data)
        all_queue_lengths[qid] = queue_data[0]
        all_times[qid] = queue_data[1]

    # prio_names = {0: "NL", 1: "CK", 2: "MD"}
    for qid in queue_ids:
        plt.plot(all_times[qid], all_queue_lengths[qid], label="Queue{}".format(qid))

    if last_plot:
        plt.xlabel("Matrix time (ns)")
    plt.ylabel("Queue len")
    if last_plot:
        plt.legend(loc='upper right')
    scenario_key = results_file.split("_key_")[1].split("_run_")[0]
    plt.title("Scenario: {}".format(scenario_key))
    if not last_plot:
        # plt.gca().axes.get_xaxis().set_visible(False)
        plt.gca().axes.set_xticklabels([])

    # creates_and_oks_by_create_id, _ = sort_data_by_request(results_file)
    # total_matrix_time = get_max_time(results_file)
    # throughputs_per_prio = parse_thoughput(creates_and_oks_by_create_id, max_time=total_matrix_time, num_points=100, in_seconds=True)
    #
    # prio_names = {0: "NL", 1: "CK", 2: "MD"}
    #
    # for prio, throughputs in throughputs_per_prio.items():
    #     plt.plot(*zip(*throughputs), label=prio_names[prio])
    #
    # if last_plot:
    #     plt.xlabel("Matrix time (s)")
    # plt.ylabel("Throughput (1/s)")
    # if last_plot:
    #     plt.legend(loc='upper right')
    # scenario_key = results_file.split("_key_")[1].split("_run_")[0]
    # plt.title("Scenario: {}".format(scenario_key))
    # if not last_plot:
    #     # plt.gca().axes.get_xaxis().set_visible(False)
    #     plt.gca().axes.set_xticklabels([])


def main(results_files):
    num_files = len(results_files)
    for i, results_file in enumerate(results_files):
        plt.subplot(num_files, 1, i + 1)
        last_plot = (i == num_files - 1)
        plot_queue_lens(results_file, last_plot=last_plot)
    plt.show()

if __name__ == '__main__':
    main(sys.argv[1:])
