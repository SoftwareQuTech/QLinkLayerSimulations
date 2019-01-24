import sys
import json
from matplotlib import pyplot as plt

from simulations.major_simulation.generate_metrics_file import parse_thoughput, sort_data_by_request


def get_max_time(results_file):
    # Simulation time
    additional_data_filename = results_file[:-3] + "_additional_data.json"
    with open(additional_data_filename, 'r') as f:
        additional_data = json.load(f)
    total_matrix_time = additional_data["total_real_time"]

    return total_matrix_time

def sweeeping_average(datapoints, nr_points=1):
    averaged_datapoints = []
    for i in range(len(datapoints)):
        left_side = max(0, i - nr_points)
        right_side = min(len(datapoints) + 1, i + 1 + nr_points)
        value = sum(datapoints[left_side:right_side]) / (2 * nr_points + 1)
        averaged_datapoints.append(value)
    return averaged_datapoints


def plot_throughput(results_file, last_plot=False):
    creates_and_oks_by_create_id, _ = sort_data_by_request(results_file)
    total_matrix_time = get_max_time(results_file)
    throughputs_per_prio = parse_thoughput(creates_and_oks_by_create_id, max_time=total_matrix_time, num_points=10000, time_window=10e9, in_seconds=True)

    prio_names = {0: "NL", 1: "CK", 2: "MD"}

    markers = ["<", ">", "*"]
    linestyles = ["-", "--", ":"]
    colors = ['C0', 'C1', 'C2']

    for prio in range(2, -1, -1):
        throughputs = throughputs_per_prio[prio]
        times, tps = zip(*throughputs)
        avg_tps = sweeeping_average(tps, nr_points=1)
        plt.plot(times, avg_tps, label=prio_names[prio], linestyle=linestyles[prio], color=colors[prio])

    # plt.yscale('log')
    plt.ylim(0, 10)
    if last_plot:
        plt.xlabel("Simulated time (s)")
    plt.ylabel("Throughput (1/s)")
    if not last_plot:
        plt.legend(loc='upper left')
    scenario_key = results_file.split("_key_")[1].split("_run_")[0]
    if "FIFO" in scenario_key:
        scheduler = "FCFS"
    else:
        scheduler = "HigherWFQ"
    plt.title("Scheduler: {}".format(scheduler))
    plt.gca().axes.set_aspect(40)
    if not last_plot:
        # plt.gca().axes.get_xaxis().set_visible(False)
        plt.gca().axes.set_xticklabels([])


def main(results_files):
    num_files = len(results_files)
    plt.rcParams.update({'font.size': 12})
    for i, results_file in enumerate(results_files):
        plt.subplot(num_files, 1, i + 1)
        last_plot = (i == num_files - 1)
        plot_throughput(results_file, last_plot=last_plot)
    plt.subplots_adjust(hspace=-0.2)
    # plt.savefig("/Volumes/Untitled/Dropbox/my_linklayer/plots/thoughput_vs_time.png", bbox_inches='tight')
    plt.show()

if __name__ == '__main__':
    # results_files = sys.argv[1:]
    # results_files = [
    #     "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure/2019-01-16T11:10:28CET_key_QLINK_WC_WC_mix_uniform_weights_FIFO_run_0.db",
    #     "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure/2019-01-16T11:10:28CET_key_QLINK_WC_WC_mix_uniform_weights_higherWFQ_run_0.db"
    # ]
    results_files = [
        "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure/2019-01-16T11:10:28CET_key_QLINK_WC_WC_mix_noNLmoreMD_weights_FIFO_run_0.db",
        "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure/2019-01-16T11:10:28CET_key_QLINK_WC_WC_mix_noNLmoreMD_weights_higherWFQ_run_0.db"
    ]
    main(results_files)
