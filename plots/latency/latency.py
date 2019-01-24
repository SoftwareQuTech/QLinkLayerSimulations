import sys
import json
from matplotlib import pyplot as plt

from simulations.major_simulation.generate_metrics_file import get_raw_metric_data


def get_max_time(results_file):
    # Simulation time
    additional_data_filename = results_file[:-3] + "_additional_data.json"
    with open(additional_data_filename, 'r') as f:
        additional_data = json.load(f)
    total_matrix_time = additional_data["total_real_time"]

    return total_matrix_time


def plot_latency(results_file, last_plot=False):
    all_raw_metric_data = get_raw_metric_data(results_file)
    scaled_req_latencies_per_prio_per_node = all_raw_metric_data["scaled_req_latencies_per_prio_per_node"]

    prio_names = {0: "NL", 1: "CK", 2: "MD"}

    markers = ["<", ">", "*"]
    linestyles = ["-", "--", ":"]
    colors = ['C0', 'C1', 'C2']

    # for prio, scaled_laten_per_node in scaled_req_latencies_per_prio_per_node.items():
    for prio in range(3):
        scaled_laten_per_node = scaled_req_latencies_per_prio_per_node[prio]
        if prio < 3:
            scaled_laten = scaled_laten_per_node[0]
            latens = [l[1] * 1e-9 for l in scaled_laten]
            times = [l[0] * 1e-9 for l in scaled_laten]
            plt.plot(times, latens, label=prio_names[prio], linestyle=linestyles[prio], color=colors[prio])

    # plt.yscale('log')
    plt.ylim(0, 100)
    if last_plot:
        plt.xlabel("Simulated time (s)")
    plt.ylabel("Scaled latency (s)")
    if last_plot:
        plt.legend(loc='upper left')
    scenario_key = results_file.split("_key_")[1].split("_run_")[0]
    if "FIFO" in scenario_key:
        scheduler = "FCFS"
    else:
        scheduler = "HigherWFQ"
    plt.title("Scheduler: {}".format(scheduler))
    plt.gca().axes.set_aspect(4)
    if not last_plot:
        # plt.gca().axes.get_xaxis().set_visible(False)
        plt.gca().axes.set_xticklabels([])


def main(results_files):
    num_files = len(results_files)
    plt.rcParams.update({'font.size': 12})
    for i, results_file in enumerate(results_files):
        plt.subplot(num_files, 1, i + 1)
        last_plot = (i == num_files - 1)
        plot_latency(results_file, last_plot=last_plot)
    plt.subplots_adjust(hspace=-0.2)
    # plt.savefig("/Volumes/Untitled/Dropbox/my_linklayer/plots/latency_vs_time.png", bbox_inches='tight')
    plt.show()

if __name__ == '__main__':
    main(sys.argv[1:])
