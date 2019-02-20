import os
import json
from matplotlib import pyplot as plt
from argparse import ArgumentParser

from simulations.generate_metrics_file import parse_thoughput, sort_data_by_request


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


def plot_throughput(results_file, last_plot=False, max_x=10, max_y=1380):
    creates_and_oks_by_create_id, _ = sort_data_by_request(results_file)
    total_matrix_time = get_max_time(results_file)
    throughputs_per_prio = parse_thoughput(creates_and_oks_by_create_id, max_time=total_matrix_time, num_points=10000, time_window=10e9, in_seconds=True)

    prio_names = {0: "NL", 1: "CK", 2: "MD"}

    linestyles = ["-", "--", ":"]
    colors = ['C0', 'C1', 'C2']

    for prio in range(2, -1, -1):
        throughputs = throughputs_per_prio[prio]
        times, tps = zip(*throughputs)

        if times[0] is not None:

            # Sort the entries by times
            times, tps = zip(*sorted(zip(times, tps), key=lambda x: x[0]))

            avg_tps = sweeeping_average(tps, nr_points=1)
            plt.plot(times, avg_tps, label=prio_names[prio], linestyle=linestyles[prio], color=colors[prio])

    plt.xlim(0, max_x)
    plt.ylim(0, max_y)
    if last_plot:
        plt.xlabel("Simulated time (s)")
    if not last_plot:
        plt.legend(loc='upper left')
    scenario_key = results_file.split("_key_")[1].split("_run_")[0]
    if "FIFO" in scenario_key:
        scheduler = "FCFS"
    else:
        scheduler = "HigherWFQ"
    ax = plt.gca().axes
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    plt.text(0.99, 0.95, scheduler, horizontalalignment='right', verticalalignment='top', transform=ax.transAxes, bbox=props)
    scale_factor = max_x / max_y * 0.29
    ax.set_aspect(scale_factor)
    if not last_plot:
        ax.set_xticklabels([])
        tick_pos = plt.yticks()[0]
        tick_names = [int(pos) for pos in tick_pos]
        plt.yticks(tick_pos, tick_names)
    else:
        tick_pos = plt.yticks()[0][:-1]
        tick_names = [int(pos) for pos in tick_pos]
        plt.yticks(tick_pos, tick_names)
        plt.text(-0.1, 1, 'Throughput (1/s)',
                 horizontalalignment='right',
                 verticalalignment='center',
                 rotation='vertical',
                 transform=ax.transAxes)


def plot_throughput_compare_scheduling(results_files, max_x=1380, max_y=10, name=None, save_dir=None):
    num_files = len(results_files)
    plt.rcParams.update({'font.size': 12})
    for i, results_file in enumerate(results_files):
        plt.subplot(num_files, 1, i + 1)
        last_plot = (i == num_files - 1)
        plot_throughput(results_file, last_plot=last_plot, max_x=max_x, max_y=max_y)
    plt.subplots_adjust(hspace=-0.32)
    if name:
        # pass
        plt.savefig(save_dir + "throughput_vs_time_{}.png".format(name), bbox_inches='tight')
    # plt.show()
    plt.close()

def main(runs, plot_dirs):
    mix_to_mix_in_data = {"Uniform": "uniform",
                          "MoreNL": "moreNL",
                          "MoreCK": "moreCK",
                          "MoreMD": "moreMD",
                          "NoNLMoreCK": "noNLmoreCK",
                          "NoNLMoreMD": "noNLmoreMD",
                          }
    for run_dir, save_dir in zip(runs, plot_dirs):
        for phys_setup in ["QL2020", "Lab"]:
            mixes = ["Uniform", "MoreNL", "MoreCK", "MoreMD", "NoNLMoreCK", "NoNLMoreMD"]
            if phys_setup == "QL2020":
                max_ys = [10] * 6
                max_xs = [1380, 2000, 2000, 1000, 2200, 800]
                phys_setup_in_data = "QLINK_WC_WC"
            else:
                max_ys = [10] * 6
                max_xs = [613, 637, 623, 550, 684, 630]
                phys_setup_in_data = "LAB_NC_NC"

            for mix, max_x, max_y in zip(mixes, max_xs, max_ys):
                name = "{}_{}".format(phys_setup, mix)
                mix_in_data = mix_to_mix_in_data[mix]

                dir_name = os.path.split(run_dir)[1]
                timestamp = dir_name.split('_')[0]
                results_basename = os.path.join(run_dir, timestamp)

                results_files = [results_basename + "_key_{}_mix_{}_weights_{}_run_0.db".format(phys_setup_in_data, mix_in_data, sched) for sched in ["FIFO", "higherWFQ"]]

                print(name)
                plot_throughput_compare_scheduling(results_files, max_x=max_x, max_y=max_y, name=name, save_dir=save_dir)

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--run1', required=True, type=str,
                        help="Path to directory containing data for simulation run 1."
                             "(Should be */2019-01-16T11:10:28CET_CREATE_and_measure)")
    parser.add_argument('--run2', required=True, type=str,
                        help="Path to directory containing data for simulation run 2."
                             "(Should be */2019-01-15T23:56:55CET_CREATE_and_measure)")
    parser.add_argument('--plots_run1', required=False, type=str, default=None,
                        help="Path to directory where the plots for run 1 should be saved."
                             "If not used the plots are simply shown and not saved.")
    parser.add_argument('--plots_run2', required=False, type=str, default=None,
                        help="Path to directory where the plots for run 2 should be saved."
                             "If not used the plots are simply shown and not saved.")

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    runs = [args.run1, args.run2]
    plot_dirs = [args.plots_run1, args.plots_run2]
    main(runs, plot_dirs)
