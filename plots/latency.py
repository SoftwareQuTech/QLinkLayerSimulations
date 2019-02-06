import os
import json
from argparse import ArgumentParser
from matplotlib import pyplot as plt

from simulations.major_simulation.generate_metrics_file import get_raw_metric_data
from simulations.analysis_sql_data import parse_table_data_from_sql
from qlinklayer.datacollection import EGPCreateDataPoint


def get_max_time(results_file):
    """
    Returns the max simulated time (ns) for a data file
    :param results_file: str
        The path to the json file containing the additional data from the simulation.
    :return: float
        Max simulated time (ns)
    """
    # Simulation time
    additional_data_filename = results_file[:-3] + "_additional_data.json"
    with open(additional_data_filename, 'r') as f:
        additional_data = json.load(f)
    total_matrix_time = additional_data["total_real_time"]

    return total_matrix_time


def get_MD_times(results_file):
    """
    Returns a sorted list of the times when a MD request was submitted to the link layer
    :param results_file: str
        The path to the .db file containing the data from the simulation run.
    :return: list of floats
        Times (ns) for submitted MD requests
    """
    creates_data = parse_table_data_from_sql(results_file, 'EGP_Creates')

    MD_times = []

    for raw_datapoint in creates_data:
        datapoint = EGPCreateDataPoint(raw_datapoint)
        MD = datapoint.measure_directly
        if MD:
            MD_times.append(datapoint.create_time)

    return sorted(MD_times)


def plot_latency(results_file, last_plot=False, max_x=1400, max_y=100, lat_type=None):
    """
    Creates a plot for latency per request priority vs simulated time.
    Assumes that a subplot enviroment with two rows and one column has been initialized.
    last_plot=False for the upper plot and True for the lower plot, to place legend correclty.
    :param results_file: str
        The path to the json file containing the additional data from the simulation.
    :param last_plot: bool
        True for lower plot, False for upper
    :param max_x: float
        Max value on x-axis
    :param max_y: float
        Max value on y-axis
    :param lat_type: str or None
        Whether request, pair or scaled request latency should be plotted. If None, scaled request latency is used.
    :return: None
    """
    all_raw_metric_data = get_raw_metric_data(results_file)
    if lat_type is None or lat_type == "scaled":
        latencies_per_prio_per_node = all_raw_metric_data["scaled_req_latencies_per_prio_per_node"]
        ylabel = "Scaled Latency (s)"
    elif lat_type == "req":
        latencies_per_prio_per_node = all_raw_metric_data["req_latencies_per_prio_per_node"]
        ylabel = "Req. Latency (s)"
    elif lat_type == "pair":
        latencies_per_prio_per_node = all_raw_metric_data["pair_latencies_per_prio_per_node"]
        ylabel = "Pair Latency (s)"
    else:
        raise ValueError("Unknown lat_type = {}".format(lat_type))

    prio_names = {0: "NL", 1: "CK", 2: "MD"}

    linestyles = ["-", "--", ":"]
    colors = ['C0', 'C1', 'C2']

    # for prio, scaled_laten_per_node in scaled_req_latencies_per_prio_per_node.items():
    for prio in range(3):
        try:
            scaled_laten_per_node = latencies_per_prio_per_node[prio]
        except KeyError:
            pass
        else:
            if prio < 3:
                scaled_laten = scaled_laten_per_node[0]
                latens = [l[1] * 1e-9 for l in scaled_laten]
                times = [l[0] * 1e-9 for l in scaled_laten]

                # Sort the entries by times
                times, latens = zip(*sorted(zip(times, latens), key=lambda x: x[0]))
                if last_plot:
                    plt.plot(times, latens, label=prio_names[prio], linestyle=linestyles[prio], color=colors[prio])
                else:
                    plt.plot(times, latens, label=None, linestyle=linestyles[prio], color=colors[prio])

    plt.xlim(0, max_x)
    plt.ylim(0, max_y)
    if last_plot:
        plt.xlabel("Simulated time (s)")
    if last_plot:
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
    else:
        tick_pos = plt.yticks()[0][:-1]
        tick_names = [int(pos) for pos in tick_pos]
        plt.yticks(tick_pos, tick_names)
        plt.text(-0.1, 1, ylabel,
                 horizontalalignment='right',
                 verticalalignment='center',
                 rotation='vertical',
                 transform=ax.transAxes)


def plot_latency_compare_scheduling(results_files, max_x=1400, max_y=100, name=None, save_dir=None):
    num_files = len(results_files)
    plt.rcParams.update({'font.size': 12})
    for lat_type in ["scaled", "req", "pair"]:
        for i, results_file in enumerate(results_files):
            plt.subplot(num_files, 1, i + 1)
            last_plot = (i == num_files - 1)
            plot_latency(results_file, last_plot=last_plot, max_x=max_x, max_y=max_y, lat_type=lat_type)
        plt.subplots_adjust(hspace=-0.32)
        if (save_dir is not None) and (name is not None):
            plt.savefig(os.path.join(save_dir, "{}_latency_vs_time_{}.png".format(lat_type, name)), bbox_inches='tight')
            plt.close()
        else:
            plt.show()


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
                max_ys = [100] + [200] * 5
                max_xs = [1380, 2000, 2000, 1000, 2200, 800]
                phys_setup_in_data = "QLINK_WC_WC"
            else:
                max_ys = [100] + [200] * 5
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
                plot_latency_compare_scheduling(results_files, max_x=max_x, max_y=max_y, name=name, save_dir=save_dir)


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
