import matplotlib.pyplot as plt
import numpy as np
from simulations.analysis_sql_data import parse_request_data_from_sql, calc_throughput, get_gen_latencies, \
    parse_table_data_from_sql, parse_raw_queue_data

# NOTE!! THIS IS A TEMPORARY FILE FOR PRODUCING PLOTS
# SHOULD NOT BE SEEN AS A GENERAL USE SCRIPT
# EDIT THE FOLLOWING PATHS TO THE DATA FROM THE SIMULATIONS
# BELOW IS A COMMENTED EXAMPLE OF WHAT THIS CAN LOOK LIKE
#
# Author: Axel Dahlberg
filebasename = "/Users/adahlberg/Documents/QLinkLayer/simulations/create_measure_simulation/" \
               "2018-09-12T18:38:51CEST_CREATE_and_measure/2018-09-12T18:38:51CEST_key_"
# filebasename = "/Users/adahlberg/Documents/QLinkLayer/simulations/create_measure_simulation/" \
#                "2018-09-17T15:31:21CEST_CREATE_and_measure/2018-09-17T15:31:21CEST_key_"
keys = [4, 5, 18, 19]
# keys = [5, 19]
# keys = [4, 18]
key_to_name = {4: "lab_no_cav_no_conv_low_req_freq",
               5: "lab_no_cav_no_conv_high_req_freq",
               18: "qlink_no_cav_no_conv_low_req_freq",
               19: "qlink_no_cav_no_conv_high_req_freq"}
files = [filebasename + "{}_run_0.db".format(key) for key in keys]
file_to_name = {}
for key in keys:
    file_to_name[filebasename + "{}_run_0.db".format(key)] = key_to_name[key]
paths = files
labels = ["Lab_NoCav_NoCav_LowReqFreq", "Lab_NoCav_NoCav_HighReqFreq", "Qlink_WithCav_WithConv_LowReqFreq",
          "Qlink_WithCav_WithConv_HighReqFreq"]
colors = ["blue", "red", "green", "purple"]

SECOND = 1e9


def plot_all_generation_times(all_gen_times, num_bins=10, scale=True, scale_real_time=None, log_y_scale=True, show=True,
                              fig_name=None, xlabel="Latency (s)"):
    """
    Plots the generation times of multiple simulations
    :param all_gen_times: A list of list containing the generation times for different simulations
    :param num_bins: Number of bins to be used by the histogram
    :param scale: Whether to scale the bins to sum up to 1 or not
    :param log_y_scale: Whether to use log-scale for the y-axis
    :param show: Whether to show the plot or not
    :param fig_name: Name of figure to save, if None no figure is saved
    :return: None
    """
    plt.clf()
    if isinstance(scale_real_time, float):
        scale_real_time = [scale_real_time] * len(all_gen_times)
    if scale_real_time:
        all_gen_times = [list(map(lambda t: t / scale_real_time[i], all_gen_times[i])) for i in
                         range(len(all_gen_times))]

    # Get max gen_time
    max_gen_time = max([max(gen_times, default=0) for gen_times in all_gen_times], default=0)

    # Compute bins
    bins = np.linspace(0, max_gen_time, num_bins + 1)

    # Plot
    if scale:
        weights = np.array([np.ones_like(gt) / float(len(gt)) for gt in all_gen_times])
        plt.hist(all_gen_times, bins=bins, label=labels, color=colors, weights=weights)
    else:
        plt.hist(all_gen_times, bins=bins, label=labels, color=colors)
    plt.xticks(np.array(bins[:-1]) + bins[1] / 2)
    if log_y_scale:
        plt.yscale('log', nonposy='clip')
    plt.xlabel(xlabel)
    plt.ylabel("Fraction of requests")
    plt.legend(loc='upper right')
    if fig_name:
        plt.savefig(fig_name)
    if show:
        plt.show()


def plot_all_fidelities(all_fidelities, num_bins=5, scale=True, log_y_scale=True, show=True, fig_name=None):
    """
    Plots the fidelities of multiple simulations
    :param all_fidelities: A list of list containing the fidelities for different simulations
    :param num_bins: Number of bins to be used by the histogram
    :param scale: Whether to scale the bins to sum up to 1 or not
    :param log_y_scale: Whether to use log-scale for the y-axis
    :param show: Whether to show the plot or not
    :param fig_name: Name of figure to save, if None no figure is saved
    :return: None
    """
    plt.clf()

    # Compute bins
    bins = np.linspace(0, 1, num_bins + 1)

    # Plot
    if scale:
        weights = np.array([np.ones_like(f) / float(len(f)) for f in all_fidelities])
        plt.hist(all_fidelities, bins=bins, label=labels, color=colors, weights=weights)
    else:
        plt.hist(all_fidelities, bins=bins, label=labels, color=colors)
    plt.xticks(np.array(bins[:-1]) + bins[1] / 2)
    if log_y_scale:
        plt.yscale('log', nonposy='clip')
    plt.xlabel("Fidelity")
    plt.ylabel("Fraction of requests")
    plt.legend(loc='upper left')
    if fig_name:
        plt.savefig(fig_name)
    if show:
        plt.show()


def plot_all_throughputs(all_throughputs, avg_nr_points=None, scale_real_time=None, log_y_scale=True, xmax=None,
                         ymax=None, show=True, fig_name=None, xlabel="Matrix time (s)"):
    """
    Plots the throughputs of multiple simulations
    :param all_throughputs: A list of list containing the throughputs for different simulations
    :param avg_nr_points: List specifying how many data points to use for sweeping average for different simulations
    :param scale_real_time: None or float
        If the matrix time should be scaled.
    :param log_y_scale: Whether to use log-scale for the y-axis
    :param xmax: Max of x-axis
    :param ymax: Max of y-axis, can be useful to fit legend
    :param show: Whether to show the plot or not
    :param fig_name: Name of figure to save, if None no figure is saved
    :return: None
    """
    plt.clf()
    if isinstance(scale_real_time, float):
        scale_real_time = [scale_real_time] * len(all_throughputs)
    for i in range(len(all_throughputs)):
        (t, s) = all_throughputs[i]
        if scale_real_time:
            t = [time / scale_real_time[i] for time in t]
        try:
            nr_points = avg_nr_points[i]
        except Exception:
            nr_points = 1

        # Compute average
        s = [sum(s[max(0, i - nr_points):min(len(s) + 1, i + 1 + nr_points)]) / (2 * nr_points + 1) for i in
             range(len(s))]

        # Plot
        plt.plot(t, s, color=colors[i], label=labels[i])

    if log_y_scale:
        plt.yscale('log', nonposy='clip')
    if xmax:
        plt.xlim(xmin=0, xmax=xmax)
    if ymax:
        plt.ylim(ymax=ymax)
    plt.ylabel("Gen. entanglement per matrix second")
    plt.xlabel(xlabel)
    plt.legend(loc='upper right')
    if fig_name:
        plt.savefig(fig_name)
    if show:
        plt.show()


def plot_all_queue_lens(all_queue_lens_and_times, fig_name=None, show=True, xlabel='Matrix time (s)',
                        scale_real_time=None, xmax=None, avg_nr_points=None):
    plt.clf()
    if isinstance(scale_real_time, float):
        scale_real_time = [scale_real_time] * len(all_queue_lens_and_times)

    for i in range(len(all_queue_lens_and_times)):
        # for i in [0, 2]:
        try:
            nr_points = avg_nr_points[i]
        except Exception:
            nr_points = 1

        queue_lens, times = all_queue_lens_and_times[i]

        # Compute average
        queue_lens = [
            sum(queue_lens[max(0, i - nr_points):min(len(queue_lens) + 1, i + 1 + nr_points)]) / (2 * nr_points + 1) for
            i in
            range(len(queue_lens))]

        if scale_real_time:
            times = [time / scale_real_time[i] for time in times]
        plt.plot(times, queue_lens, color=colors[i], label=labels[i])

    if xmax:
        plt.xlim(xmin=0, xmax=xmax)
    plt.ylabel("Local Queue length at node A")
    plt.xlabel(xlabel)
    plt.legend(loc='upper left')
    if fig_name:
        plt.savefig(fig_name)
    if show:
        plt.show()


def main(scale_matrix_time=False):
    try:
        paths
    except NameError:
        raise RuntimeError("You must provide the paths to the data before running this file. See top of this file.")
    try:
        labels
    except NameError:
        raise RuntimeError(
            "You must provide the labels to be used in the plot before running this file. See top of this file.")
    try:
        colors
    except NameError:
        raise RuntimeError(
            "You must provide the colors to be used in the plot before running this file. See top of this file.")

    # Get data from simulation
    all_gen_times = []
    all_gen_list = []
    all_throughputs = []
    all_queue_lens_and_times = []
    for path in paths:
        # Read request data from sql
        (requests, rejected_requests), (gen_dct, gen_list), total_requested_pairs = parse_request_data_from_sql(path)

        # Get generation times
        gen_starts, gen_times = get_gen_latencies(requests, gen_dct)
        all_gen_times.append([t / 1e9 for t in gen_times])
        all_gen_list.append(gen_list)

        # Get throughputs
        all_throughputs.append(calc_throughput(gen_list))

        raw_queue_dataA = parse_table_data_from_sql(path, "EGP_Local_Queue_A")
        queue_lens, times, _, _, _ = parse_raw_queue_data(raw_queue_dataA)
        times = [t / 1e9 for t in times]
        all_queue_lens_and_times.append([queue_lens, times])

    # Plot data
    # Plot generation times
    if scale_matrix_time:
        scale_real_time = [12980, 12980, 168432.41955231575, 168432.41955231575]
        scale_real_time = [t * 1e-6 for t in scale_real_time]
        plot_all_generation_times(all_gen_times, xlabel="Latency (x 1e3 MHP cycles)", fig_name="latencies_scaled.png",
                                  scale_real_time=scale_real_time)
    else:
        plot_all_generation_times(all_gen_times, fig_name="latencies.png")

    # Plot throughputs
    if scale_matrix_time:
        scale_real_time = [12980, 12980, 168432.41955231575, 168432.41955231575]
        scale_real_time = [t * 1e-3 for t in scale_real_time]
        plot_all_throughputs(all_throughputs, avg_nr_points=[10, 10, 10, 10], xmax=6.5, ymax=1e2,
                             xlabel="x 1e6 MHP cycles", fig_name='throughputs_scaled.png',
                             scale_real_time=scale_real_time)
    else:
        plot_all_throughputs(all_throughputs, avg_nr_points=[10, 10, 10, 10], xmax=90, ymax=1e2,
                             fig_name='throughputs.png')

    # Plot queue lens
    if scale_matrix_time:
        scale_real_time = [12980, 12980, 168432.41955231575, 168432.41955231575]
        scale_real_time = [t * 1e-3 for t in scale_real_time]
        plot_all_queue_lens(all_queue_lens_and_times, avg_nr_points=[10] * 4, xmax=6.5,
                            fig_name='queue_lens_all_scaled_sweeping_avg.png', xlabel="x 1e6 MHP cycles",
                            scale_real_time=scale_real_time)
    else:
        plot_all_queue_lens(all_queue_lens_and_times, avg_nr_points=[10] * 4, xmax=90,
                            fig_name='queue_lens_all_sweeping_avg.png')


if __name__ == '__main__':
    main(scale_matrix_time=False)
    main(scale_matrix_time=True)
