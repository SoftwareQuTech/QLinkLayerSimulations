import sys
import os
import numpy as np
from collections import defaultdict
from matplotlib import pyplot as plt
import random


from simulations.request_freq_sweep.generate_metrics_file import get_raw_metric_data, get_metrics_from_single_file


def collect_data(results_folder):
    min_fids = [0.6, 0.65, 0.7, 0.75, 0.8, 0.85]
    req_types = ["MD", "NL"]
    data = {req_type: {min_fid: {"throughputs": [], "latencies": [], "fidelities": []} for min_fid in min_fids} for req_type in req_types}

    for entry in os.listdir(results_folder):
        if entry.endswith(".db"):
            scenario_key = entry.split("_key_")[1].split("_run_")[0]
            req_type = scenario_key.split("QL2020_")[1].split("_req_frac_")[0]
            min_fid = scenario_key.split("_min_fid_")[1].split("_FCFS_")[0]
            min_fid = float("{}.{}{}".format(*min_fid))
            run_index = entry.split("_run_")[1].split(".")[0]
            run_ID = scenario_key + run_index
            print(run_ID)

            metrics = get_metrics_from_single_file(os.path.join(results_folder, entry))
            avg_throughput = metrics["AvgThroughp_Prio{} (1/s)".format(req_type)]
            avg_scaled_laten = metrics["AvgScaledReqLaten_Prio{}_NodeID0 (s)".format(req_type)]
            avg_fid = metrics["AvgFid_Prio{}".format(req_type)]

            data[req_type][min_fid]["throughputs"].append(avg_throughput)
            data[req_type][min_fid]["latencies"].append(avg_scaled_laten)
            data[req_type][min_fid]["fidelities"].append(avg_fid)

    avgs = {}
    stds = {}
    for req_type in req_types:
        avgs[req_type] = {}
        stds[req_type] = {}
        for min_fid in min_fids:
            avg_th = np.mean(data[req_type][min_fid]["throughputs"])
            avg_l = np.mean(data[req_type][min_fid]["latencies"])
            avg_f = np.mean(data[req_type][min_fid]["fidelities"])
            std_th = np.std(data[req_type][min_fid]["throughputs"]) / np.sqrt(len(data[req_type][min_fid]["throughputs"]))
            std_l = np.std(data[req_type][min_fid]["latencies"]) / np.sqrt(len(data[req_type][min_fid]["latencies"]))
            std_f = np.std(data[req_type][min_fid]["fidelities"]) / np.sqrt(len(data[req_type][min_fid]["fidelities"]))
            avgs[req_type][min_fid] = (avg_th, avg_l, avg_f)
            stds[req_type][min_fid] = (std_th, std_l, std_f)

    return avgs, stds


def plot_l_vs_f(avgs, stds):
    plt.rcParams.update({'font.size': 12})
    plt.clf()
    markers = {"MD": '<', "NL": '>'}
    colors = {"MD": 'C2', "NL": 'C0'}
    linestyles = {"MD": ':', "NL": '-'}
    for req_type in avgs:
        min_fids = sorted(avgs[req_type].keys())
        latencies = [avgs[req_type][r][1] for r in min_fids]
        lat_errors = [stds[req_type][r][1] for r in min_fids]
        plt.errorbar(min_fids, latencies, yerr=lat_errors, label=req_type, linestyle=linestyles[req_type],
                     marker=markers[req_type], color=colors[req_type])
    plt.xlabel("F_min")
    plt.ylabel("Scaled Latency (s)")
    plt.legend(loc='upper left')
    plt.gca().axes.set_aspect(1/160)
    # plt.savefig("/Volumes/Untitled/Dropbox/linklayer/Sigcomm/V3/plots/latency_vs_fidelity.png", bbox_inches='tight')
    plt.show()

def plot_th_vs_f(avgs, stds):
    plt.rcParams.update({'font.size': 12})
    plt.clf()
    markers = {"MD": '<', "NL": '>'}
    colors = {"MD": 'C2', "NL": 'C0'}
    linestyles = {"MD": ':', "NL": '-'}
    for req_type in avgs:
        min_fids = sorted(avgs[req_type].keys())
        throughputs = [avgs[req_type][r][0] for r in min_fids]
        th_errors = [stds[req_type][r][0] for r in min_fids]
        plt.errorbar(min_fids, throughputs, yerr=th_errors, label=req_type, linestyle=linestyles[req_type],
                     marker=markers[req_type], color=colors[req_type])
    plt.xlabel(r"$F_\mathrm{min}$")
    plt.ylabel("Throughput (1/s)")
    plt.legend(loc='upper right')
    plt.gca().axes.set_aspect(1/50)
    plt.savefig("/Volumes/Untitled/Dropbox/linklayer/Sigcomm/V3/plots/throughput_vs_fidelity.png", bbox_inches='tight')
    plt.show()

# def plot_th_vs_l(avgs, stds):
#     plt.clf()
#     markers = {"MD": '<', "NL": '>'}
#     colors = {"MD": 'C2', "NL": 'C0'}
#     linestyles = {"MD": ':', "NL": '-'}
#     for req_type in avgs:
#         req_freqs = sorted(avgs[req_type].keys())
#         latencies = [avgs[req_type][r][1] for r in req_freqs]
#         lat_errors = [stds[req_type][r][1] for r in req_freqs]
#         throughputs = [avgs[req_type][r][0] for r in req_freqs]
#         th_errors = [stds[req_type][r][0] for r in req_freqs]
#         plt.errorbar(latencies, throughputs, xerr=lat_errors, yerr=th_errors, label=req_type,
#                      linestyle=linestyles[req_type], marker=markers[req_type], color=colors[req_type])
#     plt.xlabel("Scaled Latency (s)")
#     plt.ylabel("Throughput (1/s)")
#     plt.legend(loc='upper right')
#     # plt.savefig("/Volumes/Untitled/Dropbox/linklayer/Sigcomm/V3/plots/throughput_vs_latency.png", bbox_inches='tight')
#     plt.show()


def main(results_folder):
    avgs, stds = collect_data(results_folder)

    # avgs = {'MD': {0.6: (28.435666666666666, 1.0817326291554814), 0.65: (24.574469999999998, 1.1922818287705172), 0.7: (20.35789333333333, 1.2721772335039405), 0.75: (16.425116666666668, 1.5603601495996067), 0.8: (12.006316666666667, 2.18668414906893), 0.85: (7.727343333333333, 2.309625860013186)}, 'NL': {0.6: (1.6954033333333334, 26.80794481008729), 0.65: (1.43187, 28.408921869241556), 0.7: (1.2121600000000003, 27.09119088344602), 0.75: (0.9665166666666667, 31.334045652950945), 0.8: (0.7154866666666665, 30.839186672576734), 0.85: (0.45882666666666666, 36.02367245778542)}}
    #
    # stds = {'MD': {0.6: (0.10635712475046125, 0.10070701582597893), 0.65: (0.11969994069895501, 0.12191763084601417), 0.7: (0.0782161286199739, 0.14900936671511075), 0.75: (0.0813159052479667, 0.1309290171663182), 0.8: (0.05330414536377974, 0.19525204461793885), 0.85: (0.047926026722516946, 0.23233877478754417)}, 'NL': {0.6: (0.0069399255252701585, 2.2619979213847436), 0.65: (0.028273061049541685, 3.1177530897780854), 0.7: (0.0064747365454768365, 2.735333024039138), 0.75: (0.0055727174367295575, 2.467993562775798), 0.8: (0.0040441527977874234, 3.143397016300691), 0.85: (0.0033288667852700376, 2.5897547220694195)}}

    # plot_l_vs_f(avgs, stds)
    # plot_th_vs_f(avgs, stds)
    # plot_th_vs_f(avgs, stds)

    print(avgs)
    print(stds)


if __name__ == '__main__':
    # results_folder = sys.argv[1]
    results_folder = "/Users/adahlberg/Documents/QLinkLayer/simulations/fidelity_sweep/2019-01-28T11:32:51CET_req_freq_sweep"
    main(results_folder)
