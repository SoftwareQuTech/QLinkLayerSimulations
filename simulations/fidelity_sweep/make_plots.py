import sys
import os
import numpy as np
from collections import defaultdict
from matplotlib import pyplot as plt
import random


from simulations.request_freq_sweep.generate_metrics_file import get_raw_metric_data, get_metrics_from_single_file


def collect_data(results_folder):
    req_freqs = [0.5, 0.6, 0.7, 0.8, 0.99]
    req_types = ["MD", "NL"]
    data = {req_type: {req_freq: {"throughputs": [], "latencies": []} for req_freq in req_freqs} for req_type in req_types}

    for entry in os.listdir(results_folder):
        if entry.endswith(".db"):
            scenario_key = entry.split("_key_")[1].split("_run_")[0]
            req_type = scenario_key.split("QL2020_")[1].split("_req_frac_")[0]
            req_freq = scenario_key.split("_req_frac_")[1].split("_FCFS_")[0]
            req_freq = float("{}.{}{}".format(*req_freq))
            run_index = entry.split("_run_")[1].split(".")[0]
            run_ID = scenario_key + run_index
            print(run_ID)

            metrics = get_metrics_from_single_file(os.path.join(results_folder, entry))
            avg_throughput = metrics["AvgThroughp_Prio{} (1/s)".format(req_type)]
            avg_scaled_laten = metrics["AvgScaledReqLaten_Prio{}_NodeID0 (s)".format(req_type)]

            data[req_type][req_freq]["throughputs"].append(avg_throughput)
            data[req_type][req_freq]["latencies"].append(avg_scaled_laten)

    avgs = {}
    stds = {}
    for req_type in req_types:
        avgs[req_type] = {}
        stds[req_type] = {}
        for req_freq in req_freqs:
            avg_th = np.mean(data[req_type][req_freq]["throughputs"])
            avg_l = np.mean(data[req_type][req_freq]["latencies"])
            std_th = np.std(data[req_type][req_freq]["throughputs"]) / np.sqrt(len(data[req_type][req_freq]["throughputs"]))
            std_l = np.std(data[req_type][req_freq]["latencies"]) / np.sqrt(len(data[req_type][req_freq]["latencies"]))
            avgs[req_type][req_freq] = (avg_th, avg_l)
            stds[req_type][req_freq] = (std_th, std_l)

    return avgs, stds


def plot_l_vs_r(avgs, stds):
    plt.rcParams.update({'font.size': 12})
    plt.clf()
    markers = {"MD": '<', "NL": '>'}
    colors = {"MD": 'C2', "NL": 'C0'}
    linestyles = {"MD": ':', "NL": '-'}
    for req_type in avgs:
        req_freqs = sorted(avgs[req_type].keys())
        latencies = [avgs[req_type][r][1] for r in req_freqs]
        lat_errors = [stds[req_type][r][1] for r in req_freqs]
        plt.errorbar(req_freqs, latencies, yerr=lat_errors, label=req_type, linestyle=linestyles[req_type],
                     marker=markers[req_type], color=colors[req_type])
    plt.xlabel("Request frequency / max throughput")
    plt.ylabel("Scaled Latency (s)")
    plt.legend(loc='upper left')
    plt.gca().axes.set_aspect(1/160)
    plt.savefig("/Volumes/Untitled/Dropbox/linklayer/Sigcomm/V3/plots/latency_vs_req_freq.png", bbox_inches='tight')
    plt.show()

def plot_th_vs_r(avgs, stds):
    plt.clf()
    markers = {"MD": '<', "NL": '>'}
    colors = {"MD": 'C2', "NL": 'C0'}
    linestyles = {"MD": ':', "NL": '-'}
    for req_type in avgs:
        req_freqs = sorted(avgs[req_type].keys())
        throughputs = [avgs[req_type][r][0] for r in req_freqs]
        th_errors = [stds[req_type][r][0] for r in req_freqs]
        plt.errorbar(req_freqs, throughputs, yerr=th_errors, label=req_type, linestyle=linestyles[req_type],
                     marker=markers[req_type], color=colors[req_type])
    plt.xlabel("Request frequency / max throughput")
    plt.ylabel("Throughput (1/s)")
    plt.legend(loc='upper left')
    # plt.savefig("/Volumes/Untitled/Dropbox/linklayer/Sigcomm/V3/plots/throughput_vs_req_freq.png", bbox_inches='tight')
    plt.show()

def plot_th_vs_l(avgs, stds):
    plt.clf()
    markers = {"MD": '<', "NL": '>'}
    colors = {"MD": 'C2', "NL": 'C0'}
    linestyles = {"MD": ':', "NL": '-'}
    for req_type in avgs:
        req_freqs = sorted(avgs[req_type].keys())
        latencies = [avgs[req_type][r][1] for r in req_freqs]
        lat_errors = [stds[req_type][r][1] for r in req_freqs]
        throughputs = [avgs[req_type][r][0] for r in req_freqs]
        th_errors = [stds[req_type][r][0] for r in req_freqs]
        plt.errorbar(latencies, throughputs, xerr=lat_errors, yerr=th_errors, label=req_type,
                     linestyle=linestyles[req_type], marker=markers[req_type], color=colors[req_type])
    plt.xlabel("Scaled Latency (s)")
    plt.ylabel("Throughput (1/s)")
    plt.legend(loc='upper right')
    # plt.savefig("/Volumes/Untitled/Dropbox/linklayer/Sigcomm/V3/plots/throughput_vs_latency.png", bbox_inches='tight')
    plt.show()


def main(results_folder):
    # avgs, stds = collect_data(results_folder)


    # for num in [10**i for i in range(4)]:
    #     rands = []
    #     for _ in range(num):
    #         rands.append(random.randint(0, 40))
    #
    #     print("{} {}".format(num, np.std(rands)))
    # exit()

    # avgs = {'MD': {0.5: (3.9931449999999997, 0.25782895487368096), 0.6: (4.7407825, 0.33077578812435066), 0.7: (5.6066175000000005, 0.43615300051442407), 0.8: (6.4377200000000006, 0.6531213258262272), 0.99: (7.626592500000001, 2.2816744173747203)}, 'NL': {0.5: (0.2460075, 4.541182993426091), 0.6: (0.30013750000000006, 6.2725975566663985), 0.7: (0.34352499999999997, 8.622585303479793), 0.8: (0.3902425, 11.015356276727122), 0.99: (0.45696249999999994, 42.049877685504704)}}
    # stds = {'MD': {0.5: (0.22509524867264527, 0.03223230630013275), 0.6: (0.2742127330080607, 0.054386519046109603), 0.7: (0.2252182895409473, 0.09663601701682839), 0.8: (0.343895037911279, 0.1846642195902953), 0.99: (0.2743415693870508, 1.236620440761773)}, 'NL': {0.5: (0.01385471738253798, 0.596223092327062), 0.6: (0.01875842060915577, 1.6102585564042597), 0.7: (0.020163788706490653, 2.4721720558340325), 0.8: (0.01952517461509628, 3.4909915744969133), 0.99: (0.020945819242751042, 20.680121351410193)}}

    avgs = {'MD': {0.5: (3.9931449999999997, 0.25782895487368096), 0.6: (4.7407825, 0.33077578812435066), 0.7: (5.6066175000000005, 0.43615300051442407), 0.8: (6.4377200000000006, 0.6531213258262272), 0.99: (7.626592500000001, 2.2816744173747203)}, 'NL': {0.5: (0.2460075, 4.541182993426091), 0.6: (0.30013750000000006, 6.2725975566663985), 0.7: (0.34352499999999997, 8.622585303479793), 0.8: (0.3902425, 11.015356276727122), 0.99: (0.45696249999999994, 42.049877685504704)}}
    stds = {'MD': {0.5: (0.035590683814377605, 0.005096375107430715), 0.6: (0.043356839986255336, 0.008599263709691723), 0.7: (0.03561013828383357, 0.015279495888998388), 0.8: (0.05437457979147977, 0.02919797681214094), 0.99: (0.0433772108064102, 0.19552685969642644)}, 'NL': {0.5: (0.0021906231633373185, 0.09427114826711887), 0.6: (0.0029659667216187704, 0.25460423300060875), 0.7: (0.0031881749285445423, 0.3908847232128248), 0.8: (0.0030872011748102847, 0.5519742333933813), 0.99: (0.0033118248132638288, 3.269814287956778)}}

    plot_l_vs_r(avgs, stds)
    # plot_th_vs_r(avgs, stds)
    # plot_th_vs_l(avgs, stds)

    print(avgs)
    print(stds)


if __name__ == '__main__':
    # results_folder = sys.argv[1]
    results_folder = "/Users/adahlberg/Documents/QLinkLayer/simulations/request_freq_sweep/2019-01-24T19:59:34CET_req_freq_sweep"
    main(results_folder)
