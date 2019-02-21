import os
import numpy as np
from argparse import ArgumentParser
from matplotlib import pyplot as plt
from simulations.generate_metrics_file import get_metrics_from_single_file


def collect_req_freq_data(results_folders):
    req_freqs = [0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]
    req_types = ["MD", "NL"]
    data = {req_type: {req_freq: {"throughputs": [], "latencies": []} for req_freq in req_freqs} for req_type in
            req_types}

    for results_folder in results_folders:
        for entry in os.listdir(results_folder):
            if entry.endswith(".db"):
                scenario_key = entry.split("_key_")[1].split("_run_")[0]
                req_type = scenario_key.split("QL2020_")[1].split("_req_frac_")[0]
                req_freq = scenario_key.split("_req_frac_")[1].split("_FCFS_")[0]
                req_freq = float("{}.{}{}".format(*req_freq))

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
            std_th = np.std(data[req_type][req_freq]["throughputs"]) / np.sqrt(
                len(data[req_type][req_freq]["throughputs"]))
            std_l = np.std(data[req_type][req_freq]["latencies"]) / np.sqrt(len(data[req_type][req_freq]["latencies"]))
            avgs[req_type][req_freq] = (avg_th, avg_l)
            stds[req_type][req_freq] = (std_th, std_l)

    return avgs, stds


def collect_fidelity_sweep_data(results_folders):
    min_fids = [0.6, 0.65, 0.7, 0.75, 0.8, 0.85]
    req_types = ["MD", "NL"]
    data = {req_type: {min_fid: {"throughputs": [], "latencies": [], "fidelities": []} for min_fid in min_fids} for
            req_type in req_types}

    for results_folder in results_folders:
        for entry in os.listdir(results_folder):
            if entry.endswith(".db"):
                scenario_key = entry.split("_key_")[1].split("_run_")[0]
                req_type = scenario_key.split("QL2020_")[1].split("_req_frac_")[0]
                min_fid = scenario_key.split("_min_fid_")[1].split("_FCFS_")[0]
                min_fid = float("{}.{}{}".format(*min_fid))

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
            std_th = np.std(data[req_type][min_fid]["throughputs"]) / np.sqrt(
                len(data[req_type][min_fid]["throughputs"]))
            std_l = np.std(data[req_type][min_fid]["latencies"]) / np.sqrt(len(data[req_type][min_fid]["latencies"]))
            std_f = np.std(data[req_type][min_fid]["fidelities"]) / np.sqrt(len(data[req_type][min_fid]["fidelities"]))
            avgs[req_type][min_fid] = (avg_th, avg_l, avg_f)
            stds[req_type][min_fid] = (std_th, std_l, std_f)

    return avgs, stds


def plot_l_vs_r(avgs, stds):
    markers = {"MD": '<', "NL": '>'}
    colors = {"MD": 'C2', "NL": 'C0'}
    linestyles = {"MD": ':', "NL": '-'}
    for req_type in avgs:
        req_freqs = sorted(avgs[req_type].keys())
        latencies = [avgs[req_type][r][1] for r in req_freqs]
        lat_errors = [stds[req_type][r][1] for r in req_freqs]
        plt.errorbar(req_freqs, latencies, yerr=lat_errors, label=req_type, linestyle=linestyles[req_type],
                     marker=markers[req_type], color=colors[req_type])

    ax = plt.gca().axes
    plt.text(0.92, 0.98, "(a)", horizontalalignment='right', verticalalignment='top', transform=ax.transAxes)
    plt.xlabel(r"$f$")
    plt.ylabel("Scaled Latency (s)")
    plt.xlim(0.5, 1)
    plt.ylim(0, 50)
    plt.xticks([0.5, 0.7, 0.9])
    plt.gca().axes.set_aspect(1 / 50)
    handles, labels = ax.get_legend_handles_labels()
    handles = [h[0] for h in handles]
    ax.legend(handles, labels, loc='upper left', numpoints=1)


def plot_l_vs_f(avgs, stds):
    markers = {"MD": '<', "NL": '>'}
    colors = {"MD": 'C2', "NL": 'C0'}
    linestyles = {"MD": ':', "NL": '-'}
    for req_type in avgs:
        min_fids = sorted(avgs[req_type].keys())
        fids = [avgs[req_type][r][2] for r in min_fids]
        latencies = [avgs[req_type][r][1] for r in min_fids]
        lat_errors = [stds[req_type][r][1] for r in min_fids]
        plt.errorbar(fids, latencies, yerr=lat_errors, label=req_type, linestyle=linestyles[req_type],
                     marker=markers[req_type], color=colors[req_type])
    ax = plt.gca().axes
    plt.text(0.92, 0.98, "(b)", horizontalalignment='right', verticalalignment='top', transform=ax.transAxes)
    plt.xlabel(r"$F$")
    plt.xlim(0.5, 0.75)
    plt.ylim(0, 50)
    plt.xticks([0.5, 0.6, 0.7])
    plt.gca().axes.set_yticklabels([])
    plt.gca().axes.set_aspect(1 / 100)


def plot_th_vs_f(avgs, stds):
    markers = {"MD": '<', "NL": '>'}
    colors = {"MD": 'C2', "NL": 'C0'}
    linestyles = {"MD": ':', "NL": '-'}
    for req_type in avgs:
        min_fids = sorted(avgs[req_type].keys())
        fids = [avgs[req_type][r][2] for r in min_fids]
        throughputs = [avgs[req_type][r][0] for r in min_fids]
        th_errors = [stds[req_type][r][0] for r in min_fids]
        plt.errorbar(fids, throughputs, yerr=th_errors, label=req_type, linestyle=linestyles[req_type],
                     marker=markers[req_type], color=colors[req_type])
    ax = plt.gca().axes
    plt.text(0.92, 0.98, "(c)", horizontalalignment='right', verticalalignment='top', transform=ax.transAxes)
    plt.xlabel(r"$F$")
    plt.ylabel("Throughput (1/s)")
    plt.xlim(0.5, 0.75)
    plt.ylim(0, 30)
    plt.xticks([0.5, 0.6, 0.7])
    plt.gca().axes.set_aspect(1 / 100 * (50 / 30))
    plt.gca().axes.yaxis.set_label_position('right')
    plt.gca().axes.yaxis.tick_right()


def main(req_freq_folders, fid_folders, plot_dir=None):
    # req_freq_avgs = {'MD': {0.5: (3.9931449999999997, 0.25782895487368096), 0.6: (4.7407825, 0.33077578812435066), 0.7: (5.6066175000000005, 0.43615300051442407), 0.8: (6.4377200000000006, 0.6531213258262272), 0.9: (7.198808999999999, 1.2462488420374107), 0.95: (7.474039, 1.8387564473488374), 0.99: (7.663329999999999, 2.6222469882919732)}, 'NL': {0.5: (0.2460075, 4.541182993426091), 0.6: (0.30013750000000006, 6.2725975566663985), 0.7: (0.34352499999999997, 8.622585303479793), 0.8: (0.3902425, 11.015356276727122), 0.9: (0.43439900000000015, 27.742306537342856), 0.95: (0.452216, 38.049510038334375), 0.99: (0.45798099999999997, 48.48601391271241)}}
    #
    # req_freq_stds = {'MD': {0.5: (0.035590683814377605, 0.005096375107430715), 0.6: (0.043356839986255336, 0.008599263709691723), 0.7: (0.03561013828383357, 0.015279495888998388), 0.8: (0.05437457979147977, 0.02919797681214094), 0.9: (0.026455718289058038, 0.059628541193311645), 0.95: (0.024379343547150736, 0.11613827446347864), 0.99: (0.0253186016793977, 0.16860707554767723)}, 'NL': {0.5: (0.0021906231633373185, 0.09427114826711887), 0.6: (0.0029659667216187704, 0.25460423300060875), 0.7: (0.0031881749285445423, 0.3908847232128248), 0.8: (0.0030872011748102847, 0.5519742333933813), 0.9: (0.001536114250308225, 1.7755627241814658), 0.95: (0.0015915198522167418, 2.051008039073476), 0.99: (0.0017081011064922359, 2.4502209320413595)}}
    #
    # fid_avgs = {'MD': {0.6: (28.435666666666666, 1.0817326291554814, 0.5286876716873077), 0.65: (24.574469999999998, 1.1922818287705172, 0.5724316817510895), 0.7: (20.35789333333333, 1.2721772335039405, 0.615895149986874), 0.75: (16.425116666666668, 1.5603601495996067, 0.6576750682964764), 0.8: (12.006316666666667, 2.18668414906893, 0.7002511482280234), 0.85: (7.727343333333333, 2.309625860013186, 0.7425719084550454)}, 'NL': {0.6: (1.6954033333333334, 26.80794481008729, 0.4776398578329559), 0.65: (1.43187, 28.408921869241556, 0.5100745345226706), 0.7: (1.2121600000000003, 27.09119088344602, 0.5454893083721156), 0.75: (0.9665166666666667, 31.334045652950945, 0.5774599374828749), 0.8: (0.7154866666666665, 30.839186672576734, 0.6116293051556442), 0.85: (0.45882666666666666, 36.02367245778542, 0.6448073325740481)}}
    #
    # fid_stds = {'MD': {0.6: (0.10635712475046125, 0.10070701582597893, 0.002124630505133395), 0.65: (0.11969994069895501, 0.12191763084601417, 0.0027947904657511097), 0.7: (0.0782161286199739, 0.14900936671511075, 0.003128750676000945), 0.75: (0.0813159052479667, 0.1309290171663182, 0.003431006567986908), 0.8: (0.05330414536377974, 0.19525204461793885, 0.0032900102862294224), 0.85: (0.047926026722516946, 0.23233877478754417, 0.0030275607623677986)}, 'NL': {0.6: (0.0069399255252701585, 2.2619979213847436, 4.017587574817841e-05), 0.65: (0.028273061049541685, 3.1177530897780854, 7.03383402700622e-05), 0.7: (0.0064747365454768365, 2.735333024039138, 0.00011234466661585413), 0.75: (0.0055727174367295575, 2.467993562775798, 0.0001635152829133578), 0.8: (0.0040441527977874234, 3.143397016300691, 0.00023900618164171295), 0.85: (0.0033288667852700376, 2.5897547220694195, 0.00046166362566563076)}}

    req_freq_avgs, req_freq_stds = collect_req_freq_data(req_freq_folders)

    fid_avgs, fid_stds = collect_fidelity_sweep_data(fid_folders)

    plt.rcParams.update({'font.size': 12})
    plt.subplot(1, 3, 1)
    plot_l_vs_r(req_freq_avgs, req_freq_stds)
    plt.subplot(1, 3, 2)

    plot_l_vs_f(fid_avgs, fid_stds)

    plt.subplot(1, 3, 3)
    plot_th_vs_f(fid_avgs, fid_stds)
    plt.subplots_adjust(wspace=0.05)

    if plot_dir is not None:
        plt.savefig(os.path.join(plot_dir, "combined_fid_req_freq.png"))
    plt.show()


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--req_freq_folders', required=True, type=str,
                        help="Paths to directories containing data for simulation of request frequency sweep."
                             "Separated by ','"
                             "(Should be */2019-01-24T19:59:34CET_req_freq_sweep,"
                             "             2019-01-26T15:59:44CET_req_freq_sweep,"
                             "             2019-01-26T16:23:25CET_req_freq_sweep)")
    parser.add_argument('--fid_sweep_folders', required=True, type=str,
                        help="Paths to directories containing data for simulation of fidelity sweep."
                             "Separated by ','"
                             "(Should be */2019-01-28T11:32:51CET_req_freq_sweep)")
    parser.add_argument('--plot_dir', required=False, type=str, default=None,
                        help="Path to directory where the plots should be saved."
                             "If not used the plots are simply shown and not saved.")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    req_freq_folders = args.req_freq_folders.split(',')
    fid_sweep_folders = args.fid_sweep_folders.split(',')
    main(req_freq_folders, fid_sweep_folders, args.plot_dir)
