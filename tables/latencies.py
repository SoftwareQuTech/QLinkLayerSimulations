import os
import csv
import numpy as np


phys_setup_2_latex = {
    "LAB_NC_NC": r"\Lab",
    "QLINK_WC_WC": r"\Qlink"
}
mix_2_latex = {
    "uniform": r"\textproc{Uniform}",
    "moreNL": r"\textproc{MoreNL}",
    "moreCK": r"\textproc{MoreCK}",
    "moreMD": r"\textproc{MoreMD}",
    "noNLmoreCK": r"\textproc{NoNLMoreCK}",
    "noNLmoreMD": r"\textproc{NoNLMoreMD}"
}
scheduler_2_latex = {
    "FIFO": r"\textproc{FCFS}",
    "lowerWFQ": r"\textproc{LowerWFQ}",
    "higherWFQ": r"\textproc{HigherWFQ}"
}

def main(results_folder):
    latex_begin = r"""
        \begin{tabular}{|l|cccccc|}
            \hline
            Scenario & SL\_\textit{NL} (s) & SL\_\textit{CK} (s) & SL\_\textit{MD} (s) & RL\_\textit{NL} (s) & RL\_\textit{CK} (s) & RL\_\textit{MD} (s) \\ \hline
"""

    latex_end = r"""
       \end{tabular}"""

    latex_middle = ""

    # throughput_file = os.path.join(results_folder, "metrics/Throughput.csv")
    latencies_per_row = {}
    for lat_file in [os.path.join(results_folder, "metrics/{}.csv".format(laten_type)) for laten_type in ["ScaledReqLatency", "ReqLatency"]]:
        with open(lat_file, 'rt', encoding='utf8') as f:
            reader = csv.reader(f)
            for r, row in enumerate(reader):
                if r > 0:
                    scenario_name = row[0]
                    if "mix" in scenario_name:
                        latencies = row[1:4]
                        lat_stds = row[7:10]
                        lat_num = row[13:16]
                        std_error = [" ({0:.2f})".format(float(s) / np.sqrt(float(n))) if s != '' else '' for s, n in zip(lat_stds, lat_num)]

                        lat_and_std = ["{0:.2f}{1}".format(float(l), s) if l != '' else '-' for l, s in zip(latencies, std_error)]

                        if not "lowerWFQ" in scenario_name:
                            # print("{}_{}_{} with {}".format(phys_setup, mix, scheduler, throughputs))
                            if scenario_name not in latencies_per_row:
                                latencies_per_row[scenario_name] = []
                            latencies_per_row[scenario_name] += lat_and_std

    for scenario_name, lat_and_std in latencies_per_row.items():
        phys_setup, rest = scenario_name.split("_mix_")
        mix, scheduler = rest.split("_weights_")
        row_name = "\t" * 3 + r"{}\_{}\_{}".format(phys_setup_2_latex[phys_setup], mix_2_latex[mix], scheduler_2_latex[scheduler])
        # latex_middle += row_name + r" & {} & {} & {} & {} & {} & {} & {} & {} & {} \\ \hline".format(*latencies) + "\n"
        latex_middle += row_name + "".join([" & {}".format(l) for l in lat_and_std]) + r" \\ \hline" + "\n"
        # print(latencies)
        # laten_latex = []
        # for l in latencies:
        #     try:
        #         l_float = float(l)
        #     except ValueError:
        #         laten_latex.append("-")
        #     else:
        #         laten_latex.append("{0:.3f}".format(l_float))
        # latex_middle += row_name + "".join([" & {}".format(l) for l in laten_latex]) + r" \\ \hline" "\n"



    latex_code = latex_begin + latex_middle[:-1] + latex_end
    table_name = "latency_SL_RL"
    with open("/Volumes/Untitled/Dropbox/my_linklayer/tables/{}.tex".format(table_name), 'w') as f:
        f.write(latex_code)
    print(latex_code)


if __name__ == '__main__':
    run = 1

    if run == 1:
        results_folder = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure"
    elif run == 2:
        results_folder = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-15T23:56:55CET_CREATE_and_measure"
    else:
        ValueError("Unkown run = {}".format(run))

    main(results_folder)
