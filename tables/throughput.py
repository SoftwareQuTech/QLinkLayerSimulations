import os
import csv


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
        \begin{tabular}{|l|ccc|}
            \hline
            Scenario & Throughput\_\textit{NL} (1/s) & Throughput\_\textit{CK} (1/s) & Throughput\_\textit{MD} (1/s) \\ \hline
"""

    latex_end = r"""
       \end{tabular}"""

    latex_middle = ""

    throughput_file = os.path.join(results_folder, "metrics/Throughput.csv")
    with open(throughput_file, 'rt', encoding='utf8') as f:
        reader = csv.reader(f)
        for r, row in enumerate(reader):
            if r > 0:
                scenario_name = row[0]
                if "mix" in scenario_name:
                    throughputs = row[1:4]
                    phys_setup, rest = scenario_name.split("_mix_")
                    mix, scheduler = rest.split("_weights_")
                    if scheduler != "lowerWFQ":
                        # print("{}_{}_{} with {}".format(phys_setup, mix, scheduler, throughputs))
                        row_name = "\t" * 3 + r"{}\_{}\_{}".format(phys_setup_2_latex[phys_setup], mix_2_latex[mix], scheduler_2_latex[scheduler])
                        th_latex = []
                        for th in throughputs:
                            try:
                                th_float = float(th)
                            except ValueError:
                                th_latex.append("-")
                            else:
                                if th_float == 0.0:
                                    th_latex.append("-")
                                else:
                                    th_latex.append("{0:.3f}".format(th_float))
                        latex_middle += row_name + "".join([" & {}".format(th) for th in th_latex]) + r" \\ \hline" + "\n"


    latex_code = latex_begin + latex_middle[:-1] + latex_end
    table_name = "throughput"
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
