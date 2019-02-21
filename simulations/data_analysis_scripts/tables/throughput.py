import os
import csv
from argparse import ArgumentParser

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


def main(results_folder, tex_path=None):
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
                        row_name = "\t" * 3 + r"{}\_{}\_{}".format(phys_setup_2_latex[phys_setup], mix_2_latex[mix],
                                                                   scheduler_2_latex[scheduler])
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
                        latex_middle += row_name + "".join(
                            [" & {}".format(th) for th in th_latex]) + r" \\ \hline" + "\n"

    latex_code = latex_begin + latex_middle[:-1] + latex_end
    print(latex_code)
    if tex_path is not None:
        table_name = "throughput"
        with open(os.path.join(tex_path, "{}.tex".format(table_name)), 'w') as f:
            f.write(latex_code)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--results_path', required=True, type=str,
                        help="Path to directory containing data for simulation run 1."
                             "(Should be */2019-01-16T11:10:28CET_CREATE_and_measure"
                             "or 2019-01-15T23:56:55CET_CREATE_and_measure)")
    parser.add_argument('--tex_path', required=False, type=str, default=None,
                        help="Path to folder where the .tex file containing the data should be saved."
                             "If not used, the tables are simply printed and not saved.")

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args.results_path, args.tex_path)
