import numpy as np
import os

from simulations.major_simulation.generate_metrics_file import get_raw_metric_data, get_metrics_from_single_file
from easysquid.toolbox import logger

def main(c_loss_folder, other_basename):

    prio_names = ["NL", "CK", "MD"]

    c_loss_metrics = {p_loss: {} for p_loss in range(4, 11)}
    diffs = {p_loss: {m: {} for m in ["F", "T", "L", "O"]} for p_loss in range(4, 11)}

    counter = 0
    for entry in sorted(os.listdir(c_loss_folder)):
        if entry.endswith(".db"):
            scenario_key = entry.split("_key_")[1].split("_run_")[0]
            if "HIGH_C_LOSS" in scenario_key:
                if "_mix_" in scenario_key:
                    p_loss = int(scenario_key.split("_HIGH_C_LOSS_")[1].split("_mix_")[0][3:])
                    prio = "mix"
                    phys_setup, rest = scenario_key.split("_mix_")
                    mix, scheduler = rest.split("_weights_")
                    prios_in_file = prio_names
                    metric_key = (phys_setup, mix, scheduler)
                else:
                    counter += 1
                    for p in prio_names:
                        if p in scenario_key:
                            prio = p
                    p_loss = int(scenario_key.split("_HIGH_C_LOSS_")[1].split("_{}_".format(prio))[0][3:])
                    phys_setup, rest = scenario_key.split("_{}_".format(prio))
                    num_pairs, rest = rest.split("_req_frac_")
                    req_frac, rest = rest.split("_origin_")
                    origin, scheduler = rest.split("_weights_")
                    prios_in_file = [prio]
                    metric_key = (phys_setup, prio, num_pairs, req_frac, origin, scheduler)


                print("Getting loss metric form key {}".format(metric_key))
                metrics = get_metrics_from_single_file(os.path.join(c_loss_folder, entry))
                total_matrix_time = metrics["TotalMatrixT (s)"] * 1e9

                fidelity = {p: metrics["AvgFid_Prio{}".format(p)] for p in prios_in_file}
                throughput = {p: metrics["AvgThroughp_Prio{} (1/s)".format(p)] for p in prios_in_file}
                latency = {p: metrics["AvgReqLaten_Prio{}_NodeID0 (s)".format(p)] for p in prios_in_file}
                nr_oks = {p: metrics["NrOKs_Prio{}".format(p)] for p in prios_in_file}

                c_loss_metrics[p_loss][metric_key] = {"F": fidelity, "T": throughput, "L": latency, "O": nr_oks, "maxtime": total_matrix_time}

    # print(c_loss_metrics[4][('QLINK_WC_WC_HIGH_C_LOSS_1e-04', 'uniform', 'lowerWFQ')]['O'])
    # print(c_loss_metrics[4][('QLINK_WC_WC_HIGH_C_LOSS_1e-04', 'uniform', 'lowerWFQ')]["maxtime"])
    # print(c_loss_metrics)
    print(counter)

    for p_loss, metric_per_p_loss in c_loss_metrics.items():
        for metric_key, metric_per_key in metric_per_p_loss.items():
            if len(metric_key) == 3:
                phys_setup, mix, scheduler = metric_key
                no_loss_setup = phys_setup.split("_HIGH_C_LOSS")[0]
                no_loss_file = other_basename + "{}_mix_{}_weights_{}_run_0.db".format(no_loss_setup, mix, scheduler)
                prios_in_file = prio_names
            else:
                phys_setup, prio, num_pairs, req_frac, origin, scheduler = metric_key
                no_loss_setup = phys_setup.split("_HIGH_C_LOSS")[0]
                no_loss_file = other_basename + "{}_{}_{}_req_frac_{}_origin_{}_weights_{}_run_0.db".format(no_loss_setup, prio, num_pairs, req_frac, origin, scheduler)
                prios_in_file = [prio]
            print("Getting no loss metrics from file {}".format(no_loss_file))

            metrics = get_metrics_from_single_file(no_loss_file, max_simulated_time=metric_per_key["maxtime"])
            # metrics = get_metrics_from_single_file(no_loss_file)
            total_matrix_time = metrics["TotalMatrixT (s)"] * 1e9
            if total_matrix_time < metric_per_key["maxtime"]:
                logger.warning("no loss has shorter matrix time")

            # fidelity = {p: metrics["AvgFid_Prio{}".format(p)] for p in prios_in_file}
            # throughput = {p: metrics["AvgThroughp_Prio{} (1/s)".format(p)] for p in prios_in_file}
            # latency = {p: metrics["AvgScaledReqLaten_Prio{}_NodeID0 (s)".format(p)] for p in prios_in_file}
            # nr_oks = {p: metrics["NrOKs_Prio{}".format(p)] for p in prios_in_file}

            f_diffs_per_prio = {}
            t_diffs_per_prio = {}
            l_diffs_per_prio = {}
            o_diffs_per_prio = {}
            abs_func = np.abs
            # abs_func = lambda x: x
            for p in prios_in_file:
                f1 = metric_per_key["F"][p]
                f2 = metrics["AvgFid_Prio{}".format(p)]
                f_rel_diff = abs_func(f1 - f2) / max(f1, f2)
                f_diffs_per_prio[p] = f_rel_diff

                t1 = metric_per_key["T"][p]
                t2 = metrics["AvgThroughp_Prio{} (1/s)".format(p)]
                t_rel_diff = abs_func(t1 - t2) / max(t1, t2)
                t_diffs_per_prio[p] = t_rel_diff

                l1 = metric_per_key["L"][p]
                l2 = metrics["AvgReqLaten_Prio{}_NodeID0 (s)".format(p)]
                l_rel_diff = abs_func(l1 - l2) / max(l1, l2)
                l_diffs_per_prio[p] = l_rel_diff

                o1 = metric_per_key["O"][p]
                o2 = metrics["NrOKs_Prio{}".format(p)]
                o_rel_diff = abs_func(o1 - o2) / max(o1, o2)
                o_diffs_per_prio[p] = o_rel_diff

            diffs[p_loss]["F"][metric_key[1:]] = f_diffs_per_prio
            diffs[p_loss]["T"][metric_key[1:]] = t_diffs_per_prio
            diffs[p_loss]["L"][metric_key[1:]] = l_diffs_per_prio
            diffs[p_loss]["O"][metric_key[1:]] = o_diffs_per_prio




    latex_begin = r"""
        \begin{tabular}{|l|cccc|}
            \hline
            $p_\mathrm{loss}$ & Max Rel. Diff. Fid. & Max Rel. Diff. Throughp. & Max Rel. Diff. Laten. & Max Rel. Diff. Nr pairs \\ \hline
"""
    latex_end = r"""
       \end{tabular}"""

    latex_middle = ""
    # for m_k_len, type in zip([2, 5], ["mix", "single"]):
    for m_k_len, type in zip([5], ["single"]):
        # max_f_diff = -1
        # max_t_diff = -1
        # max_l_diff = -1
        # max_o_diff = -1
        # max_f_scenario = None
        # max_t_scenario = None
        # max_l_scenario = None
        # max_o_scenario = None
        print("{}: ".format(type))
        for p_loss, diffs_per_p_loss in diffs.items():
            max_m_diffs = {m: -1 for m in ["F", "T", "L", "O"]}
            max_m_scenarios = {m: None for m in ["F", "T", "L", "O"]}
            print("  p_loss = {}".format(p_loss))
            for m, diffs_per_m in diffs_per_p_loss.items():
                for metric_key, diffs_per_metric_key in diffs_per_m.items():
                    if len(metric_key) == m_k_len:
                        max_m_tmp = max(diffs_per_metric_key.values())
                        if max_m_tmp > max_m_diffs[m]:
                            max_m_diffs[m] = max_m_tmp
                            max_m_scenarios[m] = metric_key

            print("     {}".format(max_m_diffs))
            diffs = [max_m_diffs[m] for m in ["F", "T", "L", "O"]]
            row_name = "\t" * 3 + r"$10^{-" + str(p_loss) + "}$"
            latex_middle += row_name + "".join([" & {0:.3f}".format(d) for d in diffs]) + r" \\ \hline" + "\n"
            # print("     {}".format(max_m_scenarios))
            print("")
        print("")

    latex_code = latex_begin + latex_middle[:-1] + latex_end
    print(latex_code)
    table_name = "high_c_loss"
    with open("/Volumes/Untitled/Dropbox/my_linklayer/tables/{}.tex".format(table_name), 'w') as f:
        f.write(latex_code)


        # print(diffs[4]["F"][('uniform', 'lowerWFQ')])
        # print(diffs[4]["T"][('uniform', 'lowerWFQ')])
        # print(diffs[4]["L"][('uniform', 'lowerWFQ')])
        # print(diffs[4]["O"][('uniform', 'lowerWFQ')])





if __name__ == '__main__':
    # c_loss_folder = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure"
    # other_basename = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure/2019-01-16T11:10:28CET_key_"

    c_loss_folder = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-28T16:49:28CET_CREATE_and_measure_tmp_at_2019-01-31T15:28:00CET"
    #run 1
    other_basename = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure/2019-01-16T11:10:28CET_key_"
    #run 2
    # other_basename = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-15T23:56:55CET_CREATE_and_measure/2019-01-15T23:56:55CET"
    main(c_loss_folder, other_basename)
