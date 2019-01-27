import numpy as np

from simulations.major_simulation.generate_metrics_file import get_raw_metric_data, get_metrics_from_single_file

c_loss = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure/2019-01-16T11:10:28CET_key_QLINK_WC_WC_HIGH_C_LOSS_1e-06_mix_uniform_weights_lowerWFQ_run_0.db"
no_c_loss = "/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure/2019-01-16T11:10:28CET_key_QLINK_WC_WC_mix_uniform_weights_lowerWFQ_run_0.db"

c_loss_metrics = get_metrics_from_single_file(c_loss)
no_c_loss_metrics = get_metrics_from_single_file(no_c_loss)

prio_names = ["NL"]

max_f_diff = -1
max_t_diff = -1
max_l_diff = -1
max_o_diff = -1
max_f_prio = None
max_t_prio = None
max_l_prio = None
max_o_prio = None
for p in prio_names:
    f1 = c_loss_metrics["AvgFid_Prio{}".format(p)]
    f2 = no_c_loss_metrics["AvgFid_Prio{}".format(p)]
    f_diff = np.abs(f1 - f2) / max(f1, f2)
    if f_diff > max_f_diff:
        max_f_diff = f_diff
        max_f_prio = p

    t1 = c_loss_metrics["AvgThroughp_Prio{} (1/s)".format(p)]
    t2 = no_c_loss_metrics["AvgThroughp_Prio{} (1/s)".format(p)]
    t_diff = np.abs(t1 - t2) / max(t1, t2)
    if t_diff > max_t_diff:
        max_t_diff = t_diff
        max_t_prio = p

    l1 = c_loss_metrics["AvgScaledReqLaten_Prio{}_NodeID0 (s)".format(p)]
    l2 = no_c_loss_metrics["AvgScaledReqLaten_Prio{}_NodeID0 (s)".format(p)]
    l_diff = np.abs(l1 - l2) / max(l1, l2)
    if l_diff > max_l_diff:
        max_l_diff = l_diff
        max_l_prio = p

    o1 = c_loss_metrics["NrOKs_Prio{}".format(p)]
    o2 = no_c_loss_metrics["NrOKs_Prio{}".format(p)]
    o_diff = np.abs(o1 - o2) / max(o1, o2)
    if o_diff > max_o_diff:
        max_o_diff = o_diff
        max_o_prio = p

print("F: {} ({})".format(max_f_diff, max_f_prio))
print("T: {} ({})".format(max_t_diff, max_t_prio))
print("L: {} ({})".format(max_l_diff, max_l_prio))
print("O: {} ({})".format(max_o_diff, max_o_prio))
