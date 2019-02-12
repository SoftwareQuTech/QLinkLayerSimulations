import os

# Nr of runs
major_runs = 169 + 169
high_c_loss = 35
fidelity_sweep = 360
req_freq_sweep = 400 + 160 + 360

print(fidelity_sweep + req_freq_sweep)

print("Tot nr runs {}".format(major_runs + high_c_loss + fidelity_sweep + req_freq_sweep))

print("Tot wall time {}".format(major_runs * 120 + high_c_loss * 70 * (fidelity_sweep + req_freq_sweep) * 24))
# print("Tot wall time {}".format(major_runs * 120/2))

# Simulated time
major_runs = 359959.982 + 520436.1181
high_c_loss = 66466.14616
fidelity_sweep = 249297.9855
req_freq_sweep = 351964.1007 + 183720.0537 + 402213.8878

tot_sim_time = major_runs + high_c_loss + fidelity_sweep + req_freq_sweep
print("Tot simulated time {}".format((tot_sim_time) / 3600))

print("Tot simulated MHP cycles {}".format(tot_sim_time * 1e9 / 10120))



###
# RUN 1
# Req freq: Low
#      Max Fidelity diff: (0.025058239624428328, 'LAB_NC_NC_MD_max3_req_frac_low_origin_originAB_weights_FIFO')
#      Max Throughput diff: (0.06692670116629358, 'LAB_NC_NC_MD_max1_req_frac_low_origin_originAB_weights_FIFO')
#      Max NumOKs diff: (0.0677055346587856, 'LAB_NC_NC_MD_max1_req_frac_low_origin_originAB_weights_FIFO')
#      Max Latency diff: (0.07329766851040084, 'LAB_NC_NC_CK_max1_req_frac_low_origin_originAB_weights_FIFO')
#
# Req freq: High
#      Max Fidelity diff: (0.03163664659769791, 'LAB_NC_NC_MD_max1_req_frac_high_origin_originAB_weights_FIFO')
#      Max Throughput diff: (0.07802874743326482, 'QLINK_WC_WC_NL_max3_req_frac_high_origin_originAB_weights_FIFO')
#      Max NumOKs diff: (0.08550185873605948, 'QLINK_WC_WC_NL_max3_req_frac_high_origin_originAB_weights_FIFO')
#      Max Latency diff: (0.03792230271249774, 'QLINK_WC_WC_CK_max3_req_frac_high_origin_originAB_weights_FIFO')
#
# Req freq: Ultra
#      Max Fidelity diff: (0.03302968737706482, 'LAB_NC_NC_MD_max1_req_frac_ultra_origin_originAB_weights_FIFO')
#      Max Throughput diff: (0.10043873855304625, 'LAB_NC_NC_NL_max3_req_frac_ultra_origin_originAB_weights_FIFO')
#      Max NumOKs diff: (0.10039982230119947, 'LAB_NC_NC_NL_max3_req_frac_ultra_origin_originAB_weights_FIFO')
#      Max Latency diff: (0.04815717111222987, 'QLINK_WC_WC_NL_max3_req_frac_ultra_origin_originAB_weights_FIFO')
#
# Req freq: Mixed
#      Max Fidelity diff: (0.10837404566587024, 'LAB_NC_NC_mix_moreCK_weights_FIFO')
#      Max Throughput diff: (0.6477025340338451, 'LAB_NC_NC_mix_moreMD_weights_higherWFQ')
#      Max NumOKs diff: (0.64900314795383, 'LAB_NC_NC_mix_moreMD_weights_higherWFQ')
#      Max Latency diff: (0.8633216110357311, 'LAB_NC_NC_mix_moreCK_weights_lowerWFQ')

counter = 1
for entry in os.listdir("/Users/adahlberg/Documents/QLinkLayer/simulations/major_simulation/2019-01-16T11:10:28CET_CREATE_and_measure"):
    if entry.endswith(".db"):
        if "AB" in entry:
            counter += 1
print(counter)

# 103
