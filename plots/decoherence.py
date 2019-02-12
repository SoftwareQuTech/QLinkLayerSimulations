import numpy as np
from matplotlib import pyplot as plt

from netsquid.components.qnoisemodels import T1T2NoiseModel
from netsquid.qubits import qubitapi as qapi
from netsquid.qubits import operators as ops
from netsquid import set_qstate_formalism, DM_FORMALISM


def calc_fidelity(d_matrix):
    """
    Computes fidelity to the state 1/sqrt(2)(|01>+|10>)
    :param d_matrix: Density matrix
    :type d_matrix: :obj:`numpy.matrix`
    :return: The fidelity
    :rtype: float
    """
    # if outcome == 1:
    #     psi = np.matrix([[1, 0, 0, -1]]).transpose() / np.sqrt(2)
    # elif outcome == 2:
    #     psi = np.matrix([[0, 1, -1, 0]]).transpose() / np.sqrt(2)
    # else:
    #     raise ValueError("Unexpected outcome")
    psi = np.matrix([[0, 1, 1, 0]]).transpose() / np.sqrt(2)
    return np.real((psi.H * d_matrix * psi)[0, 0])


set_qstate_formalism(DM_FORMALISM)

electron_native_T1 = 3.9e6
electron_native_T2 = 3.3e3
# electron_dd_T1 = 0
# electron_dd_T2 = 1.46e9
electron_dd_T1 = 2.86e6
electron_dd_T2 = 1.00e6
carbon_T1 = 0
carbon_T2 = 10e9

dec_times = {
    # "Electron (natural)": (electron_native_T1, electron_native_T2),
    "Electron": (electron_dd_T1, electron_dd_T2),
    # "Carbon": (carbon_T1, carbon_T2)
}

num_points = 100
max_time = 1e6

c = 206753.41931034482
L = 500
comm_delay = (L / c) * 1e9
# t_points = [comm_delay * nr for nr in range(10)]
# L_points = [4000 * nr for nr in range(11)]
# L_points = np.linspace(0, 40000, 100)
L_points = np.linspace(0, 30/7, 100)
plt.rcParams.update({'font.size': 12})
markers = {"Electron": '>', "Carbon": '<'}
# t_points = np.linspace(0, max_time, num_points)
for name, times in dec_times.items():
    T1, T2 = times
    fid_points = []
    for L in L_points:
        noise = T1T2NoiseModel(T1, T2)

        q1, q2 = qapi.create_qubits(2)
        qapi.operate([q1], ops.H)
        qapi.operate([q1, q2], ops.CNOT)
        qapi.operate([q1], ops.X)

        t = (L/c) * 1e9

        noise.apply_noise(q1, t)
        noise.apply_noise(q2, t)

        d_matrix = q1.qstate._dm

        fid = calc_fidelity(d_matrix)
        fid_points.append(fid)

    # plt.plot(L_points, fid_points, label=name, linestyle='None', marker=markers[name], markersize=10)
    plt.plot(L_points, fid_points)

plt.xlabel("Distance km")
plt.ylabel("Fidelity")
# plt.xlim(0, 40000)
# plt.ylim(0.5, 1.01)
# plt.gca().axes.set_aspect(30000)
# plt.xticks(L_points, L_points)
# plt.legend(loc="lower right")
# plt.savefig('/Volumes/Untitled/Dropbox/my_linklayer/figures/fid_vs_comm.png')
plt.show()

