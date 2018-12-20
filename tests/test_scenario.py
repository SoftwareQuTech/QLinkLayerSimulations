import unittest
from types import MethodType

from netsquid.simutil import sim_run, sim_reset
from util.config_paths import ConfigPathStorage
from easysquid.easynetwork import setup_physical_network
from qlinklayer.egp import NodeCentricEGP
from qlinklayer.specific_scenarios import MixedScenario
from SimulaQron.cqc.backend.entInfoHeader import EntInfoCreateKeepHeader, EntInfoMeasDirectHeader


class TestScenario(unittest.TestCase):
    def setUp(self):
        sim_reset()
        network = setup_physical_network(ConfigPathStorage.NETWORK_NV_LAB_NOCAV_NOCONV)

        alice = network.get_node_by_id(0)
        bob = network.get_node_by_id(1)
        mhp_conn = network.get_connection(alice, bob, "mhp_conn")
        self.egp = NodeCentricEGP(alice, mhp_conn)

        network.start()

    def test_init(self):
        with self.assertRaises(TypeError):
            MixedScenario()
        with self.assertRaises(TypeError):
            MixedScenario(None)
        with self.assertRaises(TypeError):
            MixedScenario(None, None)

        request_cycle = 3
        request_params = {"A": {"not_prob": 0, "not_params": {}}}
        with self.assertRaises(KeyError):
            MixedScenario(self.egp, 3, request_params)

        request_params = {"A": {"prob": 0, "not_params": {}}}
        with self.assertRaises(KeyError):
            MixedScenario(self.egp, 3, request_params)

        request_params = {"A": {"not_prob": 0, "params": {}}}
        with self.assertRaises(KeyError):
            MixedScenario(self.egp, 3, request_params)

        request_params = {"A": {"prob": 1.3, "params": {}}}
        with self.assertRaises(AssertionError):
            MixedScenario(self.egp, 3, request_params)

        request_params = {"A": {"prob": 0.9, "params": {}}, "B": {"prob": 0.9, "params": {}}}
        with self.assertRaises(AssertionError):
            scen = MixedScenario(self.egp, 3, request_params)

        request_params = {"A": {"prob": 0.1, "params": {}}, "B": {"prob": 0.1, "params": {}}}
        scen = MixedScenario(self.egp, 3, request_params)
        self.assertEqual(scen.time_step, request_cycle)
        self.assertIs(scen.egp, self.egp)
        self.assertEqual(scen.scenario_names, ["A", "B"])
        self.assertEqual(list(scen.scenario_probs.values()), [0.1, 0.1])
        self.assertEqual(list(scen.scenario_num_requests.values()), [float('inf'), float('inf')])

    def test_run_protocol(self):
        paramsA = {"num_pairs": 1, "tmax_pair": 0, "min_fidelity": 0.9, "purpose_id": 0, "priority": 0, "store": True,
                   "atomic": False, "measure_directly": False}
        paramsB = {"num_pairs": 2, "tmax_pair": 1, "min_fidelity": 0.8, "purpose_id": 0, "priority": 1, "store": False,
                   "atomic": False, "measure_directly": True}
        paramsC = {"num_pairs": [3, 4], "tmax_pair": 2, "min_fidelity": 0.7, "purpose_id": 0, "priority": 2,
                   "store": True, "atomic": True, "measure_directly": False}
        request_params = {"A": {"prob": 0.1, "params": paramsA}, "B": {"prob": 0.3, "params": paramsB},
                          "C": {"prob": 0.5, "params": paramsC}, }

        scen = MixedScenario(self.egp, 1, request_params)

        requests = []

        def fake_create(cqc_request_raw):
            egp_request = NodeCentricEGP._get_egp_request(cqc_request_raw)
            requests.append(egp_request)

        scen._create = fake_create

        scen.start()

        num_cycles = 5000
        sim_run(num_cycles)

        prioritites = [req.priority for req in requests]
        fractions = [prioritites.count(i)/num_cycles for i in range(3)]
        avg_num_pairs = {"A": 1, "B": 2, "C": 3.5}
        ideal_fractions = [request_params[name]["prob"]/avg_num_pairs[name] for name in ["A", "B", "C"]]
        for f, id_f in zip(fractions, ideal_fractions):
            self.assertAlmostEqual(f, id_f, places=1)

        Crequests = []
        for req in requests:
            if req.priority == 0:
                params = paramsA
                self.assertEqual(req.num_pairs, params["num_pairs"])
            elif req.priority == 1:
                params = paramsB
                self.assertEqual(req.num_pairs, params["num_pairs"])
            else:
                params = paramsC
                Crequests.append(req)
            self.assertAlmostEqual(req.max_time, params["tmax_pair"] * req.num_pairs)
            self.assertAlmostEqual(req.min_fidelity, params["min_fidelity"])
            self.assertEqual(req.purpose_id, params["purpose_id"])
            self.assertEqual(req.store, params["store"])
            self.assertEqual(req.atomic, params["atomic"])
            self.assertEqual(req.measure_directly, params["measure_directly"])

        frac_num_pairs = [[req.num_pairs for req in Crequests].count(n_p)/len(Crequests) for n_p in paramsC["num_pairs"]]
        one_over_num_pairs = [1 / num_p for num_p in paramsC["num_pairs"]]
        ideal_frac_num_pairs = [p / sum(one_over_num_pairs) for p in one_over_num_pairs]
        for f_n_p, i_f_n_p in zip(frac_num_pairs, ideal_frac_num_pairs):
            self.assertAlmostEqual(f_n_p, i_f_n_p, places=1)

    def test__ok_callback(self):
        paramsA = {"num_pairs": 1, "tmax_pair": 0, "min_fidelity": 0.9, "purpose_id": 0, "priority": 0, "store": True,
                   "atomic": False, "measure_directly": False}
        paramsB = {"num_pairs": 2, "tmax_pair": 1, "min_fidelity": 0.8, "purpose_id": 0, "priority": 1, "store": False,
                   "atomic": False, "measure_directly": True}
        paramsC = {"num_pairs": [3, 4], "tmax_pair": 2, "min_fidelity": 0.7, "purpose_id": 0, "priority": 2,
                   "store": True, "atomic": True, "measure_directly": False}
        request_params = {"A": {"prob": 0.1, "params": paramsA}, "B": {"prob": 0.3, "params": paramsB},
                          "C": {"prob": 0.5, "params": paramsC}, }

        scen = MixedScenario(self.egp, 1, request_params)

        def MD_result(i):
            return NodeCentricEGP.construct_cqc_ok_message(EntInfoMeasDirectHeader.type, create_id=i,
                                                           ent_id=(0, 0, i),
                                                           fidelity_estimate=0.5, t_create=0, m=0, basis=0)

        def CK_result(i):
            return NodeCentricEGP.construct_cqc_ok_message(EntInfoCreateKeepHeader.type, create_id=i,
                                                           ent_id=(0, 0, i),
                                                           fidelity_estimate=0.5, t_create=0, logical_id=0,
                                                           t_goodness=0)

        MD_results = [MD_result(create_id) for create_id in range(10)]
        CK_results = [CK_result(create_id) for create_id in range(10, 20)]

        def fake_store_qstate(self, qubit_id, source_id, other_id, mhp_seq):
            key = (source_id, other_id, mhp_seq)
            self.entangled_qstates[key] = None

        scen.store_qstate = MethodType(fake_store_qstate, scen)

        results = []
        for i in range(len(MD_results) + len(CK_results)):
            if i % 2 == 0:
                result = MD_results.pop(0)
            else:
                self.egp.node.qmem.create_qubits(1)
                result = CK_results.pop(0)
            results.append(result)
            scen._ok_callback(result)

        self.assertEqual(scen.ok_storage, results)
        self.assertEqual(len(scen.entangled_qstates), 10)
        self.assertEqual(len(scen.node_measurement_storage), len(results))


if __name__ == '__main__':
    unittest.main()
