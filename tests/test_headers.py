import unittest
from copy import copy

from qlinklayer.egp import EGPRequest
from qlinklayer.scenario import EGPSimulationScenario


class TestRequestHeader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.other_ip = 1
        cls.other_port = 0
        cls.num_pairs = 3
        cls.min_fidelity = 0.5
        cls.max_time = 0.1
        cls.purpose_id = 4
        cls.priority = 5
        cls.store = False
        cls.measure_directly = True
        cls.sched_cycle = 3

        cls.create_id = 6
        cls.create_time = 0.2

    def test_init(self):
        request = EGPRequest()
        self.assertEqual(request.other_ip, 0)
        self.assertEqual(request.other_port, 0)
        self.assertEqual(request.otherID, 0)
        self.assertEqual(request.num_pairs, 0)
        self.assertAlmostEqual(request.min_fidelity, 0)
        self.assertAlmostEqual(request.max_time, 0)
        self.assertEqual(request.purpose_id, 0)
        self.assertEqual(request.priority, 0)
        self.assertIs(request.store, True)
        self.assertIs(request.measure_directly, False)
        self.assertEqual(request.create_id, 0)
        self.assertAlmostEqual(request.create_time, 0)
        self.assertEqual(request.sched_cycle, 0)

        request = EGPRequest(EGPSimulationScenario.construct_cqc_epr_request(otherID=self.other_ip,
                                                                             num_pairs=self.num_pairs,
                                                                             min_fidelity=self.min_fidelity,
                                                                             max_time=self.max_time,
                                                                             purpose_id=self.purpose_id,
                                                                             priority=self.priority,
                                                                             store=self.store,
                                                                             measure_directly=self.measure_directly))
        self.assertEqual(request.other_ip, self.other_ip)
        self.assertEqual(request.other_port, self.other_port)
        self.assertEqual(request.otherID, self.other_ip)
        self.assertEqual(request.num_pairs, self.num_pairs)
        self.assertAlmostEqual(request.min_fidelity, self.min_fidelity)
        self.assertAlmostEqual(request.max_time, self.max_time)
        self.assertEqual(request.purpose_id, self.purpose_id)
        self.assertEqual(request.priority, self.priority)
        self.assertIs(request.store, self.store)
        self.assertIs(request.measure_directly, self.measure_directly)
        self.assertEqual(request.create_id, 0)
        self.assertAlmostEqual(request.create_time, 0)
        self.assertEqual(request.sched_cycle, 0)

    def test_assign_create_id(self):
        request = EGPRequest(EGPSimulationScenario.construct_cqc_epr_request(otherID=self.other_ip,
                                                                             num_pairs=self.num_pairs,
                                                                             min_fidelity=self.min_fidelity,
                                                                             max_time=self.max_time,
                                                                             purpose_id=self.purpose_id,
                                                                             priority=self.priority,
                                                                             store=self.store,
                                                                             measure_directly=self.measure_directly))
        request.assign_create_id(self.create_id, self.create_time)
        self.assertEqual(request.other_ip, self.other_ip)
        self.assertEqual(request.other_port, self.other_port)
        self.assertEqual(request.otherID, self.other_ip)
        self.assertEqual(request.num_pairs, self.num_pairs)
        self.assertAlmostEqual(request.min_fidelity, self.min_fidelity)
        self.assertAlmostEqual(request.max_time, self.max_time)
        self.assertEqual(request.purpose_id, self.purpose_id)
        self.assertEqual(request.priority, self.priority)
        self.assertIs(request.store, self.store)
        self.assertIs(request.measure_directly, self.measure_directly)
        self.assertEqual(request.create_id, self.create_id)
        self.assertAlmostEqual(request.create_time, self.create_time)
        self.assertEqual(request.sched_cycle, 0)

        create_id, create_time = request.get_create_info()
        self.assertEqual(create_id, self.create_id)
        self.assertAlmostEqual(create_time, self.create_time)

    def test_add_sched_cycle(self):
        request = EGPRequest(EGPSimulationScenario.construct_cqc_epr_request(otherID=self.other_ip,
                                                                             num_pairs=self.num_pairs,
                                                                             min_fidelity=self.min_fidelity,
                                                                             max_time=self.max_time,
                                                                             purpose_id=self.purpose_id,
                                                                             priority=self.priority,
                                                                             store=self.store,
                                                                             measure_directly=self.measure_directly))
        request.add_sched_cycle(self.sched_cycle)
        self.assertEqual(request.other_ip, self.other_ip)
        self.assertEqual(request.other_port, self.other_port)
        self.assertEqual(request.otherID, self.other_ip)
        self.assertEqual(request.num_pairs, self.num_pairs)
        self.assertAlmostEqual(request.min_fidelity, self.min_fidelity)
        self.assertAlmostEqual(request.max_time, self.max_time)
        self.assertEqual(request.purpose_id, self.purpose_id)
        self.assertEqual(request.priority, self.priority)
        self.assertIs(request.store, self.store)
        self.assertIs(request.measure_directly, self.measure_directly)
        self.assertEqual(request.create_id, 0)
        self.assertAlmostEqual(request.create_time, 0.0)
        self.assertEqual(request.sched_cycle, self.sched_cycle)

        create_id, create_time = request.get_create_info()
        self.assertEqual(create_id, 0)
        self.assertAlmostEqual(create_time, 0.0)

    def test_copy(self):
        request = EGPRequest(EGPSimulationScenario.construct_cqc_epr_request(otherID=self.other_ip,
                                                                             num_pairs=self.num_pairs,
                                                                             min_fidelity=self.min_fidelity,
                                                                             max_time=self.max_time,
                                                                             purpose_id=self.purpose_id,
                                                                             priority=self.priority,
                                                                             store=self.store,
                                                                             measure_directly=self.measure_directly))
        request.assign_create_id(self.create_id, self.create_time)
        request.add_sched_cycle(self.sched_cycle)
        request_copy = copy(request)
        self.assertEqual(request.other_ip, request_copy.other_ip)
        self.assertEqual(request.other_port, request_copy.other_port)
        self.assertEqual(request.otherID, request_copy.other_ip)
        self.assertEqual(request.num_pairs, request_copy.num_pairs)
        self.assertAlmostEqual(request.min_fidelity, request_copy.min_fidelity)
        self.assertAlmostEqual(request.max_time, request_copy.max_time)
        self.assertEqual(request.purpose_id, request_copy.purpose_id)
        self.assertEqual(request.priority, request_copy.priority)
        self.assertIs(request.store, request_copy.store)
        self.assertIs(request.measure_directly, request_copy.measure_directly)
        self.assertEqual(request.create_id, request_copy.create_id)
        self.assertAlmostEqual(request.create_time, request_copy.create_time)
        self.assertEqual(request.sched_cycle, request_copy.sched_cycle)

        request_copy.create_time += 0.1
        self.assertEqual(request.create_time, self.create_time)

    def test_pack_unpack(self):
        request = EGPRequest(EGPSimulationScenario.construct_cqc_epr_request(otherID=self.other_ip,
                                                                             num_pairs=self.num_pairs,
                                                                             min_fidelity=self.min_fidelity,
                                                                             max_time=self.max_time,
                                                                             purpose_id=self.purpose_id,
                                                                             priority=self.priority,
                                                                             store=self.store,
                                                                             measure_directly=self.measure_directly))
        request.assign_create_id(self.create_id, self.create_time)
        request.add_sched_cycle(self.sched_cycle)
        requestH = request.pack()
        self.assertEqual(len(requestH), 26)
        request_copy = EGPRequest()
        request_copy.unpack(requestH)
        self.assertEqual(request.other_ip, request_copy.other_ip)
        self.assertEqual(request.other_port, request_copy.other_port)
        self.assertEqual(request.otherID, request_copy.other_ip)
        self.assertEqual(request.num_pairs, request_copy.num_pairs)
        self.assertAlmostEqual(request.min_fidelity, request_copy.min_fidelity)
        self.assertAlmostEqual(request.max_time, request_copy.max_time)
        self.assertEqual(request.purpose_id, request_copy.purpose_id)
        self.assertEqual(request.priority, request_copy.priority)
        self.assertIs(request.store, request_copy.store)
        self.assertIs(request.measure_directly, request_copy.measure_directly)
        self.assertEqual(request.create_id, request_copy.create_id)
        self.assertAlmostEqual(request.create_time, request_copy.create_time)
        self.assertEqual(request.sched_cycle, request_copy.sched_cycle)

        request_copy.create_time += 0.1
        self.assertAlmostEqual(request.create_time, self.create_time)


if __name__ == '__main__':
    unittest.main()
