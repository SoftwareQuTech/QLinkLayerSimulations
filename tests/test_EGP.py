import unittest
import netsquid as ns
from collections import defaultdict
from functools import partial
from math import ceil
import logging
import random
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easynetwork import EasyNetwork
from easysquid.entanglementGenerator import NV_PairPreparation
from easysquid.puppetMaster import PM_Controller, PM_Test
from easysquid.qnode import QuantumNode
from easysquid.quantumMemoryDevice import NVCommunicationDevice
from easysquid.toolbox import SimulationScheduler, logger
from netsquid.components.delaymodels import FibreDelayModel
from netsquid.simutil import sim_reset, sim_run
from qlinklayer.egp import NodeCentricEGP
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection
from qlinklayer.scenario import EGPSimulationScenario, MeasureAfterSuccessScenario, MeasureBeforeSuccessScenario
from SimulaQron.cqc.backend.entInfoHeader import EntInfoCreateKeepHeader, EntInfoMeasDirectHeader

logger.setLevel(logging.ERROR)


def store_result(storage, result):
    try:
        storage.append((EntInfoMeasDirectHeader.type,) + MeasureBeforeSuccessScenario.unpack_cqc_ok(result))
        return
    except ValueError:
        pass
    try:
        storage.append((EntInfoCreateKeepHeader.type,) + MeasureAfterSuccessScenario.unpack_cqc_ok(result))
        return
    except ValueError:
        pass
    raise ValueError("Unknown OK type")


def store_errors(storage, result):
    storage.append(result)


def count_errors(storage):
    errors = list(filter(lambda item: len(item) == 2, storage))
    return len(errors)


class PM_Test_Ent(PM_Test):
    def __init__(self, name):
        super(PM_Test_Ent, self).__init__(name=name)
        self.stored_data = []
        self.num_tested_items = 0

    def _test(self, event):
        egp = event.source
        assert len(self.stored_data) == self.num_tested_items + 1
        assert len(self.stored_data) == len(set(self.stored_data))

        self.num_tested_items += 1
        try:
            create_id, ent_id, logical_id, goodness, t_create, t_goodness = self.stored_data[-1]
        except Exception as err:
            print(err)
            raise err
        assert len(ent_id) == 3

        creator, peer, mhp_seq = ent_id
        assert egp.node.nodeID == creator or egp.node.nodeID == peer

    def store_data(self, result):
        try:
            self.stored_data.append(MeasureBeforeSuccessScenario.unpack_cqc_ok(result))
            return
        except ValueError:
            pass
        try:
            self.stored_data.append(MeasureAfterSuccessScenario.unpack_cqc_ok(result))
            return
        except ValueError:
            pass
        raise ValueError("Unknown OK type")


class PM_Test_Counter(PM_Test):
    def __init__(self, name):
        super(PM_Test_Counter, self).__init__(name=name)
        self.num_tested_items = 0

    def _test(self, event):
        self.num_tested_items += 1


class TestNodeCentricEGP(unittest.TestCase):
    def setUp(self):
        ns.set_qstate_formalism(ns.DM_FORMALISM)
        sim_reset()
        self.alice_results = []
        self.bob_results = []
        self.alice_callback = partial(store_result, storage=self.alice_results)
        self.bob_callback = partial(store_result, storage=self.bob_results)
        self.alice_err_callback = partial(store_errors, storage=self.alice_results)
        self.bob_err_callback = partial(store_errors, storage=self.bob_results)

    def check_memories(self, aliceMemory, bobMemory, addresses):
        # Check the entangled pairs, ignore communication qubit
        for i in addresses:
            qA = aliceMemory.peek(i + 1)[0]
            qB = bobMemory.peek(i + 1)[0]
            self.assertEqual(qA.qstate.dm.shape, (4, 4))
            self.assertTrue(qA.qstate.compare(qB.qstate))
            self.assertIn(qB, qA.qstate._qubits)
            self.assertIn(qA, qB.qstate._qubits)

    def create_nodes(self, alice_device_positions, bob_device_positions):
        # Set up Alice
        aliceMemory = NVCommunicationDevice(name="AliceMem", num_positions=alice_device_positions,
                                            pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = NVCommunicationDevice(name="BobMem", num_positions=bob_device_positions,
                                          pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        return alice, bob

    def create_egps(self, nodeA, nodeB, connected=True, accept_all=True, num_priorities=1, strict_priority=False):
        if strict_priority:
            weights = [0] * num_priorities
        else:
            weights = None
        # Set up EGP
        egpA = NodeCentricEGP(node=nodeA, err_callback=self.alice_err_callback, ok_callback=self.alice_callback,
                              accept_all_requests=accept_all, num_priorities=num_priorities,
                              scheduler_weights=weights)
        egpB = NodeCentricEGP(node=nodeB, err_callback=self.bob_err_callback, ok_callback=self.bob_callback,
                              accept_all_requests=accept_all, num_priorities=num_priorities,
                              scheduler_weights=weights)

        if connected:
            egpA.connect_to_peer_protocol(egpB)

        return egpA, egpB

    def create_network(self, egpA, egpB):
        nodes = [
            (egpA.node, [egpA, egpA.dqp, egpA.mhp]),
            (egpB.node, [egpB, egpB.dqp, egpB.mhp])
        ]

        conns = [
            (egpA.dqp.conn, "dqp_conn", [egpA.dqp, egpB.dqp]),
            (egpA.conn, "egp_conn", [egpA, egpB]),
            (egpA.mhp.conn, "mhp_conn", [egpA.mhp, egpB.mhp])
        ]

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        return network

    def test_create(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_create_counter = PM_Test_Counter(name="AliceCreateCounter")
        bob_create_counter = PM_Test_Counter(name="BobCreateCounter")
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_CREATE, ds=alice_create_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_CREATE, ds=bob_create_counter)
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # Schedule egp CREATE commands mid simulation
        alice_pairs = 1
        bob_pairs = 2
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0,
                                                                        purpose_id=1, priority=10)
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_pairs,
                                                                      min_fidelity=0.5, max_time=0,
                                                                      purpose_id=2, priority=2)

        # Schedule a sequence of various create requests
        alice_create_id = egpA.create(alice_request)
        bob_create_id = egpB.create(bob_request)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(10)

        # Check both nodes have the same results
        self.assertEqual(len(self.alice_results), alice_pairs + bob_pairs)
        self.assertEqual(self.alice_results, self.bob_results)

        # Verify the individual results
        _, create_id, ent_id, logical_id, _, _, _ = self.alice_results[0]
        self.assertEqual(create_id, alice_create_id)
        self.assertEqual(ent_id, (alice.nodeID, bob.nodeID, 0))
        self.assertEqual(logical_id, 1)

        _, create_id, ent_id, logical_id, _, _, _ = self.bob_results[1]
        self.assertEqual(create_id, bob_create_id)
        self.assertEqual(ent_id, (bob.nodeID, alice.nodeID, 1))
        self.assertEqual(logical_id, 2)

        _, create_id, ent_id, logical_id, _, _, _ = self.bob_results[2]
        self.assertEqual(create_id, bob_create_id)
        self.assertEqual(ent_id, (bob.nodeID, alice.nodeID, 2))
        self.assertEqual(logical_id, 3)

        self.check_memories(alice.qmem, bob.qmem, range(alice_pairs + bob_pairs))

        # Verify that the pydynaa create events were scheduled correctly
        self.assertTrue(alice_create_counter.test_passed())
        self.assertTrue(bob_create_counter.test_passed())
        self.assertEqual(alice_create_counter.num_tested_items, 1)
        self.assertEqual(bob_create_counter.num_tested_items, 1)
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_multi_create(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_create_counter = PM_Test_Counter(name="AliceCreateCounter")
        bob_create_counter = PM_Test_Counter(name="BobCreateCounter")
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_CREATE, ds=alice_create_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_CREATE, ds=bob_create_counter)
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # Schedule egp CREATE commands mid simulation
        alice_pairs = 1
        num_requests = 4
        alice_requests = [EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                          min_fidelity=0.5, max_time=0,
                                                                          purpose_id=1, priority=10)
                          for _ in range(num_requests)]

        # Schedule a sequence of various create requests
        alice_create_info = []
        for request in alice_requests:
            alice_create_id = egpA.create(request)
            alice_create_info.append(alice_create_id)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(10)

        # Verify all requests returned results
        self.assertEqual(len(self.alice_results), num_requests)
        self.assertEqual(self.alice_results, self.bob_results)

        # Verify that the create id incremented for each call and was tracked for each request
        for create_id, result in zip(alice_create_info, self.alice_results):
            _, stored_id, ent_id, _, _, _, _ = result
            self.assertEqual(stored_id, create_id)

        # Verify that the pydynaa create events were scheduled correctly
        self.assertTrue(alice_create_counter.test_passed())
        self.assertTrue(bob_create_counter.test_passed())
        self.assertEqual(alice_create_counter.num_tested_items, num_requests)
        self.assertEqual(bob_create_counter.num_tested_items, 0)
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_successful_simulation(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_pairs = 1
        bob_pairs = 2
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0,
                                                                        purpose_id=1, priority=10)
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_pairs,
                                                                      min_fidelity=0.5, max_time=0,
                                                                      purpose_id=2, priority=2)

        alice_scheduled_create = partial(egpA.create, cqc_request_raw=alice_request)
        bob_scheduled_create = partial(egpB.create, cqc_request_raw=bob_request)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=0)
        sim_scheduler.schedule_function(func=bob_scheduled_create, t=5)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(10)

        # Verify both nodes have all results
        self.assertEqual(len(self.alice_results), alice_pairs + bob_pairs)
        self.assertEqual(self.alice_results, self.bob_results)

        # Check the entangled pairs, ignore communication qubit
        self.check_memories(alice.qmem, bob.qmem, range(alice_pairs + bob_pairs))

    def test_successful_measure_directly(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_num_bits = 255
        bob_num_bits = 255
        # Construct a request with higher priority
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_num_bits,
                                                                        min_fidelity=0.9, max_time=0,
                                                                        purpose_id=1, priority=0,
                                                                        measure_directly=True)

        # Construct a request with lower priority
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_bits,
                                                                      min_fidelity=0.9, max_time=0,
                                                                      purpose_id=2, priority=1, measure_directly=True)

        alice_scheduled_create = partial(egpA.create, cqc_request_raw=alice_request)
        bob_scheduled_create = partial(egpB.create, cqc_request_raw=bob_request)

        # Schedule a sequence of various create requests
        # We issue the low priority request first so that it begins and then verify that issuing a higher priority
        # request afterwards will overtake the low priority request
        sim_scheduler.schedule_function(func=bob_scheduled_create, t=0)
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=2)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(500)
        import pdb
        pdb.set_trace()

        # Verify all the bits were generated
        self.assertEqual(len(self.alice_results), alice_num_bits + bob_num_bits)

        # Verify that the results are correct
        correlated_measurements = defaultdict(int)
        total_measurements = defaultdict(int)
        for resA, resB in zip(self.alice_results, self.bob_results):
            _, a_create, a_id, a_m, a_basis, _, a_t = resA
            _, b_create, b_id, b_m, b_basis, _, b_t = resB
            self.assertEqual(a_create, b_create)
            self.assertEqual(a_id, b_id)
            self.assertEqual(a_t, b_t)

            # Count occurrences of measurements that should be correlated
            if a_basis == b_basis:
                total_measurements[a_basis] += 1
                if a_m == b_m:
                    correlated_measurements[a_basis] += 1

        # Assume basis == 0 -> Z and basis == 1 -> X
        alpha = egpA.mhp.alpha
        expected_z = 1 - alpha / (4 - 3 * alpha)
        actual_z = correlated_measurements[0] / total_measurements[0]
        expected_x = (8 - 7 * alpha) / (8 - 6 * alpha)
        actual_x = correlated_measurements[1] / total_measurements[1]
        expected_y = (8 - 7 * alpha) / (8 - 6 * alpha)
        actual_y = correlated_measurements[2] / total_measurements[1]

        # Allow a tolerance of 10%
        tolerance = 0.1
        self.assertGreaterEqual(actual_z, expected_z - tolerance)
        self.assertGreaterEqual(actual_x, expected_x - tolerance)
        self.assertGreaterEqual(actual_y, expected_y - tolerance)

    def test_successful_mixed_requests(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_num_pairs = 1
        alice_num_bits = 255
        bob_num_pairs = 2
        bob_num_bits = 255
        alice_request_epr = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID,
                                                                            num_pairs=alice_num_pairs, min_fidelity=0.5,
                                                                            max_time=0, purpose_id=1, priority=10)

        alice_request_bits = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID,
                                                                             num_pairs=alice_num_bits, min_fidelity=0.5,
                                                                             max_time=0, purpose_id=1, priority=10,
                                                                             measure_directly=True)

        bob_request_epr = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_pairs,
                                                                          min_fidelity=0.9, max_time=0,
                                                                          purpose_id=2, priority=2)

        bob_request_bits = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_bits,
                                                                           min_fidelity=0.9, max_time=0,
                                                                           purpose_id=2, priority=2,
                                                                           measure_directly=True)

        alice_scheduled_epr = partial(egpA.create, cqc_request_raw=alice_request_epr)
        alice_scheduled_bits = partial(egpA.create, cqc_request_raw=alice_request_bits)
        bob_scheduled_epr = partial(egpB.create, cqc_request_raw=bob_request_epr)
        bob_scheduled_bits = partial(egpB.create, cqc_request_raw=bob_request_bits)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=alice_scheduled_epr, t=0)
        sim_scheduler.schedule_function(func=bob_scheduled_bits, t=2.5)
        sim_scheduler.schedule_function(func=bob_scheduled_epr, t=5)
        sim_scheduler.schedule_function(func=alice_scheduled_bits, t=7.5)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(500)
        self.assertEqual(len(self.alice_results), alice_num_bits + bob_num_bits + alice_num_pairs + bob_num_pairs)

        # Check the generated bits
        correlated_measurements = defaultdict(int)
        total_measurements = defaultdict(int)
        for resA, resB in zip(self.alice_results, self.bob_results):
            self.assertEqual(resA[0], resB[0])
            if resA[0] == EntInfoCreateKeepHeader.type:
                continue
            _, a_create, a_id, a_m, a_basis, _, a_t = resA
            _, b_create, b_id, b_m, b_basis, _, b_t = resB
            self.assertEqual(a_create, b_create)
            self.assertEqual(a_id, b_id)
            self.assertEqual(a_t, b_t)

            if a_basis == b_basis:
                total_measurements[a_basis] += 1
                if a_m == b_m:
                    correlated_measurements[a_basis] += 1

        # Assume basis == 0 -> Z and basis == 1 -> X
        alpha = egpA.mhp.alpha
        expected_z = 1 - alpha / (4 - 3 * alpha)
        actual_z = correlated_measurements[0] / total_measurements[0]
        expected_x = (8 - 7 * alpha) / (8 - 6 * alpha)
        actual_x = correlated_measurements[1] / total_measurements[1]
        expected_y = (8 - 7 * alpha) / (8 - 6 * alpha)
        actual_y = correlated_measurements[2] / total_measurements[1]

        # Allow a tolerance of 10%
        tolerance = 0.1
        self.assertGreaterEqual(actual_z, expected_z - tolerance)
        self.assertGreaterEqual(actual_x, expected_x - tolerance)
        self.assertGreaterEqual(actual_y, expected_y - tolerance)

        # Check the entangled pairs, ignore communication qubit
        self.check_memories(alice.qmem, bob.qmem, range(alice_num_pairs + bob_num_pairs))

    def test_asymmetric_mixed_requests(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(alice, bob, connected=False, accept_all=True)

        egp_conn = ClassicalFibreConnection(nodeA=alice, nodeB=bob, length=0.05)
        dqp_conn = ClassicalFibreConnection(nodeA=alice, nodeB=bob, length=0.05)

        c = FibreDelayModel().c
        time_window = 1
        lengthA = 0.02
        lengthB = 0.03
        delayB = lengthB * 1e9 / c
        delayAM = lengthA * 1e9 / c
        mhp_cycle_period = 1.1
        delayMA = (2 * delayB - delayAM)
        mhp_conn = NodeCentricMHPHeraldedConnection(nodeA=alice, nodeB=bob, lengthA=lengthA, lengthB=lengthB,
                                                    delay_A=[delayAM, delayMA], time_window=time_window,
                                                    use_time_window=True, t_cycle=mhp_cycle_period)

        egpA.connect_to_peer_protocol(egpB, egp_conn=egp_conn, dqp_conn=dqp_conn, mhp_conn=mhp_conn)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_num_pairs = 1
        alice_num_bits = 30
        bob_num_pairs = 2
        bob_num_bits = 30
        alice_request_epr = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID,
                                                                            num_pairs=alice_num_pairs, min_fidelity=0.5,
                                                                            max_time=0, purpose_id=1, priority=10)

        alice_request_bits = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID,
                                                                             num_pairs=alice_num_bits, min_fidelity=0.5,
                                                                             max_time=0, purpose_id=1, priority=10,
                                                                             measure_directly=True)

        bob_request_epr = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_pairs,
                                                                          min_fidelity=0.9, max_time=0,
                                                                          purpose_id=2, priority=2)

        bob_request_bits = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_bits,
                                                                           min_fidelity=0.9, max_time=0,
                                                                           purpose_id=2, priority=2,
                                                                           measure_directly=True)

        alice_scheduled_epr = partial(egpA.create, cqc_request_raw=alice_request_epr)
        alice_scheduled_bits = partial(egpA.create, cqc_request_raw=alice_request_bits)
        bob_scheduled_epr = partial(egpB.create, cqc_request_raw=bob_request_epr)
        bob_scheduled_bits = partial(egpB.create, cqc_request_raw=bob_request_bits)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=alice_scheduled_epr, t=0)
        sim_scheduler.schedule_function(func=bob_scheduled_bits, t=2.5)
        sim_scheduler.schedule_function(func=bob_scheduled_epr, t=5)
        sim_scheduler.schedule_function(func=alice_scheduled_bits, t=7.5)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(11000)

        self.assertEqual(len(self.alice_results), alice_num_bits + bob_num_bits + alice_num_pairs + bob_num_pairs)
        for resA, resB in zip(self.alice_results, self.bob_results):
            self.assertEqual(resA[0], resB[0])
            if resA[0] == EntInfoCreateKeepHeader.type:
                continue
            _, a_create, a_id, a_m, a_basis, _, a_t = resA
            _, b_create, b_id, b_m, b_basis, _, b_t = resB
            self.assertEqual(a_create, b_create)
            self.assertEqual(a_id, b_id)
            self.assertEqual(a_t, b_t)

        # Check the entangled pairs, ignore communication qubit
        self.check_memories(alice.qmem, bob.qmem, range(alice_num_pairs + bob_num_pairs))

    def test_priority_mixed_requests(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True, num_priorities=4,
                                      strict_priority=True)

        # Schedule egp CREATE commands mid simulation
        alice_num_pairs = 1
        alice_num_bits = 20
        bob_num_pairs = 2
        bob_num_bits = 30
        alice_request_epr = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID,
                                                                            num_pairs=alice_num_pairs, min_fidelity=0.5,
                                                                            max_time=0, purpose_id=1, priority=0)

        bob_request_epr = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_pairs,
                                                                          min_fidelity=0.9, max_time=0,
                                                                          purpose_id=2, priority=1)

        alice_request_bits = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID,
                                                                             num_pairs=alice_num_bits, min_fidelity=0.5,
                                                                             max_time=0, purpose_id=1, priority=2,
                                                                             measure_directly=True)

        bob_request_bits = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_bits,
                                                                           min_fidelity=0.9, max_time=0,
                                                                           purpose_id=2, priority=3,
                                                                           measure_directly=True)

        # Create the requests such that lowest priority enters first but highest priorities complete in correct order
        create_id_bob_bits = egpB.create(cqc_request_raw=bob_request_bits)
        create_id_alice_bits = egpA.create(cqc_request_raw=alice_request_bits)
        create_id_bob_epr = egpB.create(cqc_request_raw=bob_request_epr)
        create_id_alice_epr = egpA.create(cqc_request_raw=alice_request_epr)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(20)
        self.assertEqual(len(self.alice_results), alice_num_bits + bob_num_bits + alice_num_pairs + bob_num_pairs)

        # Check the entangled pairs, ignore communication qubit
        self.check_memories(alice.qmem, bob.qmem, range(alice_num_pairs + bob_num_pairs))

        # We expect the OKs to come back in priority order
        expected_result_data = [(EntInfoCreateKeepHeader.type, create_id_alice_epr, (alice.nodeID, bob.nodeID, i))
                                for i in range(alice_num_pairs)]
        expected_result_data += [(EntInfoCreateKeepHeader.type, create_id_bob_epr, (bob.nodeID, alice.nodeID, i))
                                 for i in range(alice_num_pairs, alice_num_pairs + bob_num_pairs)]
        expected_result_data += [(EntInfoMeasDirectHeader.type, create_id_alice_bits, (alice.nodeID, bob.nodeID, i))
                                 for i in range(alice_num_pairs + bob_num_pairs,
                                                alice_num_pairs + bob_num_pairs + alice_num_bits)]

        for resA, resB, expected in zip(self.alice_results, self.bob_results, expected_result_data):
            self.assertEqual(resA[0:3], expected)
            self.assertEqual(resB[0:3], expected)

        # Need to use initial sequence number because measure directly may lose successful sequence numbers when
        # requests are completed
        mhp_seq_start = self.alice_results[-bob_num_bits][2][2]
        expected_result_data = [(EntInfoMeasDirectHeader.type, create_id_bob_bits, (bob.nodeID, alice.nodeID, i))
                                for i in range(mhp_seq_start, mhp_seq_start + bob_num_bits)]

        for resA, resB, expected in zip(self.alice_results[-bob_num_bits:], self.bob_results[-bob_num_bits:],
                                        expected_result_data):

            self.assertEqual(resA[0:3], expected)
            self.assertEqual(resB[0:3], expected)

    def test_mixed_fidelity(self):
        # Verify switching between bright state populations
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True, num_priorities=4,
                                      strict_priority=True)

        # Schedule egp CREATE commands mid simulation
        alice_num_pairs = 1
        alice_num_bits = 20
        bob_num_pairs = 2
        bob_num_bits = 30
        alice_request_epr = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID,
                                                                            num_pairs=alice_num_pairs,
                                                                            min_fidelity=0.7,
                                                                            max_time=0, purpose_id=1, priority=0)

        bob_request_epr = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID,
                                                                          num_pairs=bob_num_pairs,
                                                                          min_fidelity=0.9, max_time=0,
                                                                          purpose_id=2, priority=1)

        alice_request_bits = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID,
                                                                             num_pairs=alice_num_bits,
                                                                             min_fidelity=0.8,
                                                                             max_time=0, purpose_id=1, priority=2,
                                                                             measure_directly=True)

        bob_request_bits = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID,
                                                                           num_pairs=bob_num_bits,
                                                                           min_fidelity=0.6, max_time=0,
                                                                           purpose_id=2, priority=3,
                                                                           measure_directly=True)

        # Create the requests such that lowest priority enters first but highest priorities complete in correct order
        egpB.create(cqc_request_raw=bob_request_bits)
        create_id_alice_bits = egpA.create(cqc_request_raw=alice_request_bits)
        create_id_bob_epr = egpB.create(cqc_request_raw=bob_request_epr)
        create_id_alice_epr = egpA.create(cqc_request_raw=alice_request_epr)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(20)
        self.assertEqual(len(self.alice_results), alice_num_bits + bob_num_bits + alice_num_pairs + bob_num_pairs)

        # Check the entangled pairs, ignore communication qubit
        self.check_memories(alice.qmem, bob.qmem, range(alice_num_pairs + bob_num_pairs))

        # We expect the OKs to come back in priority order
        expected_result_data = [(EntInfoCreateKeepHeader.type, create_id_alice_epr, (alice.nodeID, bob.nodeID, i))
                                for i in range(alice_num_pairs)]
        expected_result_data += [(EntInfoCreateKeepHeader.type, create_id_bob_epr, (bob.nodeID, alice.nodeID, i))
                                 for i in range(alice_num_pairs, alice_num_pairs + bob_num_pairs)]
        expected_result_data += [(EntInfoMeasDirectHeader.type, create_id_alice_bits, (alice.nodeID, bob.nodeID, i))
                                 for i in range(alice_num_pairs + bob_num_pairs,
                                                alice_num_pairs + bob_num_pairs + alice_num_bits)]

        for resA, resB, expected in zip(self.alice_results, self.bob_results, expected_result_data):
            self.assertEqual(resA[0:3], expected)
            self.assertEqual(resB[0:3], expected)

        # Need to use initial sequence number because measure directly may lose successful sequence numbers when
        # requests are completed
        mhp_seq_start = self.alice_results[-bob_num_bits][2][2]
        expected_result_data = [(EntInfoMeasDirectHeader.type, create_id_alice_bits, (bob.nodeID, alice.nodeID, i))
                                for i in range(mhp_seq_start, mhp_seq_start + bob_num_bits)]

        for resA, resB, expected in zip(self.alice_results[-bob_num_bits:], self.bob_results[-bob_num_bits:],
                                        expected_result_data):
            self.assertEqual(resA[0:3], expected)
            self.assertEqual(resB[0:3], expected)

    def test_priority_overtake(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True, num_priorities=3,
                                      strict_priority=True)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_num_bits = 10
        bob_num_bits = 50
        bob_num_bits2 = 100

        # Construct a request with higher priority
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_num_bits,
                                                                        min_fidelity=0.9, max_time=0,
                                                                        purpose_id=1, priority=0,
                                                                        measure_directly=True)

        # Construct a request with lower priority
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_bits,
                                                                      min_fidelity=0.9, max_time=0,
                                                                      purpose_id=2, priority=1, measure_directly=True)

        # Construct a request with lower priority
        bob_request2 = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_bits2,
                                                                       min_fidelity=0.9, max_time=0,
                                                                       purpose_id=2, priority=2, measure_directly=True)

        alice_scheduled_create = partial(egpA.create, cqc_request_raw=alice_request)
        bob_scheduled_create = partial(egpB.create, cqc_request_raw=bob_request)
        bob_scheduled_create2 = partial(egpB.create, cqc_request_raw=bob_request2)

        # Schedule a sequence of various create requests
        # We issue the low priority request first so that it begins and then verify that issuing a higher priority
        # request afterwards will overtake the low priority request
        sim_scheduler.schedule_function(func=bob_scheduled_create2, t=0)
        sim_scheduler.schedule_function(func=bob_scheduled_create, t=2)
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=4)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(20)

        # Verify all the bits were generated
        self.assertEqual(len(self.alice_results), alice_num_bits + bob_num_bits + bob_num_bits2)
        for resA, resB in zip(self.alice_results, self.bob_results):
            self.assertEqual(resA[0:3], resB[0:3])
            if resA[1] == 1 and resA[2][0] == bob.nodeID:
                bob_num_bits2 -= 1
            elif resA[1] == 0 and resA[2][0] == bob.nodeID:
                bob_num_bits -= 1
            elif resA[1] == 0 and resA[2][0] == alice.nodeID:
                alice_num_bits -= 1

            if bob_num_bits2 == 0:
                self.assertEqual(bob_num_bits, 0)
                self.assertEqual(alice_num_bits, 0)

            if bob_num_bits == 0:
                self.assertEqual(alice_num_bits, 0)

    def test_atomic_requests(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_num_bits = 30
        bob_num_bits = 10

        # Construct a request with higher priority
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_num_bits,
                                                                        min_fidelity=0.9, max_time=0,
                                                                        purpose_id=1, priority=0,
                                                                        measure_directly=True)

        # Construct a request with lower priority but is atomic
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_num_bits,
                                                                      min_fidelity=0.9, max_time=0,
                                                                      purpose_id=2, priority=1, atomic=True,
                                                                      measure_directly=True)

        alice_scheduled_create = partial(egpA.create, cqc_request_raw=alice_request)
        bob_scheduled_create = partial(egpB.create, cqc_request_raw=bob_request)

        # Schedule a sequence of various create requests
        # We issue the low priority request first so that it begins and then verify that issuing a higher priority
        # request afterwards does not break up the OKs for the lower priority request
        sim_scheduler.schedule_function(func=bob_scheduled_create, t=0)
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=5)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(20)

        # Verify all the bits were generated
        self.assertEqual(len(self.alice_results), alice_num_bits + bob_num_bits)

        for resA, resB in zip(self.alice_results, self.bob_results):
            self.assertEqual(resA[0:3], resB[0:3])
            if resA[2][0] == bob.nodeID:
                bob_num_bits -= 1
            elif resA[2][0] == alice.nodeID:
                alice_num_bits -= 1

            if alice_num_bits == 0:
                self.assertEqual(bob_num_bits, 0)

    def test_manual_connect(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(alice, bob, connected=False, accept_all=True)

        egp_conn = ClassicalFibreConnection(nodeA=alice, nodeB=bob, length=0.1)
        dqp_conn = ClassicalFibreConnection(nodeA=alice, nodeB=bob, length=0.05)
        mhp_conn = NodeCentricMHPHeraldedConnection(nodeA=alice, nodeB=bob, lengthA=0.02, lengthB=0.03,
                                                    use_time_window=True)
        egpA.connect_to_peer_protocol(egpB, egp_conn=egp_conn, dqp_conn=dqp_conn, mhp_conn=mhp_conn)

        self.assertEqual(egpA.conn, egp_conn)
        self.assertEqual(egpB.conn, egp_conn)
        self.assertEqual(egpA.dqp.conn, dqp_conn)
        self.assertEqual(egpB.dqp.conn, dqp_conn)
        self.assertEqual(egpA.mhp.conn, mhp_conn)
        self.assertEqual(egpB.mhp.conn, mhp_conn)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_pairs = 1
        bob_pairs = 2
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0,
                                                                        purpose_id=1, priority=10)
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_pairs,
                                                                      min_fidelity=0.5, max_time=0,
                                                                      purpose_id=2, priority=2)

        alice_scheduled_create = partial(egpA.create, cqc_request_raw=alice_request)
        bob_scheduled_create = partial(egpB.create, cqc_request_raw=bob_request)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=0)
        sim_scheduler.schedule_function(func=bob_scheduled_create, t=5)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(12000)

        # Don't include t_goodness and t_create, since these could differ
        alice_results = list(map(lambda res: res[:-2], self.alice_results))
        bob_results = list(map(lambda res: res[:-2], self.bob_results))
        self.assertEqual(len(self.alice_results), alice_pairs + bob_pairs)
        self.assertEqual(alice_results, bob_results)

        # Check the entangled pairs, ignore communication qubit
        for resA, resB in zip(self.alice_results, self.bob_results):
            self.assertEqual(len(resA), len(resB))
            if len(resA) > 2:
                qA = alice.qmem.peek(resA[3])[0]
                qB = bob.qmem.peek(resB[3])[0]
                self.assertEqual(qA.qstate, qB.qstate)
                self.assertIn(qB, qA.qstate._qubits)
                self.assertIn(qA, qB.qstate._qubits)

    def test_rollover_mhp_cycle(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        alice_pairs = 1
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0,
                                                                        purpose_id=1, priority=10)

        offset = 5
        egpA.scheduler.mhp_cycle_number = egpA.scheduler.max_mhp_cycle_number - offset
        egpB.scheduler.mhp_cycle_number = egpB.scheduler.max_mhp_cycle_number - offset
        sched_time = egpA.scheduler.get_schedule_cycle(alice_request)
        self.assertGreaterEqual(sched_time, 0)
        self.assertLess(sched_time, egpA.scheduler.max_mhp_cycle_number)
        egpA.create(cqc_request_raw=alice_request)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(10)

        # Verify both nodes have all results
        self.assertEqual(len(self.alice_results), alice_pairs)
        self.assertEqual(self.alice_results, self.bob_results)

        # Check the entangled pairs, ignore communication qubit
        self.check_memories(alice.qmem, bob.qmem, range(alice_pairs))

    def test_unresponsive_dqp(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        num_requests = 3
        alice_requests = [EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=1,
                                                                          min_fidelity=0.5, max_time=0,
                                                                          purpose_id=1, priority=10)
                          for _ in range(num_requests)]

        # Schedule egp CREATE commands mid simulation to ensure timeout order when checking results
        sim_scheduler = SimulationScheduler()
        for t, request in enumerate(alice_requests):
            alice_scheduled_create = partial(egpA.create, cqc_request_raw=request)
            sim_scheduler.schedule_function(func=alice_scheduled_create, t=t)

        # Construct a network for the simulation
        nodes = [
            (alice, [egpA, egpA.dqp, egpA.mhp]),
            bob
        ]

        conns = [
            (egpA.dqp.conn, "dqp_conn", [egpA.dqp]),
            (egpA.conn, "egp_conn", [egpA, egpB]),
            (egpA.mhp.conn, "mhp_conn", [egpA.mhp, egpB.mhp])
        ]

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(10)

        expected_results = [(egpA.dqp.DQ_TIMEOUT, create_id) for create_id in range(len(alice_requests))]
        self.assertEqual(self.alice_results, expected_results)
        self.assertEqual(self.bob_results, [])

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_unresponsive_mhp(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # Schedule egp CREATE commands mid simulation
        max_time = 10
        raw_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=1,
                                                                      min_fidelity=0.5, max_time=max_time,
                                                                      purpose_id=1, priority=10)

        request = egpA._get_egp_request(raw_request)
        egpA.create(cqc_request_raw=raw_request)

        # Construct a network for the simulation
        nodes = [
            (alice, [egpA, egpA.dqp, egpA.mhp]),
            (bob, [egpB, egpB.dqp])
        ]

        conns = [
            (egpA.dqp.conn, "dqp_conn", [egpA.dqp, egpB.dqp]),
            (egpA.conn, "egp_conn", [egpA, egpB]),
            (egpA.mhp.conn, "mhp_conn", [egpA.mhp])
        ]

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()

        # Make the MHP at the peer unresponsive
        egpB.mhp.stop()

        # Get the start time of the generation attempts
        sched_cycle = egpA.scheduler.get_schedule_cycle(request)
        cycles_per_gen = ceil(egpA.mhp.conn.full_cycle / egpA.mhp.time_step)
        timeout_cycle = egpA.scheduler.get_timeout_cycle(request) - (cycles_per_gen - 1)
        sim_run(max_time + 1)

        # Calculate the amount of time a full generation cycle takes
        offset = (cycles_per_gen - (sched_cycle % cycles_per_gen)) % cycles_per_gen
        start_cycle = sched_cycle + offset

        # Calculate the number of errors we should have received
        num_timeouts = int((timeout_cycle - start_cycle) // cycles_per_gen)

        # Assert that there were a few entanglement attempts before timing out the request
        expected_err_mhp = egpA.mhp.conn.ERR_NO_CLASSICAL_OTHER
        expected_err_egp = egpA.ERR_TIMEOUT

        # Unresponsive error two times followed by a timeout of the request
        expected_results = [(expected_err_mhp, 0)] * num_timeouts + [(expected_err_egp, 0)]
        self.assertEqual(len(self.alice_results), len(expected_results))
        self.assertEqual(self.alice_results, expected_results)

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_unresponsive_egp(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=1)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=1,
                                                                        min_fidelity=0.5, max_time=100,
                                                                        purpose_id=1, priority=10)

        egpA.create(alice_request)

        # Construct a network for the simulation
        nodes = [
            (alice, [egpA.dqp, egpA.mhp]),
            (bob, [egpB.dqp, egpB.mhp])
        ]

        conns = [
            (egpA.dqp.conn, "dqp_conn", [egpA.dqp, egpB.dqp]),
            (egpA.mhp.conn, "mhp_conn", [egpA.mhp, egpB.mhp])
        ]

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(110)

        self.assertEqual(len(self.alice_results), 1)

        # Check that we got the correct error and that the requests are the same
        [(error, _)] = self.alice_results
        self.assertEqual(error, egpA.ERR_TIMEOUT)

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_egp_comm_loss(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        self.p_loss = 1

        def alice_faulty_request():
            if random.random() < self.p_loss:
                pass
            else:
                self.p_loss = max(self.p_loss - 0.1, 0)
                alice_free_mem = egpA.qmm.get_free_mem_ad()
                egpA.conn.put_from(alice.nodeID, [(egpA.CMD_REQ_E, alice_free_mem)])

        egpA.request_other_free_memory = alice_faulty_request

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_pairs = 3
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0,
                                                                        purpose_id=1, priority=10)

        alice_scheduled_create = partial(egpA.create, cqc_request_raw=alice_request)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=0)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(10)

        # Verify both nodes have all results
        self.assertEqual(len(self.alice_results), alice_pairs)
        self.assertEqual(self.alice_results, self.bob_results)

        # Check the entangled pairs, ignore communication qubit
        self.check_memories(alice.qmem, bob.qmem, range(alice_pairs))

    def test_request_timeout(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(alice, bob, connected=False, accept_all=True)

        egp_conn = ClassicalFibreConnection(nodeA=alice, nodeB=bob, length=0.1)
        dqp_conn = ClassicalFibreConnection(nodeA=alice, nodeB=bob, length=0.05)
        mhp_conn = NodeCentricMHPHeraldedConnection(nodeA=alice, nodeB=bob, lengthA=0.02, lengthB=0.03,
                                                    use_time_window=True)
        egpA.connect_to_peer_protocol(egpB, egp_conn=egp_conn, dqp_conn=dqp_conn, mhp_conn=mhp_conn)

        alice_pairs = 2
        bob_pairs = 2

        # Use a max time that is a multiple of the mhp timestep and allows the request to begin processing
        max_time = ceil(egpA.mhp.conn.full_cycle / egpA.mhp.time_step) * egpA.mhp.time_step * 5
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=max_time,
                                                                        purpose_id=1, priority=10)

        # This request should be completed successfully after the first one times out
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_pairs,
                                                                      min_fidelity=0.5, max_time=0,
                                                                      purpose_id=2, priority=2)

        alice_create_id = egpA.create(cqc_request_raw=alice_request)
        bob_create_id = egpB.create(cqc_request_raw=bob_request)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(10000)

        # There should be at least a timeout message and the two successful generations from bob's request
        self.assertGreaterEqual(len(self.alice_results), 3)
        self.assertEqual(len(self.alice_results), len(self.bob_results))

        # Check that the timeout message is in the results
        self.assertIn((egpA.ERR_TIMEOUT, alice_create_id), self.alice_results)

        # Check that the oks correspond to bob's request and worked
        for alice_ok, bob_ok in zip(self.alice_results[-2:], self.bob_results[-2:]):
            self.assertEqual(alice_ok[0], EntInfoCreateKeepHeader.type)
            self.assertEqual(alice_ok[0], bob_ok[0])
            self.assertEqual(alice_ok[1], bob_create_id)
            self.assertEqual(alice_ok[1], bob_ok[1])
            self.assertEqual(alice_ok[2], bob_ok[2])
            aID, bID = alice_ok[3], bob_ok[3]
            qA = alice.qmem.peek(aID)[0]
            qB = bob.qmem.peek(bID)[0]
            self.assertEqual(qA.qstate.dm.shape, (4, 4))
            self.assertTrue(qA.qstate.compare(qB.qstate))
            self.assertIn(qB, qA.qstate._qubits)
            self.assertIn(qA, qB.qstate._qubits)

    def test_one_node_expires(self):
        alice, bob = self.create_nodes(alice_device_positions=10, bob_device_positions=10)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # Make the heralding station "drop" message containing MHP Seq = 2 to nodeA
        def faulty_send(node, data, conn):
            logger.debug("Faulty send, MHP Seq {}".format(conn.mhp_seq))
            if node.nodeID == alice.nodeID:
                if not conn.mhp_seq == 1:
                    logger.debug("Sending to {}".format(node.nodeID))
                    conn.channel_M_to_A.send(data)

            elif node.nodeID == bob.nodeID:
                logger.debug("Sending to {}".format(node.nodeID))
                conn.channel_M_to_B.send(data)

        egpA.mhp.conn._send_to_node = partial(faulty_send, conn=egpA.mhp.conn)

        alice_pairs = 4
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0, purpose_id=1,
                                                                        priority=10)

        bob_pairs = 4
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_pairs,
                                                                      min_fidelity=0.5, max_time=0, purpose_id=1,
                                                                      priority=10)

        alice_create_id = egpA.create(alice_request)
        egpB.create(bob_request)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(20)

        # Check that we were able to get the first generation of alice's request completed
        self.assertEqual(self.alice_results[0], self.bob_results[0])

        # Check that alice only has an entanglement identifier for one of the pairs in her request
        alice_oks = list(filter(lambda info: len(info) == 7 and info[2][:2] == (alice.nodeID, bob.nodeID),
                                self.alice_results))
        self.assertEqual(len(alice_oks), 1)
        [ok_message] = alice_oks
        _, create_id, ent_id, logical_id, _, _, _ = ok_message
        self.assertEqual(create_id, alice_create_id)
        expected_mhp = 0

        # We ignore the logical id that the qubit was stored in
        self.assertEqual(ent_id, (alice.nodeID, bob.nodeID, expected_mhp))

        # Check that any additional entanglement identifiers bob's egp may have passed up were expired
        # Get the issued ok's containing entanglement identifiers corresponding to alice's create
        bob_oks = list(filter(lambda info: len(info) == 7 and info[2][:2] == (alice.nodeID, bob.nodeID),
                              self.bob_results))

        # Get the expiration message we received from alice
        expiry_messages = list(filter(lambda info: len(info) == 2 and info[0] == egpB.ERR_EXPIRE, self.bob_results))

        self.assertEqual(len(expiry_messages), 1)
        [expiry_message] = expiry_messages

        invalid_oks = set(bob_oks) - set(alice_oks)
        uncovered_ids = [ok_message[1:3] for ok_message in invalid_oks]

        expired_create_id, expired_origin_id = expiry_message[1]

        # Verify that all entanglement identifiers bob has that alice does not have are covered within the expiry
        for create_id, ent_id in uncovered_ids:
            self.assertEqual(create_id, expired_create_id)
            self.assertEqual(ent_id[0], expired_origin_id)

        # Check that we were able to resynchronize for bob's request
        # Get the gen ok's corresponding to bob's request after the error
        alice_gens_post_error = list(filter(lambda info: len(info) == 7 and info[2][:2] == (bob.nodeID, alice.nodeID),
                                            self.alice_results))
        bob_gens_post_error = list(filter(lambda info: len(info) == 7 and info[2][:2] == (bob.nodeID, alice.nodeID),
                                          self.bob_results))

        # Check that we were able to complete the request
        self.assertEqual(len(alice_gens_post_error), bob_pairs)
        self.assertEqual(len(alice_gens_post_error), len(bob_gens_post_error))

        # Check that the sequence numbers match
        for alice_gen, bob_gen in zip(alice_gens_post_error, bob_gens_post_error):
            self.assertEqual(alice_gen[2][2], bob_gen[2][2])
            qA = alice.qmem.peek(alice_gen[3])[0]
            qB = bob.qmem.peek(bob_gen[3])[0]
            self.assertEqual(qA.qstate.dm.shape, (4, 4))
            self.assertTrue(qA.qstate.compare(qB.qstate))
            self.assertIn(qB, qA.qstate._qubits)
            self.assertIn(qA, qB.qstate._qubits)

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

        # Verify that egp states synchronized
        self.assertEqual(egpA.mhp.conn.mhp_seq, egpA.expected_seq)
        self.assertEqual(egpA.expected_seq, egpB.expected_seq)

    def test_lost_last_success(self):
        alice, bob = self.create_nodes(alice_device_positions=10, bob_device_positions=10)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # Make the heralding station "drop" message containing MHP Seq = 2 to nodeA
        def faulty_send(node, data, conn):
            logger.debug("Faulty send, MHP Seq {}".format(conn.mhp_seq))
            if node.nodeID == alice.nodeID:
                if not conn.mhp_seq == 0 and not conn.mhp_seq == 2:
                    logger.debug("Sending to {}".format(node.nodeID))
                    conn.channel_M_to_A.send(data)

            elif node.nodeID == bob.nodeID:
                logger.debug("Sending to {}".format(node.nodeID))
                conn.channel_M_to_B.send(data)

        egpA.mhp.conn._send_to_node = partial(faulty_send, conn=egpA.mhp.conn)

        alice_pairs = 1
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0, purpose_id=1,
                                                                        priority=10)

        bob_pairs = 1
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_pairs,
                                                                      min_fidelity=0.5, max_time=0, purpose_id=1,
                                                                      priority=10)

        bob_request2 = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_pairs,
                                                                       min_fidelity=0.5, max_time=0, purpose_id=1,
                                                                       priority=10)

        create_idA = egpA.create(alice_request)
        create_idB = egpB.create(bob_request)
        create_idB2 = egpB.create(bob_request2)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(5)

        # Verify number of messages
        self.assertEqual(len(self.alice_results), 5)  # 2 errors, 2 expires, 1 ok
        self.assertEqual(len(self.bob_results), 6)    # 2 errors, 2 expires, 3 ok's

        # Verify that alice received an ERR_QUEUE_MISMATCH
        self.assertEqual(self.alice_results[0], (egpA.mhp.conn.ERR_QUEUE_MISMATCH, 0))
        self.assertEqual(self.bob_results[1], (egpB.mhp.conn.ERR_QUEUE_MISMATCH, 0))

        # Verify that an expire message propagated to both nodes
        self.assertEqual(self.alice_results[1], (egpA.ERR_EXPIRE, (create_idA, alice.nodeID)))
        self.assertEqual(self.bob_results[2], (egpB.ERR_EXPIRE, (create_idA, alice.nodeID)))

        # Verify that bobs first request was successfully completed
        expected_okay_data = (EntInfoCreateKeepHeader.type, create_idB, (bob.nodeID, alice.nodeID, 1))
        self.assertEqual(self.alice_results[2][0:3], expected_okay_data)
        self.assertEqual(self.bob_results[3][0:3], expected_okay_data)

        # Verify that alice received an ERR_NO_CLASSICAL_OTHER
        self.assertEqual(self.alice_results[3], (egpA.mhp.conn.ERR_NO_CLASSICAL_OTHER, 0))

        # Verify that expire message propagated to both nodes
        self.assertEqual(self.alice_results[4], (egpA.ERR_EXPIRE, (create_idB2, bob.nodeID)))
        self.assertEqual(self.bob_results[5], (egpB.ERR_EXPIRE, (create_idB2, bob.nodeID)))

        # Verify that egp states synchronized
        self.assertEqual(egpA.mhp.conn.mhp_seq, egpA.expected_seq)
        self.assertEqual(egpA.expected_seq, egpB.expected_seq)

    def test_both_nodes_expire(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)

        # Force the connection to "accidentally" increment MHP seq too much
        self.inc_sequence = [2, 3, 4, 5, 6]

        def bad_inc():
            return self.inc_sequence.pop(0)

        # Set up EGP
        egpA, egpB = self.create_egps(alice, bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        egpA.mhp.conn._get_next_mhp_seq = bad_inc
        alice_pairs = 3
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0,
                                                                        purpose_id=1, priority=10)

        create_id = egpA.create(cqc_request_raw=alice_request)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(10)

        # Verify that when both detect MHP Sequence number skip then results are the same
        self.assertEqual(len(self.alice_results), 2)
        self.assertEqual(self.alice_results, self.bob_results)
        self.assertEqual(self.alice_results[0][2][:3], (alice.nodeID, bob.nodeID, 0))

        # Verify that first create was successful
        idA = self.alice_results[0][3]
        idB = self.bob_results[0][3]
        qA = alice.qmem.peek(idA)[0]
        qB = bob.qmem.peek(idB)[0]
        self.assertEqual(qA.qstate.dm.shape, (4, 4))
        self.assertTrue(qA.qstate.compare(qB.qstate))
        self.assertIn(qB, qA.qstate._qubits)
        self.assertIn(qA, qB.qstate._qubits)

        # Verify we have ERR_EXPIRE messages for individual generation requests
        expiry_message = self.alice_results[1]
        error_code, _ = expiry_message
        expired_create_id, expired_origin_id = expiry_message[1]
        self.assertEqual(expired_create_id, create_id)
        self.assertEqual(expired_origin_id, alice.nodeID)
        self.assertEqual(error_code, egpA.ERR_EXPIRE)

        # Verify that egp states synchronized
        self.assertEqual(egpA.mhp.conn.mhp_seq, egpA.expected_seq)
        self.assertEqual(egpA.expected_seq, egpB.expected_seq)

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_lost_expire(self):
        alice, bob = self.create_nodes(alice_device_positions=10, bob_device_positions=10)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # Make the heralding station "drop" message containing MHP Seq = 2 to nodeA
        def faulty_send(node, data, conn):
            logger.debug("Faulty send, MHP Seq {}".format(conn.mhp_seq))
            if node.nodeID == alice.nodeID:
                if not conn.mhp_seq == 0 and not conn.mhp_seq == 2:
                    logger.debug("Sending to {}".format(node.nodeID))
                    conn.channel_M_to_A.send(data)

            elif node.nodeID == bob.nodeID:
                logger.debug("Sending to {}".format(node.nodeID))
                conn.channel_M_to_B.send(data)

        egpA.mhp.conn._send_to_node = partial(faulty_send, conn=egpA.mhp.conn)

        self.lost_messages = 0

        # Make the heralding station "drop" message containing MHP Seq = 2 to nodeA
        def faulty_send_expire(aid, createID, originID, new_seq):
            if self.lost_messages >= 2:
                egpA.conn.put_from(alice.nodeID, [(egpA.CMD_EXPIRE, (aid, createID, originID, new_seq))])
            else:
                self.lost_messages += 1

        egpA._send_exp_msg = faulty_send_expire

        alice_pairs = 1
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0, purpose_id=1,
                                                                        priority=10)

        create_idA = egpA.create(alice_request)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(5)
        self.assertEqual(len(self.alice_results), 2)
        self.assertEqual(len(self.bob_results), 2)

        # Check that the request completed for bob
        expected_ok_data = (EntInfoCreateKeepHeader.type, create_idA, (alice.nodeID, bob.nodeID, 0))
        self.assertEqual(self.bob_results[0][0:3], expected_ok_data)
        
        # Check that alice received an error from the midpoint and expired the request
        self.assertEqual(self.alice_results[0], (egpA.mhp.conn.ERR_NO_CLASSICAL_OTHER, 0))
        self.assertEqual(self.alice_results[1], (egpA.ERR_EXPIRE, (create_idA, alice.nodeID)))

        # Check that bob also received an error from the midpoint
        self.assertEqual(self.bob_results[1], (egpA.ERR_EXPIRE, (create_idA, alice.nodeID)))

        # Verify that egp states synchronized
        self.assertEqual(egpA.mhp.conn.mhp_seq, egpA.expected_seq)
        self.assertEqual(egpA.expected_seq, egpB.expected_seq)

    def test_lost_expire_ack(self):
        alice, bob = self.create_nodes(alice_device_positions=10, bob_device_positions=10)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # Make the heralding station "drop" message containing MHP Seq = 2 to nodeA
        def faulty_send(node, data, conn):
            logger.debug("Faulty send, MHP Seq {}".format(conn.mhp_seq))
            if node.nodeID == alice.nodeID:
                if not conn.mhp_seq == 0 and not conn.mhp_seq == 2:
                    logger.debug("Sending to {}".format(node.nodeID))
                    conn.channel_M_to_A.send(data)

            elif node.nodeID == bob.nodeID:
                logger.debug("Sending to {}".format(node.nodeID))
                conn.channel_M_to_B.send(data)

        egpA.mhp.conn._send_to_node = partial(faulty_send, conn=egpA.mhp.conn)

        self.lost_messages = 0

        # Make the heralding station "drop" message containing MHP Seq = 2 to nodeA
        def faulty_send_expire_ack(aid):
            if self.lost_messages >= 2:
                egpB.conn.put_from(bob.nodeID, [(egpB.CMD_EXPIRE_ACK, (aid, egpB.expected_seq))])
            else:
                self.lost_messages += 1

        egpB._send_exp_ack = faulty_send_expire_ack

        alice_pairs = 1
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0, purpose_id=1,
                                                                        priority=10)

        create_idA = egpA.create(alice_request)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        sim_run(5)
        self.assertEqual(len(self.alice_results), 2)
        self.assertEqual(len(self.bob_results), 4)

        # Check that the request completed for bob
        expected_ok_data = (EntInfoCreateKeepHeader.type, create_idA, (alice.nodeID, bob.nodeID, 0))
        self.assertEqual(self.bob_results[0][0:3], expected_ok_data)

        # Check that alice received an error from the midpoint and expired the request
        self.assertEqual(self.alice_results[0], (egpA.mhp.conn.ERR_NO_CLASSICAL_OTHER, 0))
        self.assertEqual(self.alice_results[1], (egpA.ERR_EXPIRE, (create_idA, alice.nodeID)))

        # Check that bob also received an error from the midpoint
        self.assertEqual(self.bob_results[1], (egpA.ERR_EXPIRE, (create_idA, alice.nodeID)))
        self.assertEqual(self.bob_results[2], (egpA.ERR_EXPIRE, (create_idA, alice.nodeID)))
        self.assertEqual(self.bob_results[3], (egpA.ERR_EXPIRE, (create_idA, alice.nodeID)))

        # Verify that egp states synchronized
        self.assertEqual(egpA.mhp.conn.mhp_seq, egpA.expected_seq)
        self.assertEqual(egpA.expected_seq, egpB.expected_seq)

    def test_creation_failure(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=True)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # EGP Request that requests entanglement with self
        node_self_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=5,
                                                                            min_fidelity=0.5, max_time=10, purpose_id=1,
                                                                            priority=10)

        # EGP Request that requests entanglement with unknown node
        unknown_id = 100
        node_unknown_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=unknown_id, num_pairs=5,
                                                                               min_fidelity=0.5, max_time=10,
                                                                               purpose_id=1, priority=10)

        # EGP Request that requets more fidelity than we
        unsuppfid_requet = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=1,
                                                                           min_fidelity=1, max_time=10, purpose_id=1,
                                                                           priority=10)

        # max_time that is too short for us to fulfill
        unsupptime_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=1,
                                                                             min_fidelity=0.5, max_time=1e-9,
                                                                             purpose_id=1, priority=10)

        egpA.create(cqc_request_raw=node_self_request)
        egpA.create(cqc_request_raw=node_unknown_request)
        egpA.create(cqc_request_raw=unsuppfid_requet)
        egpA.create(cqc_request_raw=unsupptime_request)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()
        sim_run(0.01)

        expected_results = [(NodeCentricEGP.ERR_CREATE, 0),
                            (NodeCentricEGP.ERR_CREATE, 0),
                            (NodeCentricEGP.ERR_UNSUPP, 0),
                            (NodeCentricEGP.ERR_UNSUPP, 0)]

        self.assertEqual(self.alice_results, expected_results)

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_queue_rules(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)
        egpA, egpB = self.create_egps(nodeA=alice, nodeB=bob, connected=True, accept_all=False)

        alice_purpose_id = 1
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=1,
                                                                        min_fidelity=0.5, max_time=0,
                                                                        purpose_id=alice_purpose_id, priority=10)

        bob_purpose_id = 2
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=1,
                                                                      min_fidelity=0.5, max_time=0,
                                                                      purpose_id=bob_purpose_id, priority=2)

        # Construct a network for the simulation
        network = self.create_network(egpA, egpB)
        network.start()

        # Verify neither alice nor bob can add requests
        expected_id = egpA.create(alice_request)
        sim_run(1)
        alice_expected = [(egpA.dqp.DQ_REJECT, expected_id)]
        self.assertEqual(self.alice_results, alice_expected)

        expected_id = egpB.create(bob_request)
        sim_run(2)
        bob_expected = [(egpB.dqp.DQ_REJECT, expected_id)]
        self.assertEqual(self.bob_results, bob_expected)

        # Verify alice can submit when bob accepts
        egpB.add_queue_rule(alice, alice_purpose_id)
        expected_id = egpA.create(alice_request)
        sim_run(5)
        self.assertEqual(len(self.alice_results), 2)
        self.assertEqual(len(self.alice_results), len(self.bob_results))
        _, create_id, alice_ent_id, _, _, _, _ = self.alice_results[-1]
        _, _, bob_ent_id, _, _, _, _ = self.bob_results[-1]
        self.assertEqual(create_id, expected_id)
        self.assertEqual(alice_ent_id, bob_ent_id)

        # Verify is still unable
        expected_id = egpB.create(bob_request)
        sim_run(7)
        self.assertEqual(len(self.bob_results), 3)
        self.assertEqual(self.bob_results[-1], (egpB.dqp.DQ_REJECT, expected_id))

        # Add a rule to alice for bob
        egpA.add_queue_rule(bob, bob_purpose_id)
        expected_id = egpB.create(bob_request)
        sim_run(12)
        self.assertEqual(len(self.alice_results), 3)
        self.assertEqual(len(self.bob_results), 4)
        _, _, alice_ent_id, _, _, _, _ = self.alice_results[-1]
        _, create_id, bob_ent_id, _, _, _, _ = self.bob_results[-1]
        self.assertEqual(create_id, expected_id)
        self.assertEqual(alice_ent_id, bob_ent_id)

        # Remove a rule from alice
        egpA.remove_queue_rule(bob, bob_purpose_id)
        expected_id = egpB.create(bob_request)
        sim_run(13)
        self.assertEqual(len(self.bob_results), 5)
        self.assertEqual(self.bob_results[-1], (egpB.dqp.DQ_REJECT, expected_id))

        # Verify alice can submit bob accepts
        expected_id = egpA.create(alice_request)
        sim_run(18)
        self.assertEqual(len(self.alice_results), 4)
        self.assertEqual(len(self.bob_results), 6)
        _, create_id, alice_ent_id, _, _, _, _ = self.alice_results[-1]
        _, _, bob_ent_id, _, _, _, _ = self.bob_results[-1]
        self.assertEqual(create_id, expected_id)
        self.assertEqual(alice_ent_id, bob_ent_id)

        # Remove alice's rule from bob
        egpB.remove_queue_rule(alice, alice_purpose_id)
        expected_id = egpA.create(alice_request)
        sim_run(23)
        self.assertEqual(len(self.alice_results), 5)
        self.assertEqual(self.alice_results[-1], (egpA.dqp.DQ_REJECT, expected_id))

    def test_events(self):
        alice, bob = self.create_nodes(alice_device_positions=5, bob_device_positions=5)

        pm = PM_Controller()
        alice_ent_tester = PM_Test_Ent(name="AliceEntTester")
        bob_ent_tester = PM_Test_Ent(name="BobEntTester")
        alice_req_tester = PM_Test_Counter(name="AliceReqCounter")
        bob_req_tester = PM_Test_Counter(name="BobReqCounter")

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=alice_ent_tester.store_data,
                              accept_all_requests=True)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=bob_ent_tester.store_data,
                              accept_all_requests=True)
        egpA.connect_to_peer_protocol(egpB)

        pm.addEvent(source=egpA, evtType=egpA._EVT_ENT_COMPLETED, ds=alice_ent_tester)
        pm.addEvent(source=egpA, evtType=egpA._EVT_REQ_COMPLETED, ds=alice_req_tester)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ENT_COMPLETED, ds=bob_ent_tester)
        pm.addEvent(source=egpB, evtType=egpB._EVT_REQ_COMPLETED, ds=bob_req_tester)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_pairs = 1
        bob_pairs = 2
        alice_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=bob.nodeID, num_pairs=alice_pairs,
                                                                        min_fidelity=0.5, max_time=0,
                                                                        purpose_id=1, priority=10)
        bob_request = EGPSimulationScenario.construct_cqc_epr_request(otherID=alice.nodeID, num_pairs=bob_pairs,
                                                                      min_fidelity=0.5, max_time=0,
                                                                      purpose_id=2, priority=2)

        alice_scheduled_create = partial(egpA.create, cqc_request_raw=alice_request)
        bob_scheduled_create = partial(egpB.create, cqc_request_raw=bob_request)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=0)
        sim_scheduler.schedule_function(func=bob_scheduled_create, t=5)

        # Construct a network for the simulation
        nodes = [
            (alice, [egpA, egpA.dqp, egpA.mhp]),
            (bob, [egpB, egpB.dqp, egpB.mhp])
        ]

        conns = [
            (egpA.dqp.conn, "dqp_conn", [egpA.dqp, egpB.dqp]),
            (egpA.conn, "egp_conn", [egpA, egpB]),
            (egpA.mhp.conn, "mhp_conn", [egpA.mhp, egpB.mhp])
        ]

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        sim_run(400)

        # Verify that the pydynaa ent and req events were scheduled correctly
        self.assertEqual(len(alice_ent_tester.stored_data), alice_pairs + bob_pairs)
        self.assertEqual(len(bob_ent_tester.stored_data), alice_pairs + bob_pairs)
        self.assertEqual(alice_req_tester.num_tested_items, 2)
        self.assertEqual(bob_req_tester.num_tested_items, 2)
        self.assertTrue(alice_ent_tester.test_passed())
        self.assertTrue(alice_req_tester.test_passed())
        self.assertTrue(bob_ent_tester.test_passed())
        self.assertTrue(bob_req_tester.test_passed())

        alice_results = alice_ent_tester.stored_data
        bob_results = bob_ent_tester.stored_data
        self.assertEqual(alice_results, bob_results)

        # Check the entangled pairs, ignore communication qubit
        self.check_memories(alice.qmem, bob.qmem, range(alice_pairs + bob_pairs))


if __name__ == "__main__":
    unittest.main()
