import unittest
import netsquid as ns
from functools import partial
from math import ceil
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easynetwork import EasyNetwork
from easysquid.entanglementGenerator import NV_PairPreparation
from easysquid.puppetMaster import PM_Controller, PM_Test
from easysquid.qnode import QuantumNode
from easysquid.quantumMemoryDevice import QuantumProcessingDevice
from easysquid.toolbox import SimulationScheduler, create_logger
from netsquid import pydynaa
from qlinklayer.egp import NodeCentricEGP, EGPRequest
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection


logger = create_logger("logger")


def store_result(storage, result):
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
        create_id, ent_id, goodness, t_goodness, t_create = self.stored_data[-1]
        assert len(ent_id) == 4

        creator, peer, mhp_seq, logical_id = ent_id
        assert egp.node.nodeID == creator or egp.node.nodeID == peer

    def store_data(self, result):
        self.stored_data.append(result)


class PM_Test_Counter(PM_Test):
    def __init__(self, name):
        super(PM_Test_Counter, self).__init__(name=name)
        self.num_tested_items = 0

    def _test(self, event):
        self.num_tested_items += 1


class TestNodeCentricEGP(unittest.TestCase):
    def setUp(self):
        ns.set_qstate_formalism(ns.DM_FORMALISM)
        pydynaa.DynAASim().reset()
        self.alice_results = []
        self.bob_results = []
        self.alice_callback = partial(store_result, storage=self.alice_results)
        self.bob_callback = partial(store_result, storage=self.bob_results)

    def test_create(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

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
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=alice_pairs, min_fidelity=0.5, max_time=1000,
                                   purpose_id=1, priority=10)
        bob_request = EGPRequest(otherID=alice.nodeID, num_pairs=bob_pairs, min_fidelity=0.5, max_time=2000,
                                 purpose_id=2, priority=2)

        # Schedule a sequence of various create requests
        alice_create_id, alice_create_time = egpA.create(alice_request)
        bob_create_id, bob_create_time = egpB.create(bob_request)

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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()

        pydynaa.DynAASim().run(1000)

        self.assertEqual(len(self.alice_results), alice_pairs + bob_pairs)
        self.assertEqual(self.alice_results, self.bob_results)

        create_id, ent_id, _, _, _ = self.alice_results[0]
        self.assertEqual(create_id, alice_create_id)
        self.assertEqual(ent_id, (alice.nodeID, bob.nodeID, 0, 1))

        create_id, ent_id, _, _, _ = self.alice_results[1]
        self.assertEqual(create_id, bob_create_id)
        self.assertEqual(ent_id, (bob.nodeID, alice.nodeID, 1, 2))

        create_id, ent_id, _, _, _ = self.alice_results[2]
        self.assertEqual(create_id, bob_create_id)
        self.assertEqual(ent_id, (bob.nodeID, alice.nodeID, 2, 3))

        # Check the entangled pairs, ignore communication qubit
        for i in range(alice_pairs + bob_pairs):
            qA = aliceMemory.get_qubit(i + 1)
            qB = bobMemory.get_qubit(i + 1)
            self.assertEqual(qA.qstate, qB.qstate)

        # Verify that the pydynaa create events were scheduled correctly
        self.assertTrue(alice_create_counter.test_passed())
        self.assertTrue(bob_create_counter.test_passed())
        self.assertEqual(alice_create_counter.num_tested_items, 1)
        self.assertEqual(bob_create_counter.num_tested_items, 1)
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_multi_create(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

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
        alice_requests = [EGPRequest(otherID=bob.nodeID, num_pairs=alice_pairs, min_fidelity=0.5, max_time=1000,
                                     purpose_id=1, priority=10) for r in range(num_requests)]

        # Schedule a sequence of various create requests
        alice_create_info = []
        for request in alice_requests:
            alice_create_id, alice_create_time = egpA.create(request)
            alice_create_info.append(alice_create_id)

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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()

        pydynaa.DynAASim().run(1000)

        self.assertEqual(len(self.alice_results), num_requests)
        self.assertEqual(self.alice_results, self.bob_results)

        # Verify that the create id incremented for each call and was tracked for each request
        for create_id, result in zip(alice_create_info, self.alice_results):
            stored_id, ent_id, _, _, _ = result
            self.assertEqual(stored_id, create_id)

        # Verify that the pydynaa create events were scheduled correctly
        self.assertTrue(alice_create_counter.test_passed())
        self.assertTrue(bob_create_counter.test_passed())
        self.assertEqual(alice_create_counter.num_tested_items, num_requests)
        self.assertEqual(bob_create_counter.num_tested_items, 0)
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_successful_simulation(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_pairs = 1
        bob_pairs = 2
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=alice_pairs, min_fidelity=0.5, max_time=1000,
                                   purpose_id=1, priority=10)
        bob_request = EGPRequest(otherID=alice.nodeID, num_pairs=bob_pairs, min_fidelity=0.5, max_time=2000,
                                 purpose_id=2, priority=2)

        alice_scheduled_create = partial(egpA.create, creq=alice_request)
        bob_scheduled_create = partial(egpB.create, creq=bob_request)

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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        pydynaa.DynAASim().run(400)

        self.assertEqual(len(self.alice_results), alice_pairs + bob_pairs)
        self.assertEqual(self.alice_results, self.bob_results)

        # Check the entangled pairs, ignore communication qubit
        for i in range(alice_pairs + bob_pairs):
            qA = aliceMemory.get_qubit(i + 1)
            qB = bobMemory.get_qubit(i + 1)
            self.assertEqual(qA.qstate, qB.qstate)

    def test_manual_connect(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)

        egp_conn = ClassicalFibreConnection(nodeA=alice, nodeB=bob, length=0.1)
        dqp_conn = ClassicalFibreConnection(nodeA=alice, nodeB=bob, length=0.2)
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
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=alice_pairs, min_fidelity=0.5, max_time=10000,
                                   purpose_id=1, priority=10)
        bob_request = EGPRequest(otherID=alice.nodeID, num_pairs=bob_pairs, min_fidelity=0.5, max_time=20000,
                                 purpose_id=2, priority=2)

        alice_scheduled_create = partial(egpA.create, creq=alice_request)
        bob_scheduled_create = partial(egpB.create, creq=bob_request)

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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        pydynaa.DynAASim().run(20000)

        self.assertEqual(len(self.alice_results), alice_pairs + bob_pairs)
        self.assertEqual(self.alice_results, self.bob_results)

        # Check the entangled pairs, ignore communication qubit
        for i in range(alice_pairs + bob_pairs):
            qA = aliceMemory.get_qubit(i + 1)
            qB = bobMemory.get_qubit(i + 1)
            self.assertEqual(qA.qstate, qB.qstate)

    def test_unresponsive_dqp(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        num_requests = 3
        alice_requests = [EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=0.5, max_time=1000,
                                     purpose_id=1, priority=10) for _ in range(num_requests)]

        # Schedule egp CREATE commands mid simulation to ensure timeout order when checking results
        sim_scheduler = SimulationScheduler()
        for t, request in enumerate(alice_requests):
            alice_scheduled_create = partial(egpA.create, creq=request)
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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        pydynaa.DynAASim().run(10)

        expected_results = [(egpA.ERR_NOTIME, req) for req in alice_requests]
        self.assertEqual(self.alice_results, expected_results)
        self.assertEqual(self.bob_results, [])

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_unresponsive_mhp(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        max_time = 200
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=0.5, max_time=max_time,
                                   purpose_id=1, priority=10)

        alice_scheduled_create = partial(egpA.create, creq=alice_request)

        # Schedule a sequence of various create requests
        t0 = 0
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=t0)

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

        egpA.mhp.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()

        pydynaa.DynAASim().run(max_time + 2)

        dqp_delay = egpA.dqp.conn.channel_from_A.compute_delay() + egpA.dqp.conn.channel_from_B.compute_delay()
        egp_delay = egpA.conn.channel_from_A.compute_delay() + egpA.conn.channel_from_B.compute_delay()
        mhp_delay = max(dqp_delay, egp_delay)
        mhp_start = egpA.mhp.timeStep * ceil(mhp_delay / egpA.mhp.timeStep)

        num_timeouts = int((max_time - mhp_start) // egpA.mhp.timeStep)

        # Assert that there were a few entanglement attempts before timing out the request
        expected_err_mhp = egpA.mhp.conn.ERR_NO_CLASSICAL_OTHER
        expected_err_egp = egpA.ERR_TIMEOUT

        # Unresponsive error two times followed by a timeout of the request
        expected_results = [(expected_err_mhp, None)] * num_timeouts + [(expected_err_egp, alice_request)]

        self.assertEqual(len(self.alice_results), len(expected_results))
        self.assertEqual(self.alice_results[:num_timeouts], expected_results[:num_timeouts])

        err, request = self.alice_results[-1]
        self.assertEqual(err, expected_err_egp)
        self.assertEqual(request.create_time, alice_request.create_time)

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_unresponsive_egp(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=1, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=0.5, max_time=100,
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

        egpA.mhp_service.start()
        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        pydynaa.DynAASim().run(160)

        self.assertEqual(len(self.alice_results), 1)
        self.assertEqual(self.alice_results, [(egpA.ERR_TIMEOUT, alice_request)])

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_one_node_expires(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=10, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=10, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

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
                    conn.channel_M_to_A.put(data)

            elif node.nodeID == bob.nodeID:
                logger.debug("Sending to {}".format(node.nodeID))
                conn.channel_M_to_B.put(data)

        egpA.mhp.conn._send_to_node = partial(faulty_send, conn=egpA.mhp.conn)

        alice_pairs = 4
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=alice_pairs, min_fidelity=0.5, max_time=1000,
                                   purpose_id=1, priority=10)

        bob_pairs = 4
        bob_request = EGPRequest(otherID=alice.nodeID, num_pairs=bob_pairs, min_fidelity=0.5, max_time=1000,
                                 purpose_id=1, priority=10)

        alice_create_id, create_time = egpA.create(alice_request)
        egpB.create(bob_request)

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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        pydynaa.DynAASim().run(40)

        # Check that we were able to get the first generation of alice's request completed
        self.assertEqual(self.alice_results[0], self.bob_results[0])

        # Check that alice only has an entanglement identifier for one of the pairs in her request
        alice_oks = list(filter(lambda info: len(info) == 5 and info[1][:2] == (alice.nodeID, bob.nodeID),
                                self.alice_results))
        self.assertEqual(len(alice_oks), 1)
        [ok_message] = alice_oks
        create_id, ent_id, _, _, _ = ok_message
        self.assertEqual(create_id, alice_create_id)
        expected_mhp = 0

        # We ignore the logical id that the qubit was stored in
        self.assertEqual(ent_id[:3], (alice.nodeID, bob.nodeID, expected_mhp))

        # Check that any additional entanglement identifiers bob's egp may have passed up were expired
        # Get the issued ok's containing entanglement identifiers corresponding to alice's create
        bob_oks = list(filter(lambda info: len(info) == 5 and info[1][:2] == (alice.nodeID, bob.nodeID),
                              self.bob_results))

        # Get the expiration message we received from alice
        expiry_messages = list(filter(lambda info: len(info) == 2 and info[0] == egpB.ERR_EXPIRE, self.bob_results))

        self.assertEqual(len(expiry_messages), 1)
        [expiry_message] = expiry_messages

        invalid_oks = set(bob_oks) - set(alice_oks)
        uncovered_ids = [ok_message[1] for ok_message in invalid_oks]
        expired_ent_ids = expiry_message[1]

        # Verify that all entanglement identifiers bob has that alice does not have are covered within the expiry
        for ent_id in uncovered_ids:
            self.assertIn(ent_id[:3], expired_ent_ids)

        # Check that we were able to resynchronize for bob's request
        self.assertEqual(self.alice_results[-bob_pairs:], self.bob_results[-bob_pairs:])

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_both_nodes_expire(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Force the connection to "accidentally" increment MHP seq too much
        self.inc_sequence = [2, 3, 4, 5, 6]

        def bad_inc():
            return self.inc_sequence.pop(0)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        egpA.mhp.conn._get_next_mhp_seq = bad_inc
        alice_pairs = 3
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=alice_pairs, min_fidelity=0.5, max_time=1000,
                                   purpose_id=1, priority=10)

        egpA.create(creq=alice_request)

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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        pydynaa.DynAASim().run(20)

        # Verify that when both detect MHP Sequence number skip then results are the same
        self.assertEqual(len(self.alice_results), 2)
        self.assertEqual(self.alice_results, self.bob_results)
        self.assertEqual(self.alice_results[0][1][:3], (alice.nodeID, bob.nodeID, 0))

        # Verify that first create was successful
        idA = self.alice_results[0][1][3]
        idB = self.bob_results[0][1][3]
        qA = aliceMemory.get_qubit(idA)
        qB = bobMemory.get_qubit(idB)
        self.assertEqual(qA.qstate, qB.qstate)

        # Verify we have ERR_EXPIRE messages for individual generation requests
        expiry_message = self.alice_results[1]
        error_code, expired_ids = expiry_message
        self.assertEqual(error_code, egpA.ERR_EXPIRE)
        expected_ids = [(alice.nodeID, bob.nodeID, 1), (alice.nodeID, bob.nodeID, 2)]
        self.assertEqual(expected_ids, expired_ids)

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_creation_failure(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=self.alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=self.bob_callback)
        egpA.connect_to_peer_protocol(egpB)

        pm = PM_Controller()
        alice_error_counter = PM_Test_Counter(name="AliceErrorCounter")
        bob_error_counter = PM_Test_Counter(name="BobErrorCounter")
        pm.addEvent(source=egpA, evtType=egpA._EVT_ERROR, ds=alice_error_counter)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ERROR, ds=bob_error_counter)

        # EGP Request that requests entanglement with self
        node_self_request = EGPRequest(otherID=alice.nodeID, num_pairs=5, min_fidelity=0.5, max_time=10, purpose_id=1,
                                       priority=10)

        # EGP Request that requests entanglement with unknown node
        unknown_id = 100
        node_unknown_request = EGPRequest(otherID=unknown_id, num_pairs=5, min_fidelity=0.5, max_time=10, purpose_id=1,
                                          priority=10)

        # EGP Request that requests more pairs than memory can contain
        noresmem_request = EGPRequest(otherID=bob.nodeID, num_pairs=5, min_fidelity=0.5, max_time=10, purpose_id=1,
                                      priority=10)

        # EGP Request that requets more fidelity than we
        unsuppfid_requet = EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=1, max_time=10, purpose_id=1,
                                      priority=10)

        # max_time that is too short for us to fulfill
        unsupptime_request = EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=0.5, max_time=0, purpose_id=1,
                                        priority=10)

        egpA.create(creq=node_self_request)
        egpA.create(creq=node_unknown_request)
        egpA.create(creq=noresmem_request)
        egpA.create(creq=unsuppfid_requet)
        egpA.create(creq=unsupptime_request)

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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        pydynaa.DynAASim().run(0.01)

        expected_results = [(NodeCentricEGP.ERR_CREATE, node_self_request),
                            (NodeCentricEGP.ERR_CREATE, node_unknown_request),
                            (NodeCentricEGP.ERR_NORES, noresmem_request),
                            (NodeCentricEGP.ERR_UNSUPP, unsuppfid_requet),
                            (NodeCentricEGP.ERR_UNSUPP, unsupptime_request)]

        self.assertEqual(self.alice_results, expected_results)

        # Verify that events were tracked
        self.assertEqual(alice_error_counter.num_tested_items, count_errors(self.alice_results))
        self.assertEqual(bob_error_counter.num_tested_items, count_errors(self.bob_results))

    def test_events(self):
        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        pm = PM_Controller()
        alice_ent_tester = PM_Test_Ent(name="AliceEntTester")
        bob_ent_tester = PM_Test_Ent(name="BobEntTester")
        alice_req_tester = PM_Test_Counter(name="AliceReqCounter")
        bob_req_tester = PM_Test_Counter(name="BobReqCounter")

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=self.alice_callback, ok_callback=alice_ent_tester.store_data)
        egpB = NodeCentricEGP(node=bob, err_callback=self.bob_callback, ok_callback=bob_ent_tester.store_data)
        egpA.connect_to_peer_protocol(egpB)

        pm.addEvent(source=egpA, evtType=egpA._EVT_ENT_COMPLETED, ds=alice_ent_tester)
        pm.addEvent(source=egpA, evtType=egpA._EVT_REQ_COMPLETED, ds=alice_req_tester)
        pm.addEvent(source=egpB, evtType=egpB._EVT_ENT_COMPLETED, ds=bob_ent_tester)
        pm.addEvent(source=egpB, evtType=egpB._EVT_REQ_COMPLETED, ds=bob_req_tester)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_pairs = 1
        bob_pairs = 2
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=alice_pairs, min_fidelity=0.5, max_time=1000,
                                   purpose_id=1, priority=10)
        bob_request = EGPRequest(otherID=alice.nodeID, num_pairs=bob_pairs, min_fidelity=0.5, max_time=2000,
                                 purpose_id=2, priority=2)

        alice_scheduled_create = partial(egpA.create, creq=alice_request)
        bob_scheduled_create = partial(egpB.create, creq=bob_request)

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

        egpA.mhp_service.start()

        network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
        network.start()
        pydynaa.DynAASim().run(400)

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
        for i in range(alice_pairs + bob_pairs):
            qA = aliceMemory.get_qubit(i + 1)
            qB = bobMemory.get_qubit(i + 1)
            self.assertEqual(qA.qstate, qB.qstate)


if __name__ == "__main__":
    unittest.main()
