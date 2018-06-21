import unittest
import netsquid as ns
from functools import partial
from easysquid.easynetwork import EasyNetwork
from easysquid.entanglementGenerator import NV_PairPreparation
from easysquid.qnode import QuantumNode
from easysquid.quantumMemoryDevice import QuantumProcessingDevice
from easysquid.toolbox import SimulationScheduler, create_logger
from netsquid import pydynaa
from qlinklayer.egp import NodeCentricEGP, EGPRequest


logger = create_logger("logger")


class TestNodeCentricEGP(unittest.TestCase):
    # def test_create(self):
    #     ns.set_qstate_formalism(ns.DM_FORMALISM)
    #     pydynaa.DynAASim().reset()
    #
    #     # Set up Alice
    #     aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
    #     alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)
    #
    #     # Set up Bob
    #     bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
    #     bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)
    #
    #     # Create callbacks for storing EGP results
    #     alice_results = []
    #     bob_results = []
    #
    #     def store_result(storage, result):
    #         storage.append(result)
    #
    #     alice_callback = partial(store_result, storage=alice_results)
    #     bob_callback = partial(store_result, storage=bob_results)
    #
    #     # Set up EGP
    #     egpA = NodeCentricEGP(node=alice, err_callback=alice_callback, ok_callback=alice_callback,
    #                           length_to_midpoint=0.002)
    #     egpB = NodeCentricEGP(node=bob, err_callback=bob_callback, ok_callback=bob_callback,
    #                           length_to_midpoint=0.003)
    #     egpA.connect_to_peer_protocol(egpB)
    #
    #     # Schedule egp CREATE commands mid simulation
    #     alice_pairs = 1
    #     bob_pairs = 2
    #     alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=alice_pairs, min_fidelity=0.5, max_time=1000,
    #                                purpose_id=1, priority=10)
    #     bob_request = EGPRequest(otherID=alice.nodeID, num_pairs=bob_pairs, min_fidelity=0.5, max_time=2000,
    #                              purpose_id=2, priority=2)
    #
    #     # Schedule a sequence of various create requests
    #     alice_create_id, alice_create_time = egpA.create(alice_request)
    #     bob_create_id, bob_create_time = egpB.create(bob_request)
    #
    #     # Construct a network for the simulation
    #     nodes = [
    #         (alice, [egpA, egpA.dqp, egpA.mhp]),
    #         (bob, [egpB, egpB.dqp, egpB.mhp])
    #     ]
    #
    #     conns = [
    #         (egpA.dqp.conn, "dqp_conn", [egpA.dqp, egpB.dqp]),
    #         (egpA.conn, "egp_conn", [egpA, egpB]),
    #         (egpA.mhp.conn, "mhp_conn", [egpA.mhp, egpB.mhp])
    #     ]
    #
    #     egpA.mhp_service.start()
    #
    #     network = EasyNetwork(name="EGPNetwork", nodes=nodes, connections=conns)
    #     network.start()
    #     pydynaa.DynAASim().run(1000)
    #
    #
    #     self.assertEqual(len(alice_results), alice_pairs + bob_pairs)
    #     for resA, resB in zip(alice_results, bob_results):
    #         # Currently ignore the t_create/t_goodness due to delayed communications
    #         self.assertEqual(resA[:3], resB[:3])
    #
    #     create_id, ent_id, _, _, _ = alice_results[0]
    #     self.assertEqual(create_id, alice_create_id)
    #     self.assertEqual(ent_id, (alice.nodeID, bob.nodeID, 0, 1))
    #
    #     create_id, ent_id, _, _, _ = alice_results[1]
    #     self.assertEqual(create_id, bob_create_id)
    #     self.assertEqual(ent_id, (bob.nodeID, alice.nodeID, 1, 2))
    #
    #     create_id, ent_id, _, _, _ = alice_results[2]
    #     self.assertEqual(create_id, bob_create_id)
    #     self.assertEqual(ent_id, (bob.nodeID, alice.nodeID, 2, 3))
    #
    #     # Check the entangled pairs, ignore communication qubit
    #     for i in range(alice_pairs + bob_pairs):
    #         qA = aliceMemory.get_qubit(i + 1)
    #         qB = bobMemory.get_qubit(i + 1)
    #         self.assertEqual(qA.qstate, qB.qstate)

    def test_multi_create(self):
        ns.set_qstate_formalism(ns.DM_FORMALISM)
        pydynaa.DynAASim().reset()

        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Create callbacks for storing EGP results
        alice_results = []
        bob_results = []

        def store_result(storage, result):
            storage.append(result)

        alice_callback = partial(store_result, storage=alice_results)
        bob_callback = partial(store_result, storage=bob_results)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=alice_callback, ok_callback=alice_callback,
                              length_to_midpoint=0.002)
        egpB = NodeCentricEGP(node=bob, err_callback=bob_callback, ok_callback=bob_callback,
                              length_to_midpoint=0.003)
        egpA.connect_to_peer_protocol(egpB)

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

        self.assertEqual(len(alice_results), num_requests)
        for resA, resB in zip(alice_results, bob_results):
            # Currently ignore the t_create/t_goodness due to delayed communications
            self.assertEqual(resA[:3], resB[:3])

        # Verify that the create id incremented for each call and was tracked for each request
        for create_id, result in zip(alice_create_info, alice_results):
            stored_id, ent_id, _, _, _ = result
            self.assertEqual(stored_id, create_id)

    def test_successful_simulation(self):
        ns.set_qstate_formalism(ns.DM_FORMALISM)
        pydynaa.DynAASim().reset()

        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Create callbacks for storing EGP results
        alice_results = []
        bob_results = []

        def store_result(storage, result):
            storage.append(result)

        alice_callback = partial(store_result, storage=alice_results)
        bob_callback = partial(store_result, storage=bob_results)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=alice_callback, ok_callback=alice_callback,
                              length_to_midpoint=0.002)
        egpB = NodeCentricEGP(node=bob, err_callback=bob_callback, ok_callback=bob_callback,
                              length_to_midpoint=0.003)
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

        self.assertEqual(len(alice_results), alice_pairs + bob_pairs)
        for resA, resB in zip(alice_results, bob_results):
            # Currently ignore the t_create/t_goodness due to delayed communications
            self.assertEqual(resA[:2], resB[:2])

        # Check the entangled pairs, ignore communication qubit
        for i in range(alice_pairs + bob_pairs):
            qA = aliceMemory.get_qubit(i + 1)
            qB = bobMemory.get_qubit(i + 1)
            self.assertEqual(qA.qstate, qB.qstate)

    def test_unresponsive_dqp(self):
        ns.set_qstate_formalism(ns.DM_FORMALISM)
        pydynaa.DynAASim().reset()

        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Create callbacks for storing EGP results
        alice_results = []
        bob_results = []

        def store_result(storage, result):
            storage.append(result)

        alice_callback = partial(store_result, storage=alice_results)
        bob_callback = partial(store_result, storage=bob_results)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=alice_callback, ok_callback=alice_callback,
                              length_to_midpoint=0.002)
        egpB = NodeCentricEGP(node=bob, err_callback=bob_callback, ok_callback=bob_callback,
                              length_to_midpoint=0.003)
        egpA.connect_to_peer_protocol(egpB)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=0.5, max_time=1000,
                                   purpose_id=1, priority=10)

        alice_scheduled_create = partial(egpA.create, creq=alice_request)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=0)

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
        pydynaa.DynAASim().run(1000)

        self.assertEqual(len(alice_results), 0)
        self.assertEqual(alice_results, bob_results)

    def test_unresponsive_mhp(self):
        ns.set_qstate_formalism(ns.DM_FORMALISM)
        pydynaa.DynAASim().reset()

        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Create callbacks for storing EGP results
        alice_results = []
        bob_results = []

        def store_result(storage, result):
            storage.append(result)

        alice_callback = partial(store_result, storage=alice_results)
        bob_callback = partial(store_result, storage=bob_results)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=alice_callback, ok_callback=alice_callback,
                              length_to_midpoint=0.002)
        egpB = NodeCentricEGP(node=bob, err_callback=bob_callback, ok_callback=bob_callback,
                              length_to_midpoint=0.003)
        egpA.connect_to_peer_protocol(egpB)

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

        pydynaa.DynAASim().run(340)

        # Assert that there were a few entanglement attempts before timing out the request
        expected_err_mhp = egpA.mhp.conn.ERR_NO_CLASSICAL_OTHER
        expected_err_egp = egpA.ERR_TIMEOUT

        # Unresponsive error two times followed by a timeout of the request
        expected_results = [(expected_err_mhp, None), (expected_err_mhp, None), (expected_err_egp, alice_request)]
        self.assertEqual(len(alice_results), 3)
        self.assertEqual(alice_results[0:2], expected_results[0:2])

        err, request = alice_results[2]
        self.assertEqual(err, expected_err_egp)
        self.assertEqual(request.create_time, alice_request.create_time)

    def test_unresponsive_egp(self):
        ns.set_qstate_formalism(ns.DM_FORMALISM)
        pydynaa.DynAASim().reset()

        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=1, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Create callbacks for storing EGP results
        alice_results = []
        bob_results = []

        def store_result(storage, result):
            storage.append(result)

        alice_callback = partial(store_result, storage=alice_results)
        bob_callback = partial(store_result, storage=bob_results)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=alice_callback, ok_callback=alice_callback,
                              length_to_midpoint=0.002)
        egpB = NodeCentricEGP(node=bob, err_callback=bob_callback, ok_callback=bob_callback,
                              length_to_midpoint=0.003)
        egpA.connect_to_peer_protocol(egpB)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()
        alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=0.5, max_time=100,
                                   purpose_id=1, priority=10)

        alice_scheduled_create = partial(egpA.create, creq=alice_request)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=alice_scheduled_create, t=0)

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

        self.assertEqual(len(alice_results), 1)
        self.assertEqual(alice_results, [(egpA.ERR_TIMEOUT, alice_request)])

    def test_creation_failure(self):
        ns.set_qstate_formalism(ns.DM_FORMALISM)
        pydynaa.DynAASim().reset()

        # Set up Alice
        aliceMemory = QuantumProcessingDevice(name="AliceMem", max_num=5, pair_preparation=NV_PairPreparation())
        alice = QuantumNode(name="Alice", nodeID=1, memDevice=aliceMemory)

        # Set up Bob
        bobMemory = QuantumProcessingDevice(name="BobMem", max_num=5, pair_preparation=NV_PairPreparation())
        bob = QuantumNode(name="Bob", nodeID=2, memDevice=bobMemory)

        # Create callbacks for storing EGP results
        alice_results = []
        bob_results = []

        def store_result(storage, result):
            storage.append(result)

        alice_callback = partial(store_result, storage=alice_results)
        bob_callback = partial(store_result, storage=bob_results)

        # Set up EGP
        egpA = NodeCentricEGP(node=alice, err_callback=alice_callback, ok_callback=alice_callback)
        egpB = NodeCentricEGP(node=bob, err_callback=bob_callback, ok_callback=bob_callback)
        egpA.connect_to_peer_protocol(egpB)

        # Schedule egp CREATE commands mid simulation
        sim_scheduler = SimulationScheduler()

        # EGP Request that requests more pairs than memory can contain
        noresmem_request = EGPRequest(otherID=bob.nodeID, num_pairs=5, min_fidelity=0.5, max_time=10, purpose_id=1,
                                      priority=10)

        # EGP Request that requets more fidelity than we
        unsuppfid_requet = EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=1, max_time=10, purpose_id=1,
                                      priority=10)

        # max_time that is too short for us to fulfill
        unsupptime_request = EGPRequest(otherID=bob.nodeID, num_pairs=1, min_fidelity=0.5, max_time=0, purpose_id=1,
                                        priority=10)

        noresmem_scheduled_create = partial(egpA.create, creq=noresmem_request)
        unsuppfid_scheduled_create = partial(egpA.create, creq=unsuppfid_requet)
        unsupptime_scheduled_create = partial(egpA.create, creq=unsupptime_request)

        # Schedule a sequence of various create requests
        sim_scheduler.schedule_function(func=noresmem_scheduled_create, t=0)
        sim_scheduler.schedule_function(func=unsuppfid_scheduled_create, t=0)
        sim_scheduler.schedule_function(func=unsupptime_scheduled_create, t=0)

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

        expected_results = [(NodeCentricEGP.ERR_NORES, None),
                            (NodeCentricEGP.ERR_UNSUPP, None),
                            (NodeCentricEGP.ERR_UNSUPP, None)]

        self.assertEqual(alice_results, expected_results)


if __name__ == "__main__":
    unittest.main()
