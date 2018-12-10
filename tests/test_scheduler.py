import unittest
import logging
from util.config_paths import ConfigPathStorage
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easynetwork import EasyNetwork, setup_physical_network
from easysquid.qnode import QuantumNode
from easysquid.quantumMemoryDevice import NVCommunicationDevice
from easysquid.easyprotocol import TimedProtocol
from easysquid.toolbox import logger
from easysquid.quantumMemoryDevice import NVCommunicationDevice
from netsquid.simutil import sim_run, sim_reset
from qlinklayer.distQueue import EGPDistributedQueue, WFQDistributedQueue
from qlinklayer.scheduler import StrictPriorityRequestScheduler, WFQRequestScheduler, SchedulerRequest
from qlinklayer.qmm import QuantumMemoryManagement
from qlinklayer.egp import EGPRequest
from qlinklayer.scenario import EGPSimulationScenario
from qlinklayer.feu import SingleClickFidelityEstimationUnit
from qlinklayer.mhp import SimulatedNodeCentricMHPService

logger.setLevel(logging.CRITICAL)


class IncreasMHPCycleProtocol(TimedProtocol):
    def __init__(self, timeStep=10, t0=0, scheduler=None):
        super().__init__(timeStep=timeStep, t0=t0)
        self.scheduler = scheduler

    def run_protocol(self):
        self.scheduler.inc_cycle()


class TestRequestScheduler(unittest.TestCase):
    def setUp(self):
        memA = NVCommunicationDevice(name="AMem", num_positions=2)
        memB = NVCommunicationDevice(name="BMem", num_positions=2)
        self.nodeA = QuantumNode(name="TestA", nodeID=1, memDevice=memA)
        self.nodeB = QuantumNode(name="TestB", nodeID=2, memDevice=memB)

        self.dqpA = EGPDistributedQueue(node=self.nodeA)
        self.dqpB = EGPDistributedQueue(node=self.nodeB)
        self.dqpA.connect_to_peer_protocol(self.dqpB)

    def test_init(self):
        qmm = QuantumMemoryManagement(node=self.nodeA)
        with self.assertRaises(TypeError):
            StrictPriorityRequestScheduler()

        with self.assertRaises(TypeError):
            StrictPriorityRequestScheduler(distQueue=self.dqpA)

        with self.assertRaises(TypeError):
            StrictPriorityRequestScheduler(qmm=qmm)

        test_scheduler = StrictPriorityRequestScheduler(distQueue=self.dqpA, qmm=qmm)
        self.assertEqual(test_scheduler.distQueue, self.dqpA)
        self.assertEqual(test_scheduler.qmm, qmm)
        self.assertEqual(test_scheduler.my_free_memory, qmm.get_free_mem_ad())
        self.assertEqual(test_scheduler.other_mem, (0, 0))

    def test_choose_queue(self):
        qmm = QuantumMemoryManagement(node=self.nodeA)
        request = EGPRequest(EGPSimulationScenario.construct_cqc_epr_request(otherID=self.nodeB.nodeID, num_pairs=1,
                                                                             min_fidelity=1, max_time=1, purpose_id=0,
                                                                             priority=0))
        test_scheduler = StrictPriorityRequestScheduler(distQueue=self.dqpA, qmm=qmm)
        self.assertEqual(test_scheduler.choose_queue(request), 0)

    def test_update_other_mem_size(self):
        qmm = QuantumMemoryManagement(node=self.nodeA)
        test_scheduler = StrictPriorityRequestScheduler(distQueue=self.dqpA, qmm=qmm)

        test_size = 3
        test_scheduler.update_other_mem_size(mem=test_size)

    def test_next(self):
        sim_reset()
        dqpA = EGPDistributedQueue(node=self.nodeA, accept_all=True)
        dqpB = EGPDistributedQueue(node=self.nodeB, accept_all=True)
        dqpA.connect_to_peer_protocol(dqpB)
        qmmA = QuantumMemoryManagement(node=self.nodeA)
        test_scheduler = StrictPriorityRequestScheduler(distQueue=dqpA, qmm=qmmA)
        test_scheduler.configure_mhp_timings(1, 2, 0, 0)

        request = EGPRequest(other_id=self.nodeB.nodeID, num_pairs=1, min_fidelity=1, max_time=0, purpose_id=0,
                             priority=0)

        conn = dqpA.conn
        self.network = EasyNetwork(name="DQPNetwork",
                                   nodes=[(self.nodeA, [dqpA]), (self.nodeB, [dqpB])],
                                   connections=[(conn, "dqp_conn", [dqpA, dqpB])])
        self.network.start()

        # Check that an empty queue has a default request
        self.assertEqual(test_scheduler.get_default_gen(), test_scheduler.next())

        # Check that an item not agreed upon also yields a default request
        test_scheduler.add_request(request)
        self.assertEqual(test_scheduler.get_default_gen(), test_scheduler.next())

        for i in range(11):
            sim_run(11)
            test_scheduler.inc_cycle()
            self.assertEqual(test_scheduler.get_default_gen(), test_scheduler.next())
        test_scheduler.inc_cycle()

        # Check that QMM reserve failure yields a default request
        comm_q = qmmA.reserve_communication_qubit()
        storage_q = [qmmA.reserve_storage_qubit() for _ in range(request.num_pairs)]
        self.assertEqual(test_scheduler.get_default_gen(), test_scheduler.next())

        # Return the reserved resources
        qmmA.vacate_qubit(comm_q)
        for q in storage_q:
            qmmA.vacate_qubit(q)

        # Check that lack of peer resources causes a default request
        self.assertEqual(test_scheduler.get_default_gen(), test_scheduler.next())

        # Verify that now we can obtain the next request
        test_scheduler.other_mem = (1, request.num_pairs)

        # Verify that the next request is the one we submitted
        gen = test_scheduler.next()
        self.assertEqual(gen, (True, (0, 0), 0, 1, {}))

    def test_priority(self):
        sim_reset()
        num_priorities = 10
        dqpA = EGPDistributedQueue(node=self.nodeA, accept_all=True, numQueues=num_priorities)
        dqpB = EGPDistributedQueue(node=self.nodeB, accept_all=True, numQueues=num_priorities)
        dqpA.connect_to_peer_protocol(dqpB)
        qmmA = QuantumMemoryManagement(node=self.nodeA)
        test_scheduler = StrictPriorityRequestScheduler(distQueue=dqpA, qmm=qmmA)
        test_scheduler.configure_mhp_timings(1, 2, 0, 0)

        requests = [EGPRequest(other_id=self.nodeB.nodeID, num_pairs=1, min_fidelity=1, max_time=0, purpose_id=0,
                               priority=i) for i in range(num_priorities)]

        conn = dqpA.conn
        self.network = EasyNetwork(name="DQPNetwork",
                                   nodes=[(self.nodeA, [dqpA]), (self.nodeB, [dqpB])],
                                   connections=[(conn, "dqp_conn", [dqpA, dqpB])])
        self.network.start()

        for i, request in enumerate(reversed(requests)):
            test_scheduler.add_request(request)
            sim_run(i * 5)
            for cycle in range(2 * test_scheduler.mhp_cycle_offset):
                test_scheduler.inc_cycle()

        for i in range(num_priorities):
            next_aid, next_request = test_scheduler._get_next_request()
            self.assertEqual((i, 0), next_aid)
            self.assertEqual(next_request.priority, i)
            test_scheduler.clear_request(next_aid)


class TestWFQRequestScheduler(unittest.TestCase):
    def setUp(self):
        sim_reset()

        network = setup_physical_network(ConfigPathStorage.NETWORK_NV_LAB_NOCAV_NOCONV)
        alice, bob = network.all_nodes()
        dqp_conn = network.get_connection(alice, bob, "dqp_conn")
        mhp_conn = network.get_connection(alice, bob, "mhp_conn")

        self.num_queues = 3

        self.distQueueA = WFQDistributedQueue(alice, numQueues=self.num_queues, accept_all=True)
        distQueueB = WFQDistributedQueue(bob, numQueues=self.num_queues, accept_all=True)
        self.distQueueA.connect_to_peer_protocol(distQueueB, conn=dqp_conn)

        self.qmmA = QuantumMemoryManagement(alice)
        qmmB = QuantumMemoryManagement(bob)

        mhp_service = SimulatedNodeCentricMHPService("mhp_service", alice, bob, conn=mhp_conn)

        self.feuA = SingleClickFidelityEstimationUnit(alice, mhp_service)
        feuB = SingleClickFidelityEstimationUnit(bob, mhp_service)

        self.schedulerA = WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA)

    def test_init(self):
        # Test default
        scheduler = WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA)
        self.assertEqual(scheduler.relative_weights, [1] * self.num_queues)

        # Test wrong type
        with self.assertRaises(TypeError):
            WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA, 0)
        with self.assertRaises(TypeError):
            WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA, {})

        # Test wrong length
        with self.assertRaises(ValueError):
            WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA, [0] * (self.num_queues + 1))

        # Test negative weights
        with self.assertRaises(ValueError):
            WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA, [0, 0, -1])

        # Test non-comparable weights
        with self.assertRaises(ValueError):
            WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA, [0, [], False])

        # Test non-trivial weights
        scheduler = WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA, [0, 5, 15])
        self.assertEqual(scheduler.relative_weights, [0, 0.25, 0.75])

    def test_compare_cycle_odd(self):
        scheduler = WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA)
        scheduler.max_mhp_cycle_number = 5
        scheduler.mhp_cycle_number = 0

        cycle1 = 0
        cycle2 = 1

        for i in range(scheduler.max_mhp_cycle_number):
            if i == 3:
                self.assertEqual(scheduler._compare_mhp_cycle(cycle1, cycle2), 1)
            else:
                self.assertEqual(scheduler._compare_mhp_cycle(cycle1, cycle2), -1)
            self.assertEqual(scheduler._compare_mhp_cycle(cycle1, cycle1), 0)
            self.assertEqual(scheduler._compare_mhp_cycle(cycle2, cycle2), 0)
            scheduler.mhp_cycle_number += 1

    def test_compare_cycle_even(self):
        scheduler = WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA)
        scheduler.max_mhp_cycle_number = 6
        scheduler.mhp_cycle_number = 0

        cycle1 = 0
        cycle2 = 1

        for i in range(scheduler.max_mhp_cycle_number):
            if i == 4:
                self.assertEqual(scheduler._compare_mhp_cycle(cycle1, cycle2), 1)
            else:
                self.assertEqual(scheduler._compare_mhp_cycle(cycle1, cycle2), -1)
            self.assertEqual(scheduler._compare_mhp_cycle(cycle1, cycle1), 0)
            self.assertEqual(scheduler._compare_mhp_cycle(cycle2, cycle2), 0)
            scheduler.mhp_cycle_number += 1

    def test_set_virtual_finish_same_queue_same_req(self):
        scheduler = WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA)

        # Compare high and low min_fidelity
        requests = [SchedulerRequest(), SchedulerRequest()]
        wfq_requests = []
        for req in requests:
            wfq_requests.append(scheduler.set_virtual_finish(req, 0))
        self.assertGreater(wfq_requests[0].init_virtual_finish, wfq_requests[1].init_virtual_finish)
        self.assertEqual(wfq_requests[0].est_cycles_per_pair, wfq_requests[1].est_cycles_per_pair)
        self.assertEqual(wfq_requests[0].init_virtual_finish + wfq_requests[0].est_cycles_per_pair, wfq_requests[1].init_virtual_finish)

    def test_set_virtual_finish_fidelity(self):
        scheduler = WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA)

        # Compare high and low min_fidelity
        requests = [SchedulerRequest(min_fidelity=0.8), SchedulerRequest(min_fidelity=0.6)]
        wfq_requests = []
        for qid, req in enumerate(requests):
            wfq_requests.append(scheduler.set_virtual_finish(req, qid))
        self.assertGreater(wfq_requests[0].init_virtual_finish, wfq_requests[1].init_virtual_finish)
        self.assertGreater(wfq_requests[0].est_cycles_per_pair, wfq_requests[1].est_cycles_per_pair)

    def test_set_virtual_finish_num_pairs(self):
        scheduler = WFQRequestScheduler(self.distQueueA, self.qmmA, self.feuA)

        # Compare high and low num pairs (non atomic)
        requests = [SchedulerRequest(num_pairs=2), SchedulerRequest(num_pairs=10)]
        wfq_requests = []
        for qid, req in enumerate(requests):
            wfq_requests.append(scheduler.set_virtual_finish(req, qid))
        self.assertEqual(wfq_requests[0].init_virtual_finish, wfq_requests[1].init_virtual_finish)
        self.assertEqual(wfq_requests[0].est_cycles_per_pair, wfq_requests[1].est_cycles_per_pair)

    def test_scheduling(self):
        pass



class TestTimings(unittest.TestCase):
    def setUp(self):
        def timeout_handler(request):
            self.timeout_handler_called[0] = True

        self.timeout_handler_called = [False]

        sim_reset()

        memA = NVCommunicationDevice(name="AMem", num_positions=2)
        memB = NVCommunicationDevice(name="BMem", num_positions=2)
        nodeA = QuantumNode(name="TestA", nodeID=1, memDevice=memA)
        nodeB = QuantumNode(name="TestB", nodeID=2, memDevice=memB)

        dqpA = EGPDistributedQueue(node=nodeA, accept_all=True)
        dqpB = EGPDistributedQueue(node=nodeB, accept_all=True)
        dqpA.connect_to_peer_protocol(dqpB)
        qmm = QuantumMemoryManagement(node=nodeA)
        self.test_scheduler = StrictPriorityRequestScheduler(distQueue=dqpA, qmm=qmm)

        conn = dqpA.conn
        network = EasyNetwork(name="DQPNetwork",
                              nodes=[(nodeA, [dqpA]), (nodeB, [dqpB])],
                              connections=[(conn, "dqp_conn", [dqpA, dqpB])])
        network.start()

        increase_mhp_cycle_protocol = IncreasMHPCycleProtocol(scheduler=self.test_scheduler)
        increase_mhp_cycle_protocol.start()

        self.test_scheduler.set_timeout_callback(timeout_handler)

    def test_timeout(self):
        self.test_scheduler.configure_mhp_timings(10, 12, 0, 0)
        request = EGPRequest(max_time=12)

        sim_run(1)

        self.test_scheduler.add_request(request)

        sim_run(2)

        self.assertFalse(self.timeout_handler_called[0])

        sim_run(19)

        self.assertFalse(self.timeout_handler_called[0])

        sim_run(20.1)

        self.assertTrue(self.timeout_handler_called[0])

    def test_too_long_timeout(self):
        max_mhp_cycle_number = 10

        self.test_scheduler.configure_mhp_timings(10, 12, 0, 0, max_mhp_cycle_number=max_mhp_cycle_number)
        request = EGPRequest(max_time=90)

        succ = self.test_scheduler.add_request(request)

        self.assertFalse(succ)

    def test_early_timeout(self):
        self.test_scheduler.configure_mhp_timings(10, 12, 0, 0)
        request = EGPRequest(max_time=1)

        sim_run(1)

        self.test_scheduler.add_request(request)
        self.assertFalse(self.timeout_handler_called[0])

        sim_run(10.1)

        self.assertTrue(self.timeout_handler_called[0])

    def test_short_timeout(self):
        self.test_scheduler.configure_mhp_timings(10, 12, 0, 0)
        request = EGPRequest(max_time=1)

        sim_run(9.9)

        self.test_scheduler.add_request(request)

        sim_run(10.1)

        self.assertFalse(self.timeout_handler_called[0])

        sim_run(20.1)

        self.assertTrue(self.timeout_handler_called[0])

    def test_multiple_timeout(self):
        self.test_scheduler.configure_mhp_timings(10, 12, 0, 0)
        request = EGPRequest(max_time=10)

        sim_run(10)

        self.test_scheduler.add_request(request)

        sim_run(10.1)

        self.assertFalse(self.timeout_handler_called[0])

        sim_run(20.1)

        self.assertTrue(self.timeout_handler_called[0])


if __name__ == "__main__":
    unittest.main()
