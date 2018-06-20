import unittest
from easysquid.easynetwork import EasyNetwork
from easysquid.qnode import QuantumNode
from easysquid.quantumMemoryDevice import QuantumProcessingDevice
from netsquid import pydynaa
from qlinklayer.distQueue import DistributedQueue
from qlinklayer.scheduler import RequestScheduler
from qlinklayer.qmm import QuantumMemoryManagement
from qlinklayer.egp import EGPRequest


class TestRequestScheduler(unittest.TestCase):
    def setUp(self):
        memA = QuantumProcessingDevice(name="AMem", max_num=2)
        memB = QuantumProcessingDevice(name="BMem", max_num=2)
        self.nodeA = QuantumNode(name="TestA", nodeID=1, memDevice=memA)
        self.nodeB = QuantumNode(name="TestB", nodeID=2, memDevice=memB)

        self.dqpA = DistributedQueue(node=self.nodeA)
        self.dqpB = DistributedQueue(node=self.nodeB)
        self.dqpA.connect_to_peer_protocol(self.dqpB)

    def test_init(self):
        qmm = QuantumMemoryManagement(node=self.nodeA)
        with self.assertRaises(TypeError):
            RequestScheduler()

        with self.assertRaises(TypeError):
            RequestScheduler(distQueue=self.dqpA)

        with self.assertRaises(TypeError):
            RequestScheduler(qmm=qmm)

        test_scheduler = RequestScheduler(distQueue=self.dqpA, qmm=qmm)
        self.assertEqual(test_scheduler.distQueue, self.dqpA)
        self.assertEqual(test_scheduler.qmm, qmm)
        self.assertEqual(test_scheduler.my_free_memory, qmm.get_free_mem_ad())
        self.assertEqual(test_scheduler.other_mem, 0)

    def test_next_pop(self):
        qmm = QuantumMemoryManagement(node=self.nodeA)
        test_scheduler = RequestScheduler(distQueue=self.dqpA, qmm=qmm)
        self.assertEqual(test_scheduler.next_pop(), 0)

    def test_get_queue(self):
        qmm = QuantumMemoryManagement(node=self.nodeA)
        request = EGPRequest(otherID=self.nodeB.nodeID, num_pairs=1, min_fidelity=1, max_time=1, purpose_id=0,
                             priority=0)
        test_scheduler = RequestScheduler(distQueue=self.dqpA, qmm=qmm)
        self.assertEqual(test_scheduler.get_queue(request), 0)

    def test_update_other_mem_size(self):
        qmm = QuantumMemoryManagement(node=self.nodeA)
        test_scheduler = RequestScheduler(distQueue=self.dqpA, qmm=qmm)

        test_size = 3
        test_scheduler.update_other_mem_size(mem=test_size)
        self.assertEqual(test_scheduler.other_mem, test_size)

    def test_timeout_stale_requests(self):
        pydynaa.DynAASim().reset()
        qmm = QuantumMemoryManagement(node=self.nodeA)
        test_scheduler = RequestScheduler(distQueue=self.dqpA, qmm=qmm)
        test_scheduler.other_mem = 1
        num_requests = 100
        request_set = [EGPRequest(otherID=self.nodeB.nodeID, num_pairs=1, min_fidelity=1, max_time=10, purpose_id=0,
                                  priority=0) for i in range(num_requests)]

        conn = self.dqpA.conn
        self.network = EasyNetwork(name="DQPNetwork",
                                   nodes=[(self.nodeA, [self.dqpA]), (self.nodeB, [self.dqpB])],
                                   connections=[(conn, "dqp_conn", [self.dqpA, self.dqpB])])
        self.network.start()

        sim_steps = 2000
        for t, request in enumerate(request_set):
            if t % 2:
                request.create_time = t
                self.dqpA.add(request)
            else:
                request.create_time = sim_steps + t
                self.dqpB.add(request)

        pydynaa.DynAASim().run(sim_steps)

        # Check that all requests became stale
        stale_requests = test_scheduler.timeout_stale_requests()
        self.assertEqual(len(stale_requests), num_requests // 2)

        for request in stale_requests:
            self.assertGreaterEqual(sim_steps, request.create_time + request.max_time)

        # Verify that the next request is the one we submitted
        generations = test_scheduler.next()
        self.assertEqual(generations, [(True, (0, 50), 0, 1, None, 0)])

    def test_request_ready(self):
        pydynaa.DynAASim().reset()
        qmmA = QuantumMemoryManagement(node=self.nodeA)
        qmmB = QuantumMemoryManagement(node=self.nodeB)
        test_scheduler = RequestScheduler(distQueue=self.dqpA, qmm=qmmA)
        test_scheduler.other_mem = qmmB.get_free_mem_ad()
        request = EGPRequest(otherID=self.nodeB.nodeID, num_pairs=1, min_fidelity=1, max_time=10, purpose_id=0,
                             priority=0)

        conn = self.dqpA.conn
        self.network = EasyNetwork(name="DQPNetwork",
                                   nodes=[(self.nodeA, [self.dqpA]), (self.nodeB, [self.dqpB])],
                                   connections=[(conn, "dqp_conn", [self.dqpA, self.dqpB])])
        self.network.start()

        # Check that an empty queue has no ready requests
        self.assertFalse(test_scheduler.request_ready())

        # Check that without agreement from dqp peer the request is not ready
        self.dqpA.add(request)
        ready_delay = self.dqpA.conn.channel_from_A.get_delay_mean() + self.dqpA.conn.channel_from_B.get_delay_mean()
        pydynaa.DynAASim().run(ready_delay - 1)
        self.assertFalse(test_scheduler.request_ready())

        # Check that when enough time has passed for queue agreement then the item is ready
        pydynaa.DynAASim().run(ready_delay + 1)
        self.assertTrue(test_scheduler.request_ready())

        # Verify that the next request is the one we submitted
        generations = test_scheduler.next()
        self.assertEqual(generations, [(True, (0, 0), 0, 1, None, 0)])

    def test_next(self):
        pydynaa.DynAASim().reset()
        qmmA = QuantumMemoryManagement(node=self.nodeA)
        test_scheduler = RequestScheduler(distQueue=self.dqpA, qmm=qmmA)
        request = EGPRequest(otherID=self.nodeB.nodeID, num_pairs=1, min_fidelity=1, max_time=10, purpose_id=0,
                             priority=0)

        conn = self.dqpA.conn
        self.network = EasyNetwork(name="DQPNetwork",
                                   nodes=[(self.nodeA, [self.dqpA]), (self.nodeB, [self.dqpB])],
                                   connections=[(conn, "dqp_conn", [self.dqpA, self.dqpB])])
        self.network.start()

        # Check that an empty queue has no next requests
        self.assertIsNone(test_scheduler.next())

        # Check that an item not agreed upon also yields no requests
        self.dqpA.add(request)
        self.assertIsNone(test_scheduler.next())

        pydynaa.DynAASim().run(10)

        # Check that QMM reserve failure yields no next request
        comm_q, storage_q = qmmA.reserve_entanglement_pair(n=request.num_pairs)
        self.assertIsNone(test_scheduler.next())

        # Return the reserved resources
        qmmA.free_qubit(comm_q)
        for q in storage_q:
            qmmA.free_qubit(q)

        # Check that lack of peer resources causes no next request
        self.assertIsNone(test_scheduler.next())

        # Verify that now we can obtain the next request
        test_scheduler.other_mem = request.num_pairs

        # Verify that the next request is the one we submitted
        generations = test_scheduler.next()
        self.assertEqual(generations, [(True, (0, 0), 0, 1, None, 0)])


if __name__ == "__main__":
    unittest.main()
