import unittest
from easysquid.easynetwork import EasyNetwork
from easysquid.qnode import QuantumNode
from easysquid.quantumMemoryDevice import NVCommunicationDevice
from netsquid.simutil import sim_run, sim_reset
from qlinklayer.distQueue import DistributedQueue
from qlinklayer.scheduler import RequestScheduler
from qlinklayer.qmm import QuantumMemoryManagement
from qlinklayer.egp import EGPRequest


class TestRequestScheduler(unittest.TestCase):
    def setUp(self):
        memA = NVCommunicationDevice(name="AMem", num_positions=2)
        memB = NVCommunicationDevice(name="BMem", num_positions=2)
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
        self.assertEqual(test_scheduler.other_mem, (0, 0))

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

    def test_next(self):
        sim_reset()
        dqpA = DistributedQueue(node=self.nodeA)
        dqpB = DistributedQueue(node=self.nodeB)
        dqpA.connect_to_peer_protocol(dqpB)
        qmmA = QuantumMemoryManagement(node=self.nodeA)
        test_scheduler = RequestScheduler(distQueue=dqpA, qmm=qmmA)

        request = EGPRequest(otherID=self.nodeB.nodeID, num_pairs=1, min_fidelity=1, max_time=12, purpose_id=0,
                             priority=0)

        conn = dqpA.conn
        self.network = EasyNetwork(name="DQPNetwork",
                                   nodes=[(self.nodeA, [dqpA]), (self.nodeB, [dqpB])],
                                   connections=[(conn, "dqp_conn", [dqpA, dqpB])])
        self.network.start()

        # Check that an empty queue has a default request
        self.assertEqual(test_scheduler.get_default_gen(), test_scheduler.next())

        # Check that an item not agreed upon also yields a default request
        dqpA.add(request)
        self.assertEqual(test_scheduler.get_default_gen(), test_scheduler.next())

        sim_run(11)

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
        self.assertEqual(gen, (True, (0, 0), 0, 1, None, (1, 1)))


if __name__ == "__main__":
    unittest.main()
