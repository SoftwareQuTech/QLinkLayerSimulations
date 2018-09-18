# Tests of the easy protocol implementation

import unittest
import numpy as np
from random import randint
from qlinklayer.distQueue import DistributedQueue
from easysquid.qnode import QuantumNode
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easynetwork import EasyNetwork
from easysquid.easyprotocol import TimedProtocol
from netsquid.simutil import sim_run, sim_reset


class FastTestProtocol(TimedProtocol):

    def __init__(self, node, dq, timeStep, p=0.7, num=1000, maxNum=1200):

        super().__init__(timeStep=timeStep, node=node)

        # Distributed queue
        self.dq = dq

        # Probability of adding an item
        self.p = p

        # Item Counter
        self.count = 0

        # Number of requests to randomly produce in timestep
        self.num = num

        # Maximum number to produce
        self.maxNum = maxNum

    def run_protocol(self):

        if self.count >= self.maxNum:
            return

        for j in range(self.num):
            if np.random.uniform(0, 1) < self.p:
                self.dq.add([self.node.name, self.count])
                self.count += 1

    def process_data(self):
        self.dq.process_data()


class TestProtocol(FastTestProtocol):
    def __init__(self, node, dq, timeStep, p=0.7):
        super().__init__(node, dq, timeStep, p, 1)


class TestDistributedQueue(unittest.TestCase):

    def test_init(self):

        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=1)

        # No arguments
        dq = DistributedQueue(node, conn)
        assert dq.node == node
        assert dq.conn == conn
        assert dq.myWsize == 100
        assert dq.master is True
        assert dq.otherWsize == 100
        assert len(dq.queueList) == 1
        assert dq.maxSeq == 2 ** 32
        assert dq.status == dq.STAT_IDLE
        assert dq.comms_seq == 0
        assert dq.acksWaiting == 0
        assert len(dq.waitAddAcks) == 0
        assert dq.expectedSeq == 0

        # No arguments, not controlling node
        dq = DistributedQueue(node2, conn)
        assert dq.node == node2
        assert dq.conn == conn
        assert dq.myWsize == 100
        assert dq.master is False
        assert dq.otherWsize == 100
        assert len(dq.queueList) == 1
        assert dq.maxSeq == 2 ** 32
        assert dq.status == dq.STAT_IDLE
        assert dq.comms_seq == 0
        assert dq.acksWaiting == 0
        assert len(dq.waitAddAcks) == 0
        assert dq.expectedSeq == 0

        # Set arguments

        dq = DistributedQueue(node, conn, False, 1, 2, 3, 4)
        assert dq.node == node
        assert dq.conn == conn
        assert dq.master is False
        assert dq.myWsize == 1
        assert dq.otherWsize == 2
        assert len(dq.queueList) == 3
        assert dq.maxSeq == 4
        assert dq.status == dq.STAT_IDLE
        assert dq.comms_seq == 0
        assert dq.acksWaiting == 0
        assert len(dq.waitAddAcks) == 0
        assert dq.expectedSeq == 0

    def test_add_basic(self):

        # Set up two nodes and run a simulation in which items
        # are randomly added at specific time intervals
        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)

        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = DistributedQueue(alice, conn)
        bobDQ = DistributedQueue(bob, conn)

        aliceProto = TestProtocol(alice, aliceDQ, 1)
        bobProto = TestProtocol(bob, bobDQ, 1)

        nodes = [
            (alice, [aliceProto]),
            (bob, [bobProto]),
        ]
        conns = [
            (conn, "dqp_conn", [aliceProto, bobProto])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(50000)

        # Check the Queue contains ordered elements from Alice and Bob
        qA = aliceDQ.queueList[0].queue
        qB = bobDQ.queueList[0].queue

        # First they should have the same length
        self.assertEqual(len(qA), len(qB))

        # Check the items are the same and the sequence numbers are ordered
        count = 0
        for k in range(len(qA)):
            self.assertEqual(qA[k].request, qB[k].request)
            self.assertEqual(qA[k].seq, qB[k].seq)
            self.assertEqual(qA[k].seq, count)
            count = count + 1

    def test_fast_master(self):

        # Set up two nodes and run a simulation in which items
        # are randomly added at specific time intervals
        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)
        conn = ClassicalFibreConnection(alice, bob, length=0.01)
        aliceDQ = DistributedQueue(alice, conn)
        bobDQ = DistributedQueue(bob, conn)

        aliceProto = FastTestProtocol(alice, aliceDQ, 1)
        bobProto = TestProtocol(bob, bobDQ, 1)
        nodes = [alice, bob]

        conns = [
            (conn, "dqp_conn", [aliceProto, bobProto])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(50000)

        # Check the Queue contains ordered elements from Alice and Bob
        qA = aliceDQ.queueList[0].queue
        qB = bobDQ.queueList[0].queue

        # First they should have the same length
        self.assertEqual(len(qA), len(qB))

        # Check the items are the same and the sequence numbers are ordered
        count = 0
        for k in range(len(qA)):
            self.assertEqual(qA[k].request, qB[k].request)
            self.assertEqual(qA[k].seq, qB[k].seq)
            self.assertEqual(qA[k].seq, count)
            count = count + 1

    def test_fast_slave(self):

        # Set up two nodes and run a simulation in which items
        # are randomly added at specific time intervals
        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)

        conn = ClassicalFibreConnection(alice, bob, length=0.01)
        aliceDQ = DistributedQueue(alice, conn)
        bobDQ = DistributedQueue(bob, conn)

        aliceProto = TestProtocol(alice, aliceDQ, 1)
        bobProto = FastTestProtocol(bob, bobDQ, 1)

        nodes = [alice, bob]

        conns = [
            (conn, "dqp_conn", [aliceProto, bobProto])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(5000)

        # Check the Queue contains ordered elements from Alice and Bob
        qA = aliceDQ.queueList[0].queue
        qB = bobDQ.queueList[0].queue

        # First they should have the same length
        self.assertEqual(len(qA), len(qB))

        # Check the items are the same and the sequence numbers are ordered
        count = 0
        for k in range(len(qA)):
            self.assertEqual(qA[k].request, qB[k].request)
            self.assertEqual(qA[k].seq, qB[k].seq)
            self.assertEqual(qA[k].seq, count)
            count = count + 1

    def test_comm_timeout(self):
        self.callback_storage = []

        def add_callback(result):
            self.callback_storage.append(result)

        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)

        conn = ClassicalFibreConnection(alice, bob, length=0.01)
        aliceDQ = DistributedQueue(alice, conn)

        aliceDQ.add_callback = add_callback

        aliceProto = TestProtocol(alice, aliceDQ, 1)

        nodes = [alice, bob]

        conns = [
            (conn, "dqp_conn", [aliceProto])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(5000)

        expected_qid = 0
        num_adds = 100
        expected_results = [(aliceDQ.DQ_TIMEOUT, expected_qid, qseq, [alice.name, qseq]) for qseq in range(num_adds)]

        # Check that all attempted add's timed out
        self.assertEqual(self.callback_storage, expected_results)

        # Check that alice's distributed queue has no outstanding add acks
        self.assertEqual(aliceDQ.waitAddAcks, {})

        # Check that all of the local queues are empty
        for local_queue in aliceDQ.queueList:
            self.assertEqual(local_queue.queue, {})

        # Check that we incremented the comms_seq
        self.assertEqual(aliceDQ.comms_seq, num_adds)

    def test_remove(self):
        # Set up two nodes and run a simulation in which items
        # are randomly added at specific time intervals
        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)

        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = DistributedQueue(alice, conn)
        bobDQ = DistributedQueue(bob, conn)

        aliceProto = TestProtocol(alice, aliceDQ, 1)
        bobProto = TestProtocol(bob, bobDQ, 1)

        nodes = [
            (alice, [aliceProto]),
            (bob, [bobProto]),
        ]
        conns = [
            (conn, "dqp_conn", [aliceProto, bobProto])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(50000)

        # Check the Queue contains ordered elements from Alice and Bob
        qA = aliceDQ.queueList[0].queue
        qB = bobDQ.queueList[0].queue

        # First they should have the same length
        self.assertEqual(len(qA), len(qB))

        # Check the items are the same and the sequence numbers are ordered
        count = 0
        for k in range(len(qA)):
            self.assertEqual(qA[k].request, qB[k].request)
            self.assertEqual(qA[k].seq, qB[k].seq)
            self.assertEqual(qA[k].seq, count)
            count = count + 1

        # Check that we can remove the items (locally) at both nodes
        rqid = 0
        rqseqs = set([randint(0, len(qA) - 1) for t in range(10)])
        for qseq in rqseqs:
            q_item = aliceDQ.remove_item(rqid, qseq)
            self.assertIsNotNone(q_item)
            self.assertFalse(aliceDQ.queueList[rqid].contains(qseq))

        # Check that we can pop the remaining items in the correct order
        remaining = set(range(len(qB))) - rqseqs
        for qseq in remaining:
            self.assertEqual(aliceDQ.queueList[rqid].popSeq, qseq)
            q_item = aliceDQ.local_pop(rqid)
            self.assertIsNotNone(q_item)


if __name__ == "__main__":
    unittest.main()
