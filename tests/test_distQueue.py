# Tests of the easy protocol implementation

import unittest
import numpy as np
import logging
from random import randint
from collections import defaultdict
from qlinklayer.scheduler import SchedulerRequest
from qlinklayer.distQueue import DistributedQueue, FilteredDistributedQueue, EGPDistributedQueue, WFQDistributedQueue
from qlinklayer.toolbox import LinkLayerException
from qlinklayer.localQueue import EGPLocalQueue
from easysquid.qnode import QuantumNode
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easynetwork import EasyNetwork
from easysquid.easyprotocol import TimedProtocol
from easysquid.toolbox import logger
from netsquid.simutil import sim_run, sim_reset
from easysquid.puppetMaster import PM_Controller
from qlinklayer.datacollection import EGPLocalQueueSequence
from qlinklayer.scheduler import WFQSchedulerRequest

logger.setLevel(logging.CRITICAL)


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
        for j in range(self.num):
            if self.count >= self.maxNum:
                return

            if np.random.uniform(0, 1) < self.p:
                try:
                    self.dq.add([self.node.name, self.count])
                    self.count += 1

                    if self.count >= self.maxNum:
                        return
                except Exception:
                    return

    def process_data(self):
        self.dq.process_data()


class TestProtocol(FastTestProtocol):
    def __init__(self, node, dq, timeStep, p=0.7, num=1, maxNum=1200):
        super().__init__(node, dq, timeStep, p, num, maxNum)


class TestDistributedQueue(unittest.TestCase):
    def check_local_queues(self, lq1, lq2):
        self.assertEqual(len(lq1.sequence_to_item), len(lq2.sequence_to_item))

        if len(lq1.sequence_to_item) == 0:
            return

        qitem1 = lq1.pop()
        qitem2 = lq2.pop()
        while qitem1 is not None:
            self.assertIsNotNone(qitem2)
            self.assertEqual(qitem1.seq, qitem2.seq)
            self.assertEqual(qitem1.request, qitem2.request)

            qitem1 = lq1.pop()
            qitem2 = lq2.pop()

    def ready_items(self, lq):
        for qseq in lq.sequence_to_item:
            lq.ready(qseq)

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
        assert dq.maxSeq == 2 ** 8
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
        assert dq.maxSeq == 2 ** 8
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

        aliceProto = TestProtocol(alice, aliceDQ, 1, maxNum=aliceDQ.maxSeq // 2)
        bobProto = TestProtocol(bob, bobDQ, 1, maxNum=bobDQ.maxSeq // 2)

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
        qA = aliceDQ.queueList[0].sequence_to_item
        qB = bobDQ.queueList[0].sequence_to_item

        # First they should have the same length
        self.assertGreater(len(qA), 0)
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
        conn = ClassicalFibreConnection(alice, bob, length=0.001)
        aliceDQ = DistributedQueue(alice, conn, maxSeq=128)
        bobDQ = DistributedQueue(bob, conn, maxSeq=128)

        aliceProto = FastTestProtocol(alice, aliceDQ, 1)
        bobProto = TestProtocol(bob, bobDQ, 1)
        nodes = [alice, bob]

        conns = [
            (conn, "dqp_conn", [aliceProto, bobProto])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(1000)

        # Check the Queue contains ordered elements from Alice and Bob
        qA = aliceDQ.queueList[0].sequence_to_item
        qB = bobDQ.queueList[0].sequence_to_item

        # First they should have the same length
        self.assertGreater(len(qA), 0)
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

        aliceProto = TestProtocol(alice, aliceDQ, 1, maxNum=aliceDQ.maxSeq // 2)
        bobProto = FastTestProtocol(bob, bobDQ, 1, maxNum=bobDQ.maxSeq // 2)

        nodes = [alice, bob]

        conns = [
            (conn, "dqp_conn", [aliceProto, bobProto])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(5000)

        # Check the Queue contains ordered elements from Alice and Bob
        qA = aliceDQ.queueList[0].sequence_to_item
        qB = bobDQ.queueList[0].sequence_to_item

        # First they should have the same length
        self.assertGreater(len(qA), 0)
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

        num_adds = 100
        aliceProto = TestProtocol(alice, aliceDQ, 1, maxNum=num_adds)

        nodes = [alice, bob]

        conns = [
            (conn, "dqp_conn", [aliceProto])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        sim_run(5000)

        expected_qid = 0
        expected_results = [(aliceDQ.DQ_TIMEOUT, expected_qid, qseq, [alice.name, qseq]) for qseq in range(num_adds)]

        # Check that all attempted add's timed out
        self.assertEqual(self.callback_storage, expected_results)

        # Check that alice's distributed queue has no outstanding add acks
        self.assertEqual(aliceDQ.waitAddAcks, {})

        # Check that all of the local queues are empty
        for local_queue in aliceDQ.queueList:
            self.assertEqual(local_queue.queue, [])
            self.assertEqual(local_queue.sequence_to_item, {})

        # Check that we incremented the comms_seq
        self.assertEqual(aliceDQ.comms_seq, num_adds)

    def test_lost_add_master(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=0.0001)

        dq = DistributedQueue(node, conn, numQueues=3)
        dq2 = DistributedQueue(node2, conn, numQueues=3)
        dq.connect_to_peer_protocol(dq2, conn)

        self.lost_messages = defaultdict(int)

        def faulty_send_add(cmd, data, clock):
            if cmd == dq.CMD_ADD:
                _, cseq, qid, qseq, request = data
                if self.lost_messages[(cseq, qid, qseq)] >= 1:
                    dq.conn.put_from(dq.myID, (cmd, data, clock))
                else:
                    self.lost_messages[(cseq, qid, qseq)] += 1
            else:
                dq.conn.put_from(dq.myID, (cmd, data, clock))

        def faulty_send_ack(cmd, data, clock):
            if cmd == dq2.CMD_ADD_ACK:
                _, ackd_id, qseq = data
                if self.lost_messages[(ackd_id, qseq)] >= 1:
                    dq2.conn.put_from(dq2.myID, (cmd, data, clock))
                else:
                    self.lost_messages[(ackd_id, qseq)] += 1
            else:
                dq2.conn.put_from(dq2.myID, (cmd, data, clock))

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        dq.send_msg = faulty_send_add
        dq2.send_msg = faulty_send_ack

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        reqs = []
        num_reqs = 10
        for i in range(num_reqs):
            req = [node.nodeID, i]
            reqs.append(req)
            dq.add(req)
            sim_run(0.1 * (i + 1))

        sim_run(200)

        # Check that all add and add_ack messages were lost once
        for v in self.lost_messages.values():
            self.assertEqual(v, 1)

        # Check that the item successfully got added
        self.assertEqual(len(dq.queueList[0].queue), 10)
        self.assertEqual(len(dq2.queueList[0].queue), 10)

        for i, expected_req in zip(range(num_reqs), reqs):
            item1 = dq.queueList[0].queue[i]
            item2 = dq2.queueList[0].queue[i]
            self.assertEqual(item1.request, expected_req)
            self.assertEqual(item2.request, expected_req)

    def test_lost_add_slave(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=0.0001)

        dq = DistributedQueue(node, conn, numQueues=3)
        dq2 = DistributedQueue(node2, conn, numQueues=3)
        dq.connect_to_peer_protocol(dq2, conn)

        self.lost_messages = defaultdict(int)

        def faulty_send_add(cmd, data, clock):
            if cmd == dq2.CMD_ADD:
                _, cseq, qid, qseq, request = data
                if self.lost_messages[(cseq, qid, qseq)] >= 1:
                    dq2.conn.put_from(dq2.myID, (cmd, data, clock))
                else:
                    self.lost_messages[(cseq, qid, qseq)] += 1
            else:
                dq2.conn.put_from(dq2.myID, (cmd, data, clock))

        def faulty_send_ack(cmd, data, clock):
            if cmd == dq.CMD_ADD_ACK:
                _, ackd_id, qseq = data
                if self.lost_messages[(ackd_id, qseq)] >= 1:
                    dq.conn.put_from(dq.myID, (cmd, data, clock))
                else:
                    self.lost_messages[(ackd_id, qseq)] += 1
            else:
                dq.conn.put_from(dq.myID, (cmd, data, clock))

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        dq2.send_msg = faulty_send_add
        dq.send_msg = faulty_send_ack

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        reqs = []
        num_reqs = 10
        for i in range(num_reqs):
            req = [node2.nodeID, i]
            reqs.append(req)
            dq2.add(req)
            sim_run(0.1 * (i + 1))

        sim_run(200)

        # Check that all add and add_ack messages were lost once
        for v in self.lost_messages.values():
            self.assertEqual(v, 1)

        # Check that the item successfully got added
        self.assertEqual(len(dq.queueList[0].queue), 10)
        self.assertEqual(len(dq2.queueList[0].queue), 10)

        for i, expected_req in zip(range(num_reqs), reqs):
            item1 = dq.queueList[0].queue[i]
            item2 = dq2.queueList[0].queue[i]
            self.assertEqual(item1.request, expected_req)
            self.assertEqual(item2.request, expected_req)

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

        sim_run(1000)

        # Check the Queue contains ordered elements from Alice and Bob
        queueA = aliceDQ.queueList[0]
        queueB = bobDQ.queueList[0]
        qA = queueA.sequence_to_item
        qB = queueB.sequence_to_item

        # Make all the items ready
        for seq in qA:
            queueA.ack(seq)
            queueA.ready(seq)
        for seq in qB:
            queueB.ack(seq)
            queueB.ready(seq)

        # First they should have the same length
        self.assertGreater(len(qA), 0)
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
            q_item = aliceDQ.local_pop(rqid)
            self.assertEqual(q_item.seq, qseq)
            self.assertIsNotNone(q_item)

    def test_full_queue(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=0.0001)

        dq = DistributedQueue(node, conn, numQueues=3)
        dq2 = DistributedQueue(node2, conn, numQueues=3)
        dq.connect_to_peer_protocol(dq2, conn)

        storage = []

        def callback(result):
            storage.append(result)

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        for j in range(dq.maxSeq):
            dq.add(request=j + 1, qid=0)

        sim_run(1000)

        with self.assertRaises(LinkLayerException):
            dq.add(request=0, qid=0)

        dq2.add_callback = callback
        for j in range(dq2.maxSeq):
            dq2.add(request=j + 1, qid=1)

        sim_run(2000)

        with self.assertRaises(LinkLayerException):
            dq2.add(request=dq2.maxSeq + 1, qid=1)

        self.assertEqual(len(storage), 257)
        self.assertEqual(storage[-1], (dq2.DQ_ERR, 1, None, dq2.maxSeq + 1))
        storage = []

        for j in range(dq.maxSeq):
            if j % 2:
                dq.add(request=j + 1, qid=2)
            else:
                dq2.add(request=j + 1, qid=2)

        dq2.add(request=dq.maxSeq + 1, qid=2)
        sim_run(3000)

        with self.assertRaises(LinkLayerException):
            dq.add(request=0, qid=2)

        self.assertEqual(len(storage), 257)
        self.assertEqual(storage[-1], (dq2.DQ_REJECT, 2, 0, dq2.maxSeq + 1))

    def test_excessive_full_queue(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=25)

        wSize = 2
        maxSeq = 6
        dq = WFQDistributedQueue(node, conn, numQueues=3, throw_local_queue_events=True, accept_all=True, myWsize=wSize,
                                 otherWsize=wSize, maxSeq=maxSeq)
        dq2 = WFQDistributedQueue(node2, conn, numQueues=3, throw_local_queue_events=True, accept_all=True,
                                  myWsize=wSize, otherWsize=wSize, maxSeq=maxSeq)
        dq.connect_to_peer_protocol(dq2, conn)

        pm = PM_Controller()
        ds = EGPLocalQueueSequence(name="EGP Local Queue A {}".format(0), dbFile='test.db')

        pm.addEvent(dq.queueList[0], dq.queueList[0]._EVT_ITEM_ADDED, ds=ds)
        pm.addEvent(dq.queueList[0], dq.queueList[0]._EVT_ITEM_REMOVED, ds=ds)

        storage = []

        def callback(result):
            storage.append(result)

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        dq.add_callback = callback
        dq2.add_callback = callback
        for i in range(1, 3 * dq2.maxSeq):
            for j in range(3 * dq2.maxSeq):
                try:
                    r = WFQSchedulerRequest(0, 0, 0, 0, j + 1, 0, 0, 0, True, False, False, True)
                    if j % i:
                        dq.add(request=r, qid=0)
                    else:
                        dq2.add(request=r, qid=0)
                except Exception:
                    pass

            sim_run(i * 1000000)

            for seq in range(dq.maxSeq):
                qitem = dq.queueList[0].sequence_to_item.get(seq)
                q2item = dq2.queueList[0].sequence_to_item.get(seq)
                self.assertEqual(qitem.request, q2item.request)

            self.assertEqual(len(dq.backlogAdd), 0)
            self.assertEqual(len(dq2.backlogAdd), 0)

            for j in range(dq.maxSeq):
                dq.remove_item(0, j)
                dq2.remove_item(0, j)

    def test_full_wraparound(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=25)

        wSize = 2
        maxSeq = 6
        dq = DistributedQueue(node, conn, numQueues=3, throw_local_queue_events=True, myWsize=wSize,
                              otherWsize=wSize, maxSeq=maxSeq)
        dq2 = DistributedQueue(node2, conn, numQueues=3, throw_local_queue_events=True, myWsize=wSize,
                               otherWsize=wSize, maxSeq=maxSeq)
        dq.connect_to_peer_protocol(dq2, conn)

        storage = []

        def callback(result):
            storage.append(result)

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        for timestep in range(1, maxSeq + 1):
            dq.add(timestep, 0)
            sim_run(timestep * 1000000)
            dq.queueList[0].queue[timestep - 1].ready = True
            dq2.queueList[0].queue[timestep - 1].ready = True

        dq.queueList[0].pop()
        dq2.queueList[0].pop()
        dq.add(maxSeq, 0)
        sim_run(maxSeq * 100000)

        dq.queueList[0].pop()
        dq2.queueList[0].pop()
        dq.queueList[0].pop()
        dq2.queueList[0].pop()

    def test_unordered_subsequent_acks(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=25)

        dq = DistributedQueue(node, conn, numQueues=3, throw_local_queue_events=True)
        dq2 = DistributedQueue(node2, conn, numQueues=3, throw_local_queue_events=True)
        dq.connect_to_peer_protocol(dq2, conn)

        storage1 = []
        storage2 = []

        def callback1(result):
            storage1.append(result)
        def callback2(result):
            storage2.append(result)

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        # Add three requests (master)
        dq.add_callback = callback1
        dq2.add_callback = callback2
        for create_id in range(3):
            request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
            dq.add(request=request, qid=0)
        run_time = dq.comm_delay * dq.timeout_factor
        sim_run(run_time)
        self.assertEqual(len(storage1), 3)
        self.assertEqual(len(storage2), 3)
        q_seqs1 = [res[2] for res in storage1]
        q_seqs2 = [res[2] for res in storage2]
        self.assertEqual(q_seqs1, [0, 1, 2])
        self.assertEqual(q_seqs2, [0, 1, 2])

        # Remove one request (such that next queue seq will be 0 again)
        dq.remove_item(0, 1)
        dq2.remove_item(0, 1)
        storage1 = []
        storage2 = []

        # Add requests from master and slave
        create_id = 3
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq.add(request=request, qid=0)
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq2.add(request=request, qid=0)

        run_time += dq.comm_delay * dq.timeout_factor
        sim_run(run_time)

        self.assertEqual(len(storage1), 2)
        self.assertEqual(len(storage2), 2)
        q_seqs1 = [res[2] for res in storage1]
        q_seqs2 = [res[2] for res in storage2]
        self.assertIn(1, q_seqs1)
        self.assertIn(3, q_seqs1)
        self.assertIn(1, q_seqs2)
        self.assertIn(3, q_seqs2)
        # TODO what should the ordering be?
        # self.assertEqual(q_seqs1, [1, 3])
        # self.assertEqual(q_seqs2, [1, 3])

    def test_resend_acks(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=25)

        dq = DistributedQueue(node, conn, numQueues=3, throw_local_queue_events=True)
        dq2 = DistributedQueue(node2, conn, numQueues=3, throw_local_queue_events=True)
        dq.connect_to_peer_protocol(dq2, conn)

        storage1 = []
        storage2 = []

        def callback1(result):
            storage1.append(result)

        def callback2(result):
            storage2.append(result)

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()
        dq.add_callback = callback1
        dq2.add_callback = callback2

        # Add one request
        create_id = 0
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq2.add(request=request, qid=0)
        run_time = dq.comm_delay * dq.timeout_factor
        sim_run(run_time)

        # Set way to short timeout (to force resend)
        dq2.timeout_factor = 1 / 2

        # Add one request (slave)
        create_id = 1
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq2.add(request=request, qid=0)
        run_time += (dq.comm_delay + 1) * 4
        sim_run(run_time)

        # Set to correct factor again
        dq2.timeout_factor = 2

        create_id = 2
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq.add(request=request, qid=0)
        dq2.add(request=request, qid=0)
        run_time += dq.comm_delay * dq.timeout_factor
        sim_run(run_time)

        self.assertEqual(len(storage1), 4)
        self.assertEqual(len(storage2), 4)

        q_seqs1 = [res[2] for res in storage1]
        q_seqs2 = [res[2] for res in storage2]

        for qseq in range(4):
            for q_seqs in [q_seqs1, q_seqs2]:
                # TODO do we care about the ordering?
                self.assertIn(qseq, q_seqs)

    def test_slave_add_while_waiting(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=25)

        dq = DistributedQueue(node, conn, numQueues=3, throw_local_queue_events=True)
        dq2 = DistributedQueue(node2, conn, numQueues=3, throw_local_queue_events=True)
        dq.connect_to_peer_protocol(dq2, conn)

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        # Add one request for both master and slave
        create_id = 0
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq.add(request=request, qid=0)
        create_id = 1
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq2.add(request=request, qid=0)

        # Wait for slaves add to arrive but not the ack
        run_time = dq.comm_delay * (3 / 4)
        sim_run(run_time)

        # Add request from master
        create_id = 2
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq.add(request=request, qid=0)

        # Make sure things are added
        run_time = dq.comm_delay * 4
        sim_run(run_time)

        self.ready_items(dq.queueList[0])
        self.ready_items(dq2.queueList[0])
        self.check_local_queues(dq.queueList[0], dq2.queueList[0])

    def test_random_add_remove(self):
        sim_reset()
        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalFibreConnection(node, node2, length=25)

        dq = DistributedQueue(node, conn, numQueues=3, throw_local_queue_events=True)
        dq2 = DistributedQueue(node2, conn, numQueues=3, throw_local_queue_events=True)
        dq.connect_to_peer_protocol(dq2, conn)

        nodes = [
            (node, [dq]),
            (node2, [dq2]),
        ]
        conns = [
            (conn, "dq_conn", [dq, dq2])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        create_id = 0
        for _ in range(20):
            # Add random requests to master
            num_reqs_master = randint(0, 3)
            for _ in range(num_reqs_master):
                request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
                try:
                    dq.add(request=request, qid=0)
                except LinkLayerException:
                    # Full queue
                    pass
                create_id += 1

            # Add random requests to slave
            num_reqs_slave = randint(0, 3)
            for _ in range(num_reqs_slave):
                request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
                try:
                    dq2.add(request=request, qid=0)
                except LinkLayerException:
                    # Full queue
                    pass
                create_id += 1

            # Randomly remove things for both
            num_pop = randint(0, 6)
            for _ in range(num_pop):
                dq.local_pop(qid=0)

            # Run for random fraction of timeout
            r = randint(1, 20)
            run_time = dq.comm_delay * dq.timeout_factor * (r / 10)
            sim_run(run_time)

        # Make sure things are not in flight
        sim_run()

        self.ready_items(dq.queueList[0])
        self.ready_items(dq2.queueList[0])
        self.check_local_queues(dq.queueList[0], dq2.queueList[0])

        # Add one request for both master and slave
        create_id = 0
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq.add(request=request, qid=0)
        create_id = 1
        request = SchedulerRequest(0, 0, 0, 0, create_id, 0, 0, True, False, False, True)
        dq2.add(request=request, qid=0)


class TestFilteredDistributedQueue(unittest.TestCase):
    def setUp(self):
        self.result = None

    def test_add_rule(self):
        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)

        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = FilteredDistributedQueue(alice, conn)

        ruleset = [(randint(0, 255), randint(0, 255)) for _ in range(255)]
        for nodeID, purpose_id in ruleset:
            aliceDQ.add_accept_rule(nodeID, purpose_id)
            self.assertTrue(purpose_id in aliceDQ.accept_rules[nodeID])

    def test_remove_rule(self):
        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)

        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = FilteredDistributedQueue(alice, conn)

        ruleset = set([(randint(0, 255), randint(0, 255)) for _ in range(255)])
        for nodeID, purpose_id in ruleset:
            aliceDQ.add_accept_rule(nodeID, purpose_id)
            self.assertTrue(purpose_id in aliceDQ.accept_rules[nodeID])

        for nodeID, purpose_id in ruleset:
            aliceDQ.remove_accept_rule(nodeID, purpose_id)
            self.assertFalse(purpose_id in aliceDQ.accept_rules[nodeID])

        for nodeID, _ in ruleset:
            self.assertEqual(aliceDQ.accept_rules[nodeID], set())

    def test_load_rule(self):
        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)

        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = FilteredDistributedQueue(alice, conn)

        rule_config = defaultdict(list)
        ruleset = [(randint(0, 255), randint(0, 255)) for _ in range(255)]
        for nodeID, purpose_id in ruleset:
            rule_config[nodeID].append(purpose_id)

        aliceDQ.load_accept_rules(rule_config)
        self.assertEqual(aliceDQ.accept_rules, rule_config)

    def test_rules(self):
        sim_reset()
        alice = QuantumNode("Alice", 1)
        bob = QuantumNode("Bob", 2)

        self.result = None

        def add_callback(result):
            self.result = result

        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = FilteredDistributedQueue(alice, conn)
        aliceDQ.add_callback = add_callback
        bobDQ = FilteredDistributedQueue(bob, conn)
        bobDQ.add_callback = add_callback

        nodes = [
            (alice, [aliceDQ]),
            (bob, [bobDQ]),
        ]
        conns = [
            (conn, "dqp_conn", [aliceDQ, bobDQ])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()

        # Test that we cannot add a request
        request = SchedulerRequest(num_pairs=1, min_fidelity=0.5, timeout_cycle=10, purpose_id=0, priority=10)

        # Test that no rule results in rejection of request
        aliceDQ.add(request)
        expected_qid = 0
        expected_qseq = 0
        sim_run(2)
        self.assertIsNotNone(self.result)
        reported_request = self.result[-1]
        self.assertEqual(reported_request, request)
        self.assertEqual(self.result[:3], (aliceDQ.DQ_REJECT, expected_qid, expected_qseq))

        # Reset result
        self.result = None

        # Test that we can now add a request with the rule in place
        bobDQ.add_accept_rule(nodeID=alice.nodeID, purpose_id=0)
        aliceDQ.add(request)
        expected_qseq = 0
        sim_run(4)
        self.assertIsNotNone(self.result)
        reported_request = self.result[-1]
        self.assertEqual(reported_request, request)
        self.assertEqual(self.result[:3], (aliceDQ.DQ_OK, expected_qid, expected_qseq))

        # Reset result
        self.result = None

        # Test that we can remove the acception rule and request will get rejected
        bobDQ.remove_accept_rule(nodeID=alice.nodeID, purpose_id=0)
        aliceDQ.add(request)
        expected_qseq += 1
        sim_run(6)
        self.assertIsNotNone(self.result)
        reported_request = self.result[-1]
        self.assertEqual(reported_request, request)
        self.assertEqual(self.result[:3], (aliceDQ.DQ_REJECT, expected_qid, expected_qseq))


class TestEGPDistributedQueue(unittest.TestCase):
    def test_init(self):
        def callback(queue_item):
            pass

        node = QuantumNode("test", nodeID=1)
        dq = EGPDistributedQueue(node)
        self.assertIs(dq.timeout_callback, None)

        dq = EGPDistributedQueue(node, timeout_callback=callback)
        self.assertIs(dq.timeout_callback, callback)

    def test_set_timeout_callback(self):
        def callback1(queue_item):
            pass

        def callback2(queue_item):
            pass

        node = QuantumNode("test", nodeID=1)
        dq = EGPDistributedQueue(node, timeout_callback=callback1)
        self.assertIs(dq.timeout_callback, callback1)

        dq = EGPDistributedQueue(node, timeout_callback=callback2)
        self.assertIs(dq.timeout_callback, callback2)

    def test__init_queues(self):
        node = QuantumNode("test", nodeID=1)
        dq = EGPDistributedQueue(node)
        dq._init_queues(2)
        self.assertEqual(len(dq.queueList), 2)
        self.assertEqual(dq.numLocalQueues, 2)
        for q in dq.queueList:
            self.assertIsInstance(q, EGPLocalQueue)

    def test_update_mhp_cycle_number(self):
        def callback_alice(queue_item):
            callback_called[0] = True

        def callback_bob(queue_item):
            callback_called[1] = True

        sim_reset()
        callback_called = [False, False]
        alice = QuantumNode("alice", nodeID=0)
        bob = QuantumNode("bob", nodeID=1)
        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = EGPDistributedQueue(alice, conn, timeout_callback=callback_alice, accept_all=True)
        bobDQ = EGPDistributedQueue(bob, conn, timeout_callback=callback_bob, accept_all=True)

        nodes = [
            (alice, [aliceDQ]),
            (bob, [bobDQ]),
        ]
        conns = [
            (conn, "dqp_conn", [aliceDQ, bobDQ])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()
        request = SchedulerRequest(sched_cycle=1, timeout_cycle=2)
        aliceDQ.add(request, 0)
        sim_run(10)
        queue_item_alice = aliceDQ.local_peek(0)
        queue_item_bob = bobDQ.local_peek(0)
        self.assertFalse(queue_item_alice.ready)

        aliceDQ.update_mhp_cycle_number(1, 10)
        self.assertTrue(queue_item_alice.ready)
        self.assertFalse(queue_item_bob.ready)
        self.assertFalse(callback_called[0])
        self.assertFalse(callback_called[1])

        aliceDQ.update_mhp_cycle_number(2, 10)
        self.assertTrue(callback_called[0])
        self.assertFalse(callback_called[1])

        bobDQ.update_mhp_cycle_number(1, 10)
        self.assertTrue(queue_item_bob.ready)
        self.assertFalse(callback_called[1])

        bobDQ.update_mhp_cycle_number(2, 10)
        self.assertTrue(queue_item_bob.ready)
        self.assertTrue(callback_called[1])

    def test_faulty_queue_ID(self):
        def add_callback(result):
            self.assertEqual(result[0], aliceDQ.DQ_REJECT)
            callback_called[0] = True

        sim_reset()

        callback_called = [False]

        alice = QuantumNode("alice", nodeID=0)
        bob = QuantumNode("bob", nodeID=1)
        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = EGPDistributedQueue(alice, conn, accept_all=True, numQueues=1)
        bobDQ = EGPDistributedQueue(bob, conn, accept_all=True, numQueues=1)
        aliceDQ.add_callback = add_callback

        nodes = [
            (alice, [aliceDQ]),
            (bob, [bobDQ]),
        ]
        conns = [
            (conn, "dqp_conn", [aliceDQ, bobDQ])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()
        request = SchedulerRequest()
        aliceDQ.add(request, qid=1)
        sim_run(10)
        self.assertTrue(callback_called[0])

    def test_multiple_queues(self):
        sim_reset()
        alice = QuantumNode("alice", nodeID=0)
        bob = QuantumNode("bob", nodeID=1)
        conn = ClassicalFibreConnection(alice, bob, length=.0001)
        aliceDQ = EGPDistributedQueue(alice, conn, accept_all=True, numQueues=2)
        bobDQ = EGPDistributedQueue(bob, conn, accept_all=True, numQueues=2)

        nodes = [
            (alice, [aliceDQ]),
            (bob, [bobDQ]),
        ]
        conns = [
            (conn, "dqp_conn", [aliceDQ, bobDQ])
        ]

        network = EasyNetwork(name="DistQueueNetwork", nodes=nodes, connections=conns)
        network.start()
        alice_requests = [SchedulerRequest(), SchedulerRequest()]
        bob_requests = [SchedulerRequest(), SchedulerRequest()]
        aliceDQ.add(alice_requests[0], qid=0)
        aliceDQ.add(alice_requests[1], qid=1)
        bobDQ.add(bob_requests[0], qid=0)
        bobDQ.add(bob_requests[1], qid=1)
        sim_run(10)
        self.assertEqual(len(aliceDQ.queueList[0].queue), 2)
        self.assertEqual(len(aliceDQ.queueList[1].queue), 2)
        self.assertEqual(len(bobDQ.queueList[0].queue), 2)
        self.assertEqual(len(bobDQ.queueList[1].queue), 2)


if __name__ == "__main__":
    unittest.main()
