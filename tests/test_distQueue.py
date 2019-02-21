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

        def faulty_send_add(cmd, data):
            if cmd == dq.CMD_ADD:
                _, cseq, qid, qseq, request = data
                if self.lost_messages[(cseq, qid, qseq)] >= 1:
                    dq.conn.put_from(dq.myID, (cmd, data))
                else:
                    self.lost_messages[(cseq, qid, qseq)] += 1
            else:
                dq.conn.put_from(dq.myID, (cmd, data))

        def faulty_send_ack(cmd, data):
            if cmd == dq2.CMD_ADD_ACK:
                _, ackd_id, qseq = data
                if self.lost_messages[(ackd_id, qseq)] >= 1:
                    dq2.conn.put_from(dq2.myID, (cmd, data))
                else:
                    self.lost_messages[(ackd_id, qseq)] += 1
            else:
                dq2.conn.put_from(dq2.myID, (cmd, data))

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

        def faulty_send_add(cmd, data):
            if cmd == dq2.CMD_ADD:
                _, cseq, qid, qseq, request = data
                if self.lost_messages[(cseq, qid, qseq)] >= 1:
                    dq2.conn.put_from(dq2.myID, (cmd, data))
                else:
                    self.lost_messages[(cseq, qid, qseq)] += 1
            else:
                dq2.conn.put_from(dq2.myID, (cmd, data))

        def faulty_send_ack(cmd, data):
            if cmd == dq.CMD_ADD_ACK:
                _, ackd_id, qseq = data
                if self.lost_messages[(ackd_id, qseq)] >= 1:
                    dq.conn.put_from(dq.myID, (cmd, data))
                else:
                    self.lost_messages[(ackd_id, qseq)] += 1
            else:
                dq.conn.put_from(dq.myID, (cmd, data))

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

        from easysquid.puppetMaster import PM_Controller
        from qlinklayer.datacollection import EGPLocalQueueSequence
        from qlinklayer.scheduler import WFQSchedulerRequest

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

        dq.callback = callback
        dq2.callback = callback
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
