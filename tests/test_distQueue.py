# Tests of the easy protocol implementation

import unittest
import numpy as np
import netsquid.pydynaa as pydynaa

from qlinklayer.distQueue import DistributedQueue
from easysquid.qnode import QuantumNode
from easysquid.toolbox import *
from easysquid.connection import ClassicalConnection
from easysquid.easyprotocol import TimedProtocol

class TestProtocol(TimedProtocol):

    def __init__(self, node, dq, timeStep, p=0.7):

        super().__init__(timeStep=timeStep, node=node)

        # Distributed queue
        self.dq = dq

        # Probability of adding an item
        self.p = p

        # Item Counter
        self.count = 0
        

    def run_protocol(self):
        if np.random.uniform(0,1) < self.p:
            self.dq.add([self.node.name, self.count])
            self.count = self.count + 1

    def process_data(self):
        self.dq.process_data()


class TestDistributedQueue(unittest.TestCase):

    def setUp(self):
        self.logger = setup_logging("GLOBAL", "logFile", level=logging.INFO)

    def test_init(self):

        node = QuantumNode("TestNode 1", 1)
        node2 = QuantumNode("TestNode 2", 2)
        conn = ClassicalConnection(1,2)

        # No arguments
        dq = DistributedQueue(node,conn)
        assert dq.node == node
        assert dq.conn == conn
        assert dq.myWsize == 100
        assert dq.master == True
        assert dq.otherWsize == 100
        assert len(dq.queueList) == 1
        assert dq.maxSeq == 2**32
        assert dq.status == dq.STAT_IDLE
        assert dq.comms_seq == 0
        assert dq.acksWaiting == 0
        assert len(dq.waitAddAcks) == 0
        assert dq.expectedSeq == 0 

        # No arguments, not controlling node
        dq = DistributedQueue(node2,conn)
        assert dq.node == node2
        assert dq.conn == conn
        assert dq.myWsize == 100
        assert dq.master == False
        assert dq.otherWsize == 100
        assert len(dq.queueList) == 1
        assert dq.maxSeq == 2**32
        assert dq.status == dq.STAT_IDLE
        assert dq.comms_seq == 0
        assert dq.acksWaiting == 0
        assert len(dq.waitAddAcks) == 0
        assert dq.expectedSeq == 0

        # Set arguments

        dq = DistributedQueue(node,conn, False, 1, 2, 3, 4)
        assert dq.node == node
        assert dq.conn == conn
        assert dq.master == False
        assert dq.myWsize == 1
        assert dq.otherWsize == 2
        assert len(dq.queueList) == 3
        assert dq.maxSeq == 4
        assert dq.status == dq.STAT_IDLE
        assert dq.comms_seq == 0
        assert dq.acksWaiting == 0
        assert len(dq.waitAddAcks) == 0
        assert dq.expectedSeq == 0 


    def test_add(self):

        # Set up two nodes and run a simulation in which items
        # are randomly added at specific time intervals
        pydynaa.DynAASim().reset()
        alice = QuantumNode("Alice",1, logger = self.logger)
        bob = QuantumNode("Bob",2, logger = self.logger)

        conn = ClassicalConnection(1,2)
        aliceDQ = DistributedQueue(alice, conn)
        bobDQ = DistributedQueue(bob, conn)

        aliceProto = TestProtocol(alice, aliceDQ,10)
        bobProto = TestProtocol(bob, bobDQ,10)

        alice.setup_connection(conn, classicalProtocol=aliceProto)
        bob.setup_connection(conn, classicalProtocol=bobProto)
        alice.start()
        bob.start()
        
        pydynaa.DynAASim().run(500)

        # Check the Queue contains ordered elements from Alice and Bob
        qA = aliceDQ.queueList[0].queue
        qB = bobDQ.queueList[0].queue

        # First they should have the same length
        assert len(qA) == len(qB)

        # Check the items are the same and the sequence numbers are ordered
        count = 0
        for k in range(len(qA)):
            assert qA[k].request == qB[k].request
            assert qA[k].seq == qB[k].seq
            assert qA[k].seq == count
            count = count + 1
            
            
if __name__ == "__main__":
    unittest.main()
