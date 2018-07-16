#
# Distributed Queue
#
# Implements a simple distributed queue shared with one other node over a connection.
#
# Author: Stephanie Wehner

import logging
from collections import deque
from easysquid.connection import ClassicalConnection
from easysquid.qnode import QuantumNode
from easysquid.easyprotocol import EasyProtocol
from easysquid.toolbox import EasySquidException, setup_logging
from netsquid.simutil import sim_reset, sim_run
from qlinklayer.localQueue import LocalQueue


class DistributedQueue(EasyProtocol):
    """
    Simple distributed queue protocol.
    """

    # Possible messages sent in protocol
    CMD_HELLO = 0  # Check connection to the other side for testing
    CMD_ERR = 1  # Error
    CMD_ADD = 2  # Request to add item
    CMD_ADD_ACK = 3  # Ack of add
    CMD_ADD_REJ = 4  # Reject addition of item
    CMD_ERR_UNKNOWN_ID = 5  # Unknown node ID
    CMD_ERR_MISSED_SEQ = 6  # Missing sequence number
    CMD_ERR_DUPLICATE_SEQ = 7  # Duplicate comms sequence number
    CMD_ERR_NOSUCH_Q = 8  # No such queue number
    CMD_ERR_DUPLICATE_QSEQ = 9  # Duplicate queue sequence number
    CMD_ERR_NOREQ = 10  # Request data missing

    # States of this protocol
    STAT_IDLE = 0  # Default idle state
    STAT_WAIT_HELLO = 1  # Sent a hello and wait for reply

    def __init__(self, node, connection, master=None, myWsize=100, otherWsize=100, numQueues=1, maxSeq=2 ^ 32 - 1):

        super(DistributedQueue, self).__init__(node, connection)

        # Record the ID of this node
        self.myID = node.nodeID

        # Maximum sequence number 
        self.maxSeq = maxSeq

        # Determine the ID of the other node
        if self.conn.idA == self.myID:
            self.otherID = self.conn.idB
        elif self.conn.idB == self.myID:
            self.otherID = self.conn.idA
        else:
            raise EasySquidException("Attempt to run hello protocol at remote nodes")

        # Set up command handlers
        self.commandHandlers = {
            self.CMD_HELLO: self.cmd_HELLO,
            self.CMD_ERR: self.cmd_ERR,
            self.CMD_ADD: self.cmd_ADD,
            self.CMD_ADD_ACK: self.cmd_ADD_ACK,
            self.CMD_ERR_UNKNOWN_ID: self.cmd_ERR,
            self.CMD_ERR_MISSED_SEQ: self.cmd_ERR,
            self.CMD_ERR_DUPLICATE_SEQ: self.cmd_ERR,
            self.CMD_ERR_NOSUCH_Q: self.cmd_ERR,
            self.CMD_ERR_DUPLICATE_QSEQ: self.cmd_ERR,
            self.CMD_ERR_NOREQ: self.cmd_ERR
        }

        # The initial state is idle
        self.status = self.STAT_IDLE

        # Flag to indicate whether we are the controlling node
        if master is None:
            if self.myID < self.otherID:
                # We are the master node responsible for the queue
                self.master = True
            else:
                # We are not responsible for the queue
                self.master = False
        else:
            self.master = master

        # Window size for us and the other node
        self.myWsize = myWsize
        self.otherWsize = otherWsize

        # Initialize queues
        self.queueList = []
        for j in range(numQueues):
            q = LocalQueue()
            self.queueList.append(q)

        # Backlog of requests
        self.backlogAdd = deque()

        # Number of items added since last time we allowed the other node to
        # go ahead first - relevant for master node only
        self.addedNumber = 0

        # Current sequence number for making add requests (distinct from queue items)
        self.comms_seq = 0

        # Waiting for acks
        self.waitAddAcks = {}
        self.acksWaiting = 0

        # expected sequence number
        self.expectedSeq = 0

    def process_data(self):

        # Fetch message from the other side
        [content, t] = self.conn.get_as(self.myID)

        if isinstance(content, tuple):
            for item in content:
                print(item)
                cmd = item[0]
                [data] = item[1:len(item)]

                self._process_cmd(cmd, data)

        else:
            cmd = content[0]
            [data] = content[1:len(content)]
            self._process_cmd(cmd, data)

    def _process_cmd(self, cmd, data):

        print(cmd)
        print(data)
        # First, let's check its of the right form, which is [message type, more data]
        if cmd is None or data is None:
            self.send_error(self.CMD_ERR)
            return

        # Process message demanding on command
        self.commandHandlers[cmd](data)

    def send_msg(self, cmd, data):
        """
        Send message to the other side

        Parameters
        ----------
        cmd : int
            Command type as specified in protocol.
        data : object
            Data to be sent
        """
        self.conn.put_from(self.myID, classical=[[cmd, data]])

    def send_error(self, error):
        """
        Send error message to the other side.

        Parameters
        ----------
        error : int
            Error code
        """
        self.send_msg(error, 0)

    def send_hello(self):
        """
        Sends a hello to the other side for testing.
        """

        if self.status == self.STAT_IDLE:
            # We are in the idle state, just send hello
            self.status = self.STAT_WAIT_HELLO
            self.send_msg(self.CMD_HELLO, 0)
            self.node.log_debug("Sending Hello")

    # CMD Handlers

    def cmd_HELLO(self, data):
        """
        Handle incoming Hello messages.
        """

        if self.status == self.STAT_IDLE:

            # We are in the idle state, just send a reply
            self.send_msg(self.CMD_HELLO, self.node.name)
            self.node.log_debug("Hello received, replying")

        elif self.status == self.STAT_WAIT_HELLO:

            # We sent a hello ourselves, and this is the reply message, go back to idle.
            self.status = self.STAT_IDLE

            self.node.log_debug("Hello Reply Received")

        else:
            # Unexpected message - this is an error
            self.node.log_debug("Unexpected CMD_HELLO")
            self.send_error(self.CMD_ERR)

        pass

    def cmd_ERR(self, data):
        """
        Handle incoming error messages.
        """
        self.node.log_debug("Error Received")
        self.status = self.STAT_IDLE

    def cmd_ADD(self, data):
        """
        Handle incoming add request.
        """

        # Parse data
        [nodeID, cseq, qid, qseq, request] = data

        # Sanity checking

        # Check whether the request is from our partner node
        if nodeID != self.otherID:
            self.send_err(self.CMD_ERR_UNKNOWN_ID)

        # Is the sequence number what we expected?
        if cseq > self.expectedSeq:

            # We seem to have missed some packets, for now just declare an error
            # TODO
            self.send_err(self.CMD_ERR_MISSED_SEQ)

        elif cseq < self.expectedSeq:

            # We have already seen this number
            # TODO
            self.send_err(self.CMD_ERR_DUPLICATE_SEQ)

        # Is the queue ID acceptable?
        if (qid < 0) or (qid >= len(self.queueList)):
            self.send_err(self.CMD_ERR_NOSUCH_Q)

        if self.queueList[qid] is None:
            self.send_err(self.CMD_ERR_NOSUCH_Q)

        # Is there such an item already in the queue?
        if not (self.master):
            if self.queueList[qid].contains(qseq):
                self.send_err(self.CMD_ERR_DUPLICATE_QSEQ)

        # Is there a request supplied?
        if request is None:
            self.send_err(self.CMD_ERR_NOREQ)

        # Process add request

        self.node.log_debug("Recv valid ADD from " + str(nodeID) + " comms seq " + str(cseq) + " queue ID " + str(
            qid) + " queue seq " + str(qseq))

        if self.master:
            # We are the node in control of the queue
            self._remote_add(nodeID, cseq, qid, request)

        else:

            # We are not in control, and must add as instructed
            self.queueList[qid].add_with_id(nodeID, qseq, request)

            # Send ack
            self.send_msg(self.CMD_ADD_ACK, [self.myID, cseq, 0])

    def cmd_ADD_ACK(self, data):
        """
        Handle incoming ack of an add request.
        """

        # Parse data
        [nodeID, ackd_id, qseq] = data

        # Sanity checking

        # Check whether this ack came from a partner node
        if nodeID != self.otherID:
            self.send_err(self.CMD_ERR_UNKNOWN_ID)

        # Check we are indeed waiting for this ack
        # TODO refine error
        if not (ackd_id in self.waitAddAcks):
            self.send_err(self.CMD_ERR_UNKNOWN_ID)

        # Check which queue id and which queue seq was ackd hereby
        # Note that if we are not the master node then we hold no prior queue id
        [qid, rec_qseq, request] = self.waitAddAcks[ackd_id]

        self.node.log_debug("Recv valid ACK ADD from " + str(nodeID) + " acking comms seq " + str(
            ackd_id) + " claiming queue seq " + str(qseq))

        # Check whether we are in control of the queue
        if self.master:
            # We are in control

            # Mark this item as ready
            # TODO add time to be scheduled
            self.queueList[qid].ready(rec_qseq, 0)

        else:

            # We are not in control but merely hold a copy of the queue
            # We can now add
            self.queueList[qid].add_with_id(nodeID, qseq, request)

            # Mark this item as ready
            # TODO add time to be scheduled
            self.queueList[qid].ready(qseq, 0)

        # Remove item from waiting acks
        self.waitAddAcks.pop(ackd_id, None)

        # We are now waiting for one ack less
        self.acksWaiting = self.acksWaiting - 1

    # API to add to Queue

    def add(self, request):
        """
        Add a request to create entanglement.
        """

        # For now we use only one queue
        qid = 0

        # Check if we are the master node in control of the queue
        # and perform the appropriate actions to add the item

        if self.master:
            # We are the node in control of the queue

            # Check how many we added before letting the slave node add items
            if self.acksWaiting < self.myWsize:

                # We have added less than until we are required to switch. We can go ahead
                # and add
                self._do_add(qid, request)

            else:

                # Add to backlog to be be processed later
                self.backlogAdd.append(request)

        else:
            # We are not in control of the queue and must make a request to the other side
            # first

            # Check if we are still waiting for outstanding acks
            if self.acksWaiting < self.myWsize:

                # We can proceed
                self._request_add(qid, request)

            else:

                # Add to the backlog to be processed later
                self.backlogAdd.append(request)

    def _remote_add(self, nodeID, cseq, qid, request):
        """
        Process request to add to queue from remote as master node.
        """
        # Add to the queue and get queue sequence number 
        # TODO introduce waiting
        queue_seq = self.queueList[qid].add(self.myID, request)

        # Send ack
        self.send_msg(self.CMD_ADD_ACK, [self.myID, cseq, queue_seq])

    def _do_add(self, qid, request):
        """
        Perform addition to queue as master node, assuming we are cleared to do so.
        """

        # Add to the queue and get queue sequence number 
        queue_seq = self.queueList[qid].add(self.myID, request)

        # Send an add message to the other side
        self.send_msg(self.CMD_ADD, [self.myID, self.comms_seq, qid, queue_seq, request])

        # Mark that we are waiting for an ack for this
        self.waitAddAcks[self.comms_seq] = [qid, queue_seq, request]

        # Record waiting ack
        self.acksWaiting = self.acksWaiting + 1

        # Increment our own sequence number of this request to add
        self.comms_seq = (self.comms_seq + 1) % self.maxSeq

    def _request_add(self, qid, request):
        """
        Request addition by contacting the node in control of the queue.
        """

        # Send an add message to the other side
        self.send_msg(self.CMD_ADD, [self.myID, self.comms_seq, qid, 0, request])

        # Mark that we are waiting for an ack for this
        self.waitAddAcks[self.comms_seq] = [qid, 0, request]

        # Increment acks we are waiting for
        self.acksWaiting = self.acksWaiting + 1

        # Increment our own sequence number of this request to add
        self.comms_seq = (self.comms_seq + 1) % self.maxSeq


sim_reset()
logger = setup_logging("GLOBAL", "logFile", level=logging.DEBUG)
alice = QuantumNode("Alice", 1, logger=logger)
bob = QuantumNode("Bob", 2, logger=logger)

conn = ClassicalConnection(1, 2)
aliceProto = DistributedQueue(alice, conn)
bobProto = DistributedQueue(bob, conn)

alice.setup_connection(2, conn, classicalProtocol=aliceProto)
bob.setup_connection(1, conn, classicalProtocol=bobProto)
alice.start()
bob.start()

aliceProto.send_hello()

aliceProto.add("Foo")

sim_run(500)
