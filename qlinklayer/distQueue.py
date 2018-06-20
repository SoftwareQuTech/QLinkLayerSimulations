#
# Distributed Queue
#
# Implements a simple distributed queue shared with one other node over a connection.
#
# Author: Stephanie Wehner

from copy import copy
from collections import deque
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easyprotocol import EasyProtocol, ClassicalProtocol
from qlinklayer.localQueue import LocalQueue
from qlinklayer.general import LinkLayerException
from easysquid.toolbox import create_logger


logger = create_logger("logger")


class DistributedQueue(EasyProtocol, ClassicalProtocol):
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
    STAT_BUSY = 1  # Processing
    STAT_WAIT_HELLO = 2  # Sent a hello and wait for reply

    # Operation response
    DQ_OK = 0  # Operation OK
    DQ_TIMEOUT = 1  # Operation TIMEOUT
    DQ_REJECT = 2  # Operation REJECT
    DQ_ERR = 3  # Operation ERROR

    def __init__(self, node, connection=None, master=None, myWsize=100, otherWsize=100, numQueues=1, maxSeq=2 ** 32):

        super(DistributedQueue, self).__init__(node, connection)

        # Record the ID of this node
        self.myID = node.nodeID

        # Maximum sequence number 
        self.maxSeq = maxSeq

        # Determine ID of our peer (if we have a connection)
        self.otherID = self.get_otherID()

        # Flag to indicate whether we are the controlling node
        self.master = self._establish_master(master)

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

        # Current sequence number for making add requests (distinct from queue items)
        self.comms_seq = 0

        # Waiting for acks
        self.waitAddAcks = {}
        self.acksWaiting = 0

        # expected sequence number
        self.expectedSeq = 0

        self.add_callback = None

        self.myTrig = 0
        self.otherTrig = 0

    def _establish_master(self, master):
        if master is None and self.otherID:
            # Lowest ID gets to be master
            return self.myID < self.otherID

        else:
            return master

    def connect_to_peer_protocol(self, other_distQueue):
        if not self.conn:
            # Create a common connection
            self.conn = ClassicalFibreConnection(self.node, other_distQueue.node, length=1e-5)

        # Perform setup on both protocols
        self._connect_to_peer_protocol(self.conn)
        other_distQueue._connect_to_peer_protocol(self.conn)

    def _connect_to_peer_protocol(self, connection):
        self.setConnection(connection)
        self.otherID = self.get_otherID()
        self.master = self._establish_master(self.master)

    def process_data(self):
        # Fetch message from the other side
        [content, t] = self.conn.get_as(self.myID)
        if isinstance(content, tuple):
            for item in content:
                cmd = item[0]
                [data] = item[1:len(item)]

                self._process_cmd(cmd, data)

        else:
            cmd = content[0]
            [data] = content[1:len(content)]
            self._process_cmd(cmd, data)

    def _process_cmd(self, cmd, data):

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
        self.conn.put_from(self.myID, [[cmd, data]])

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
            logger.debug("Sending Hello")

    # CMD Handlers

    def cmd_HELLO(self, data):
        """
        Handle incoming Hello messages.
        """

        if self.status == self.STAT_IDLE:

            # We are in the idle state, just send a reply
            self.send_msg(self.CMD_HELLO, self.node.name)
            logger.debug("Hello received, replying")

        elif self.status == self.STAT_WAIT_HELLO:

            # We sent a hello ourselves, and this is the reply message, go back to idle.
            self.status = self.STAT_IDLE

            logger.debug("Hello Reply Received")

        else:
            # Unexpected message - this is an error
            logger.debug("Unexpected CMD_HELLO")
            self.send_error(self.CMD_ERR)

    def cmd_ERR(self, data):
        """
        Handle incoming error messages.
        """
        logger.debug("Error Received, Data: {}".format(data))
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
            self.send_error(self.CMD_ERR_UNKNOWN_ID)

        # Is the sequence number what we expected?
        if cseq > self.expectedSeq:

            # We seem to have missed some packets, for now just declare an error
            # TODO is this what we want?
            logger.debug("ADD ERROR Skipped sequence number from {} comms seq {} queue ID {} queue seq {}"
                         .format(nodeID, cseq, qid, qseq))
            self.send_error(self.CMD_ERR_MISSED_SEQ)
            return

        elif cseq < self.expectedSeq:

            # We have already seen this number
            # TODO is this what we want?
            logger.debug("ADD ERROR Duplicate sequence number from {} comms seq {} queue ID {} queue seq {}"
                         .format(nodeID, cseq, qid, qseq))
            self.send_error(self.CMD_ERR_DUPLICATE_SEQ)
            return

        # Is the queue ID acceptable?
        if not (self._valid_qid(qid)):
            logger.debug("ADD ERROR No such queue from {} comms seq {} queue ID {} queue seq {}"
                         .format(nodeID, cseq, qid, qseq))
            self.send_error(self.CMD_ERR_NOSUCH_Q)
            return

        # Is there such an item already in the queue?
        if not self.master:

            # Duplicate sequence number
            # TODO is this what we want?
            if self.queueList[qid].contains(qseq):
                logger.debug("ADD ERROR duplicate sequence number from {} comms seq {} queue ID {} queue seq {}"
                             .format(nodeID, cseq, qid, qseq))
                self.send_error(self.CMD_ERR_DUPLICATE_QSEQ)
                return

        # Is there a request supplied?
        if request is None:
            # Request details missing
            # TODO is this what we want?
            logger.debug("ADD ERROR missing request from {} comms seq {} queue ID {} queue seq {}"
                         .format(nodeID, cseq, qid, qseq))
            self.send_error(self.CMD_ERR_NOREQ)
            return

        # Received valid ADD: Process add request

        logger.debug("ADD from {} comms seq {} queue ID {} queue seq {}".format(nodeID, cseq, qid, qseq))

        # Increment next sequence number expected
        self.expectedSeq = (self.expectedSeq + 1) % self.maxSeq

        if self.master:
            # We are the node in control of the queue
            qseq = self._master_remote_add(nodeID, cseq, qid, request)

        else:

            # We are not in control, and must add as instructed
            self.queueList[qid].add_with_id(nodeID, qseq, request)

            # Send ack
            self.send_msg(self.CMD_ADD_ACK, [self.myID, cseq, 0])

        logger.debug("Distributed queue readying item ({}, {})".format(qid, qseq))
        self.queueList[qid].ready(qseq, 0)

        conn_delay = self.conn.channel_from_node(self.node).compute_delay()
        scheduleAfter = max(0, conn_delay + self.otherTrig - self.myTrig)
        self.queueList[qid].modify_schedule(qseq, scheduleAfter)

        if self.add_callback:
            self.add_callback((self.DQ_OK, qid, qseq, copy(request)))

    def cmd_ADD_ACK(self, data):
        """
        Handle incoming ack of an add request.
        """

        # Parse data
        [nodeID, ackd_id, qseq] = data

        # Sanity checking

        # Check whether this ack came from a partner node
        if nodeID != self.otherID:
            logger.debug("ADD ACK ERROR Unknown node {}".format(nodeID))
            self.send_error(self.CMD_ERR_UNKNOWN_ID)

        # Check we are indeed waiting for this ack
        # TODO refine error
        if ackd_id not in self.waitAddAcks:
            logger.debug("ADD ACK ERROR No such id from {} acking comms seq {} claiming queue seq {}"
                         .format(nodeID, ackd_id, qseq))
            self.send_error(self.CMD_ERR_UNKNOWN_ID)

        # Received valid ADD ACK

        # Check which queue id and which queue seq was ackd hereby
        # Note that if we are not the master node then we hold no prior queue id
        [qid, rec_qseq, request] = self.waitAddAcks[ackd_id]

        logger.debug("ADD ACK from {} acking comms seq {} claiming queue seq {}".format(nodeID, ackd_id, qseq))

        # Check whether we are in control of the queue
        if self.master:
            # We are in control

            # Mark this item as ready
            # TODO add time to be scheduled
            logger.debug("Distributed queue readying item ({}, {})".format(qid, rec_qseq))
            self.queueList[qid].ready(rec_qseq, 0)

        else:

            # We are not in control but merely hold a copy of the queue
            # We can now add
            self.queueList[qid].add_with_id(nodeID, qseq, request)

            # Mark this item as ready
            # TODO add time to be scheduled
            logger.debug("Distributed queue readying item ({}, {})".format(qid, qseq))
            self.queueList[qid].ready(qseq, 0)

        conn_delay = self.conn.channel_from_node(self.node).compute_delay()
        scheduleAfter = max(0, -(conn_delay + self.otherTrig - self.myTrig))
        logger.debug("Scheduling after {}".format(scheduleAfter))
        self.queueList[qid].modify_schedule(qseq, scheduleAfter)

        # Return the results if told to
        if self.add_callback:
            self.add_callback((self.DQ_OK, qid, qseq, copy(request)))

        # Remove item from waiting acks
        self.waitAddAcks.pop(ackd_id, None)

        # We are now waiting for one ack less
        self.acksWaiting = self.acksWaiting - 1

        # Process backlog, and go idle if applicable
        self._try_go_idle()

    # API to add to Queue

    def add(self, request, qid=0):
        """
        Add a request to create entanglement.
        """
        if (self.acksWaiting < self.myWsize) and (len(self.backlogAdd) == 0):

            # Still in window, and no backlog left to process, go add

            self._general_do_add(request, qid)

        else:

            # Add to backlog for later processing
            logger.debug("ADD to backlog")
            self.backlogAdd.append(request)

    def local_pop(self, qid=0):
        """
        Get top item from the queue locally if it is ready to be scheduled. This does NOT remove the item from the
        other side by design.
        
        Parameters
        ----------
        qid : int
            Queue ID (Default: 0)
        """

        if not (self._valid_qid(qid)):
            # Not a valid Queue ID
            raise LinkLayerException("Invalid Queue ID")

        return self.queueList[qid].pop()

    def local_peek(self, qid=0):
        """
        Get top item from the queue locally without removing it from the queue.
        :param qid:
        :return:
        """
        if not (self._valid_qid(qid)):
            # Not a valid Queue ID
            raise LinkLayerException("Invalid Queue ID")

        return self.queueList[qid].peek()

    def set_triggers(self, myTrig, otherTrig):
        self.myTrig = myTrig
        self.otherTrig = otherTrig

    def get_min_schedule(self, qid=0):
        return self.queueList[qid].get_min_schedule()

    # Internal helpers

    def _try_go_idle(self):
        """
        Go back to idle state, processing backlog if appropriate.
        """

        # Process backlog if applicable
        diff = self.myWsize - self.acksWaiting
        if diff > 0:

            # There's items on the backlog, and we can add maximum diff of them now
            canAdd = min(diff, len(self.backlogAdd))

            for j in range(canAdd):
                logger.debug("Processing backlog")
                oldRequest = self.backlogAdd.popleft()
                self._general_do_add(oldRequest)

        # If there are no outstanding acks, we can go back to being idle
        if self.acksWaiting == 0:
            self.status = self.STAT_IDLE

    def _valid_qid(self, qid):
        """
        Check whether qid is a valid queue identifier.

        Parameters
        ----------
        qid : int
            Queue ID
        """

        if (qid < 0) or (qid >= len(self.queueList)):
            return False

        if self.queueList[qid] is None:
            return False

        return True

    def _master_remote_add(self, nodeID, cseq, qid, request):
        """
        Process request to add to queue from remote if this is the master node.
        """

        # Add to the queue and get queue sequence number 
        # TODO introduce waiting
        queue_seq = self.queueList[qid].add(self.myID, request)

        # Send ack
        self.send_msg(self.CMD_ADD_ACK, [self.myID, cseq, queue_seq])

        return queue_seq

    def _general_do_add(self, request, qid=0):

        # Check if we are the master node in control of the queue
        # and perform the appropriate actions to add the item

        if self.master:
            self._master_do_add(request, qid)

        else:
            # We are not in control of the queue and must make a request to the other side
            # first
            self._request_add(request, qid)

    def _master_do_add(self, request, qid):
        """
        Master node: Perform addition to queue as master node, assuming we are cleared to do so.
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

        self.status = self.STAT_BUSY

    def _request_add(self, request, qid):
        """
        Non-master node: Request addition by contacting the node in control of the queue.
        """

        # Send an add message to the other side
        self.send_msg(self.CMD_ADD, [self.myID, self.comms_seq, qid, 0, request])

        # Mark that we are waiting for an ack for this
        self.waitAddAcks[self.comms_seq] = [qid, 0, request]

        # Increment acks we are waiting for
        self.acksWaiting = self.acksWaiting + 1

        # Increment our own sequence number of this request to add
        self.comms_seq = (self.comms_seq + 1) % self.maxSeq

        self.status = self.STAT_BUSY
