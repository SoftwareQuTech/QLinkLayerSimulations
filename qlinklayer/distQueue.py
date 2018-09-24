#
# Distributed Queue
#
# Implements a simple distributed queue shared with one other node over a connection.
#
# Author: Stephanie Wehner

from copy import copy
from collections import deque
from functools import partial
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easyprotocol import EasyProtocol, ClassicalProtocol
from netsquid.pydynaa import EventType, EventHandler
from netsquid.simutil import sim_time
from qlinklayer.localQueue import TimeoutLocalQueue
from qlinklayer.toolbox import LinkLayerException
from easysquid.toolbox import logger


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

    def __init__(self, node, connection=None, master=None, myWsize=100, otherWsize=100, numQueues=1, maxSeq=2 ** 32,
                 throw_local_queue_events=False):

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

        # Queue item management
        self.timed_out_items = []
        # self.queue_item_timeout_handler = EventHandler(self._queue_item_timeout_handler)
        self._EVT_QUEUE_TIMEOUT = EventType("DIST QUEUE TIMEOUT", "Triggers when queue item times out")
        self.ready_items = []
        self.last_schedule_time = 0
        self.schedule_item_handler = EventHandler(self._schedule_item_handler)
        self._EVT_SCHEDULE = EventType("DIST QUEUE SCHEDULE", "Triggers when a queue item is ready to be scheduled")

        # Initialize queues
        self.queueList = []
        self.numLocalQueues = numQueues
        for j in range(numQueues):
            q = TimeoutLocalQueue(qid=j, throw_events=throw_local_queue_events)
            self.queueList.append(q)
            # self._wait(self.queue_item_timeout_handler, entity=q, event_type=q._EVT_PROC_TIMEOUT)
            self._wait(self.schedule_item_handler, entity=q, event_type=q._EVT_SCHEDULE)

        # Backlog of requests
        self.backlogAdd = deque()

        # Current sequence number for making add requests (distinct from queue items)
        self.comms_seq = 0
        self.comm_delay = 0

        # Waiting for acks
        self.waitAddAcks = {}
        self.addAckBacklog = deque()
        self.acksWaiting = 0
        self.comm_timeout_handler = None
        self._EVT_COMM_TIMEOUT = EventType("COMM TIMEOUT", "Communication timeout")

        # expected sequence number
        self.expectedSeq = 0

        self.add_callback = None

        self.myTrig = 0
        self.otherTrig = 0

    def _establish_master(self, master):
        """
        Establishes the role of master/slave for the local DQP based on info about the other DQP.  If a user
        has defined their DQP to be the master then use this, otherwise rely on lowest nodeID.
        :param master: bool or None
            User defined assignment for master role
        :return: bool
            Whether we are master or not
        """
        if master is None and self.otherID:
            # Lowest ID gets to be master
            return self.myID < self.otherID

        else:
            return master

    def connect_to_peer_protocol(self, other_distQueue, conn=None, scheduling_offsets=None):
        """
        Connects to a peer DQP.  Sets up a default connection if none specified to be used.  If scheduling_offsets is
        provided then the DQPs will also configure scheduling delay information.
        :param other_distQueue: obj `~qlinklayer.distQueue.DistributedQueue`
            The peer distributed queue that we want to connect with
        :param conn: obj `~easysquid.connection.Connection`
            The connection to use for communication
        :param scheduling_offsets: dict
            Contains (nodeID, offset) information
        """
        if not conn:
            # Create a common connection
            conn = ClassicalFibreConnection(self.node, other_distQueue.node, length=1e-5)

        # Perform setup on both protocols
        self.establish_connection(conn, scheduling_offsets)
        other_distQueue.establish_connection(conn, scheduling_offsets)

    def establish_connection(self, connection, scheduling_offsets=None):
        """
        Sets up the internal connection and configures the master/slave relationship for the queue and configures
        and scheduling delay info
        :param connection: obj `~easysquid.connection.Connection`
            The communication connection used by the distributed queue
        :param scheduling_offsets: dict
            Contains (nodeID, offset) information
        """
        self.setConnection(connection)
        self.otherID = self.get_otherID()
        self.master = self._establish_master(self.master)
        self.comm_delay = self.conn.channel_from_A.compute_delay() + self.conn.channel_from_B.compute_delay()

        # Set the triggers for scheduling delays
        if scheduling_offsets:
            myTrig = scheduling_offsets[self.node.nodeID]
            otherTrig = scheduling_offsets[self.otherID]
            self.set_triggers(myTrig=myTrig, otherTrig=otherTrig)

    def schedule_comm_timeout(self, ack_id):
        """
        Schedules a communication timeout event and attaches a handler that resets the protocol
        :return:
        """
        self.comm_timeout_handler = EventHandler(partial(self._comm_timeout_handler, ack_id=ack_id))

        if not self.comm_delay:
            self.comm_delay = self.conn.channel_from_A.compute_delay() + self.conn.channel_from_B.compute_delay()

        logger.debug("Scheduling communication timeout event after {}.".format(10 * self.comm_delay))
        self.comm_timeout_event = self._schedule_after(10 * self.comm_delay, self._EVT_COMM_TIMEOUT)
        self._wait_once(self.comm_timeout_handler, event=self.comm_timeout_event)

    def _comm_timeout_handler(self, evt, ack_id):
        """
        DQP Communication timeout handler.  Triggered after we have waited too long for a response from our peer.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered the timeout handler
        """
        if ack_id in self.waitAddAcks:
            logger.warning("Timed out waiting for communication response for comms_seq {}!".format(ack_id))

            # Remove the item from the local queue
            qid, queue_seq, request = self.waitAddAcks.pop(ack_id)
            self.queueList[qid].remove_item(queue_seq)

            # If our peer failed to add our item we should remove any Acks we should provide for
            # subsequent items they attempted to add
            if self.has_subsequent_acks(qid=qid, qseq=queue_seq):
                self.delete_outstanding_acks()

            # Pass error information upwards
            if self.add_callback:
                self.add_callback(result=(self.DQ_TIMEOUT, qid, queue_seq, request))

    # def _queue_item_timeout_handler(self, evt):
    #     """
    #     DQP Queue Item timeout handler.  Triggered when an underlying TimedLocalQueue has expired a queue item
    #     due to it not being serviced within it's max time period.
    #     :param evt: obj `~netsquid.pydynaa.Event`
    #         The event that triggered this handler
    #     """
    #     # Get the local queue that triggered the event
    #     queue = evt.source
    #     logger.debug("Handling local queue item timeout")
    #
    #     # Pull the timed out item from the local queue's internal storage
    #     queue_item = queue.timed_out_items.pop(0)
    #     logger.debug("Got timed out queue item {}".format(queue_item))
    #
    #     # Set up a timeout event for passing to higher layers
    #     self.timed_out_items.append(queue_item)
    #     logger.debug("Scheduling queue timeout event now.")
    #     self._schedule_now(self._EVT_QUEUE_TIMEOUT)

    def _schedule_item_handler(self, evt):
        """
        Handler for queue items that are ready to be scheduled.  Bubbles the event up to higher layers.
        :param evt: obj `~netsquid.pydynaa.Event`
            The schedule event that triggered this handler
        """
        logger.debug("{} Schedule handler triggered in dist queue".format(self.node.nodeID))
        logger.debug("Scheduling schedule event now.")
        queue = evt.source
        qid = queue.qid
        queue_item = queue.ready_items.pop(0)
        self.ready_items.append((qid, queue_item))
        self._schedule_now(self._EVT_SCHEDULE)

    def process_data(self):
        """
        Processes incoming messages and forwards them to the appropriate handlers
        """
        # Fetch message from the other side
        [content, t] = self.conn.get_as(self.myID)
        for item in content:
            if len(item) != 2:
                raise ValueError("Unexpected format of classical message.")
            cmd = item[0]
            data = item[1]

            self._process_cmd(cmd, data)

    def _process_cmd(self, cmd, data):
        """
        Processes commands received from our peer
        :param cmd: int
            The identifier for the command our peer wants us to execute
        :param data: obj any
            Data associated for execution of the requested command
        """
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

            # Only respond and allow the slave to add this item if they have responded to the add requests that come in
            # the queue before this item
            if not self.waitAddAcks:
                # Schedule the item to be ready
                conn_delay = self.conn.channel_from_node(self.node).delay_mean
                scheduleAfter = max(0, conn_delay + self.myTrig - self.otherTrig)
                self.ready_and_schedule(qid, qseq, scheduleAfter)

        else:

            # We are not in control, and must add as instructed
            self.queueList[qid].add_with_id(nodeID, qseq, request)

            # Send ack
            self.send_msg(self.CMD_ADD_ACK, [self.myID, cseq, 0])

            # Schedule the item to be ready
            conn_delay = self.conn.channel_from_node(self.node).delay_mean
            scheduleAfter = max(0, conn_delay + self.myTrig - self.otherTrig)
            self.ready_and_schedule(qid, qseq, scheduleAfter)

        # Add a timeout on the queue item
        self.queueList[qid].add_scheduling_event(qseq)

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

        # Check whether we are in control of the queue
        if self.master:
            # We are in control
            logger.debug("ADD ACK from {} acking comms seq {} claiming queue seq {}".format(nodeID, ackd_id, rec_qseq))
            agreed_qseq = rec_qseq

        else:
            logger.debug("ADD ACK from {} acking comms seq {} claiming queue seq {}".format(nodeID, ackd_id, qseq))
            # We are not in control but merely hold a copy of the queue
            # We can now add
            self.queueList[qid].add_with_id(nodeID, qseq, request)
            agreed_qseq = qseq

        # Schedule the item
        conn_delay = self.conn.channel_from_node(self.node).delay_mean
        scheduleAfter = max(0, -(conn_delay + self.otherTrig - self.myTrig))
        self.ready_and_schedule(qid, agreed_qseq, scheduleAfter)

        # Add a timeout on the queue item
        self.queueList[qid].add_scheduling_event(agreed_qseq)

        # Return the results if told to
        if self.add_callback:
            self.add_callback((self.DQ_OK, qid, agreed_qseq, copy(request)))

        # Remove item from waiting acks
        self.waitAddAcks.pop(ackd_id, None)

        # We are now waiting for one ack less
        self.acksWaiting = self.acksWaiting - 1

        # If this is the last queue item before our backlog of the slave's request then ready/schedule and send an
        # ack to the slave
        if self.has_subsequent_acks(qid, agreed_qseq):
            self.release_acks()

        # Process backlog, and go idle if applicable
        self._try_go_idle()

    def has_subsequent_acks(self, qid, qseq):
        """
        Given a qid and qseq, checks if the addAckBacklog contains a queue item subsequent to this one
        :param qid: int
            Queue ID to check addAckBacklog for
        :param qseq: int
            Queue sequence number to check if there are subsequent items for
        :return: bool
            Whether the addAckBacklog contains queue items that are subsequent to the specified info
        """
        if self.addAckBacklog:
            cseq, next_qid, next_qseq = self.addAckBacklog[0]
            return next_qid == qid and next_qseq == qseq + 1
        else:
            return False

    def delete_outstanding_acks(self):
        """
        Deletes an ACK we should provide and any subsequent ACKs
        """
        # Grab item info, ack, and schedule
        cseq, qid, qseq = self.addAckBacklog.popleft()

        # Check if the following item(s) belong to the same queue and release them as well
        while self.has_subsequent_acks(qid=qid, qseq=qseq):
            # Check if this item is a subsequent item in the same queue
            cseq, qid, qseq = self.addAckBacklog.popleft()

    def release_acks(self):
        """
        Releases a stored ack and any stored items that are subsequent queue items
        """
        # Grab item info, ack, and schedule
        cseq, qid, qseq = self.addAckBacklog.popleft()
        self.send_msg(self.CMD_ADD_ACK, [self.myID, cseq, qseq])

        conn_delay = self.conn.channel_from_node(self.node).delay_mean
        scheduleAfter = max(0, conn_delay + self.otherTrig - self.myTrig)
        self.ready_and_schedule(qid, qseq, scheduleAfter)

        # Check if the following item(s) belong to the same queue and release them as well
        while self.has_subsequent_acks(qid=qid, qseq=qseq):
            cseq, qid, qseq = self.addAckBacklog.popleft()
            self.send_msg(self.CMD_ADD_ACK, [self.myID, cseq, qseq])
            self.ready_and_schedule(qid, qseq, scheduleAfter)

    def ready_and_schedule(self, qid, qseq, schedule_after):
        """
        Readies an item in the queue and schedules it for retrieval
        :param qid: int
            Queue ID that the item belongs to
        :param qseq: int
            Sequence identifier within the specified queue
        :param schedule_after: float
            Delay before the queue item is ready to be retrieved
        """
        logger.debug("Distributed queue readying item ({}, {})".format(qid, qseq))
        self.queueList[qid].ready(qseq, 0)

        now = sim_time()

        if now + schedule_after <= self.last_schedule_time:
            schedule_after += self.comm_delay

        self.last_schedule_time = now + schedule_after

        logger.debug("{} Scheduling after {}".format(self.node.nodeID, schedule_after))
        self.queueList[qid].schedule_item(qseq, schedule_after)

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

    def remove_item(self, qid, qseq):
        """
        Removes the specified item from our local portion of the distributed queue
        :param qid: int
            The ID of the LocalQueue to remove the item from
        :param qseq: int
            The Sequence Number of the item in the specified queue to remove
        :return: obj
            The request that we removed if any otherwise None
        """
        # Check if the QID specified is valid
        if qid < 0 or qid >= self.numLocalQueues:
            logger.error("Invalid QID {} selected when specifying item removal".format(qid))

        # Attempt to remove the item from the specified local queue
        removed_item = self.queueList[qid].remove_item(qseq)

        # Check if we actually removed anything
        if removed_item is not None:
            logger.debug("Successfully removed queue item ({}, {}) from distributed queue".format(qid, qseq))

        else:
            logger.debug("Failed to remove queue item ({}, {}) from distributed queue".format(qid, qseq))

        return removed_item

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
        """
        Sets delay triggers to be used for coordinating queue item scheduling.  Allows higher protocols
        to have queue items synchronized.
        :param myTrig: float
            Time offset of protocols on our end
        :param otherTrig: float
            Time offset of protocols on the peer's end
        """
        self.myTrig = myTrig
        self.otherTrig = otherTrig

    def get_min_schedule(self, qid=0):
        """
        Returns the min_schedule for the specified local queue
        :param qid: int
            Queue ID of the local queue to get the min_schedule for
        :return:
        """
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
        queue_seq = self.queueList[qid].add(self.myID, request)

        # Check if we are waiting for any acks from the slave
        if not self.waitAddAcks:
            self.send_msg(self.CMD_ADD_ACK, [self.myID, cseq, queue_seq])

        # Otherwise wait on a response for our ADDs we have in flight before we ack
        else:
            self.addAckBacklog.append((cseq, qid, queue_seq))

        return queue_seq

    def _general_do_add(self, request, qid=0):

        # Check if we are the master node in control of the queue
        # and perform the appropriate actions to add the item

        logger.debug("{} Adding new item to queue".format(self.node.nodeID))
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
        self.send_msg(self.CMD_ADD, [self.myID, self.comms_seq, qid, queue_seq, copy(request)])
        logger.debug("{} Communicated absolute queue id ({}, {}) to slave".format(self.node.nodeID, qid, queue_seq))

        # Mark that we are waiting for an ack for this
        self.waitAddAcks[self.comms_seq] = [qid, queue_seq, request]
        self.schedule_comm_timeout(ack_id=self.comms_seq)
        logger.debug("{} Added waiting item in ADD ACKS list: {}".format(self.node.nodeID, [qid, queue_seq, request]))

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
        self.send_msg(self.CMD_ADD, [self.myID, self.comms_seq, qid, 0, copy(request)])
        logger.debug("{} Sent ADD request to master".format(self.node.nodeID))

        # Mark that we are waiting for an ack for this
        self.waitAddAcks[self.comms_seq] = [qid, 0, request]
        self.schedule_comm_timeout(ack_id=self.comms_seq)
        logger.debug("{} Added waiting item in ADD ACKS list: {}".format(self.node.nodeID, [qid, 0, request]))

        # Increment acks we are waiting for
        self.acksWaiting = self.acksWaiting + 1

        # Increment our own sequence number of this request to add
        self.comms_seq = (self.comms_seq + 1) % self.maxSeq

        self.status = self.STAT_BUSY

    def _reset_data(self):
        """
        Resets the variables storing the data for data collection
        :return:
        """
        self._last_aid_added = None
