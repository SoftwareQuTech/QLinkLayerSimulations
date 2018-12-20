#
# Distributed Queue
#
# Implements a simple distributed queue shared with one other node over a connection.
#
# Authors: Stephanie Wehner, Matthew Skrzypczyk, Axel Dahlberg

from copy import copy
from collections import deque, defaultdict
from functools import partial
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easyprotocol import EasyProtocol, ClassicalProtocol
from netsquid.pydynaa import EventType, EventHandler
from qlinklayer.localQueue import TimeoutLocalQueue, EGPLocalQueue, WFQLocalQueue
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
    CMD_ERR_REJ = 11  # Rejected add request

    # States of this protocol
    STAT_IDLE = 0  # Default idle state
    STAT_BUSY = 1  # Processing
    STAT_WAIT_HELLO = 2  # Sent a hello and wait for reply

    # Operation response
    DQ_OK = 0  # Operation OK
    DQ_TIMEOUT = 1  # Operation TIMEOUT
    DQ_REJECT = 2  # Operation REJECT
    DQ_ERR = 3  # Operation ERROR

    def __init__(self, node, connection=None, master=None, myWsize=100, otherWsize=100, numQueues=1, maxSeq=2 ** 8,
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
            self.CMD_ERR_NOREQ: self.cmd_ERR,
            self.CMD_ERR_REJ: self.cmd_ERR_REJ
        }

        # Set up add validators
        self.add_validators = [
            self._validate_otherID,
            self._validate_comms_seq,
            self._validate_qid,
            self._validate_aid,
            self._validate_request
        ]

        # The initial state is idle
        self.status = self.STAT_IDLE

        # Window size for us and the other node
        self.myWsize = myWsize
        self.otherWsize = otherWsize

        # Initialize queues
        self._init_queues(numQueues, maxSeq=maxSeq, throw_local_queue_events=throw_local_queue_events)

        # Backlog of requests
        self.backlogAdd = deque()

        # Current sequence number for making add requests (distinct from queue items)
        self.comms_seq = 0
        self.comm_delay = 0
        self.timeout_factor = 2
        self.max_add_attempts = 3

        # Track the absolute queue ID we transmitted for the corresponding comms_seq
        self.transmitted_aid = {}

        # Waiting for acks
        self.waitAddAcks = {}
        self.addAckBacklog = [deque() for i in range(numQueues)]
        self.acksWaiting = 0
        self.comm_timeout_handler = None
        self._EVT_COMM_TIMEOUT = EventType("COMM TIMEOUT", "Communication timeout")

        # expected sequence number
        self.expectedSeq = 0
        self.add_callback = None

    def _init_queues(self, numQueues=1, maxSeq=2 ** 8, throw_local_queue_events=False):
        """
        Initializes the local queues
        :param numQueues: int
            Number of queues
        :param maxSeq: int
            Max number of elements in the queues
        :param throw_local_queue_events: bool
            Whether the local queues should throw events for data collection or not
        :return:
        """
        # Initialize queues
        self.queueList = []
        self.numLocalQueues = numQueues
        for j in range(numQueues):
            q = TimeoutLocalQueue(qid=j, maxSeq=maxSeq, throw_events=throw_local_queue_events)
            self.queueList.append(q)

    def _establish_master(self, master):
        """
        Establishes the role of master/slave for the local DQP based on info about the other DQP.  If a user
        has defined their DQP to be the master then use this, otherwise rely on lowest nodeID.
        :param master: bool or None
            User defined assignment for master role
        :return: bool
            Whether we are master or not
        """
        if master is None and self.otherID is not None:
            # Lowest ID gets to be master
            return self.myID < self.otherID

        else:
            return master

    def connect_to_peer_protocol(self, other_distQueue, conn=None):
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
        if conn is None:
            # Create a common connection
            conn = ClassicalFibreConnection(self.node, other_distQueue.node, length=1e-5)

        # Perform setup on both protocols
        self.establish_connection(conn)
        other_distQueue.establish_connection(conn)

    def establish_connection(self, connection):
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

    def schedule_comm_timeout(self, ack_id):
        """
        Schedules a communication timeout event and attaches a handler that resets the protocol
        :return:
        """
        self.comm_timeout_handler = EventHandler(partial(self._comm_timeout_handler, ack_id=ack_id))

        if not self.comm_delay:
            self.comm_delay = self.conn.channel_from_A.compute_delay() + self.conn.channel_from_B.compute_delay()

        logger.debug("Scheduling communication timeout event after {}.".format(self.timeout_factor * self.comm_delay))
        evt = self._schedule_after(self.timeout_factor * self.comm_delay, self._EVT_COMM_TIMEOUT)
        self._wait_once(self.comm_timeout_handler, event=evt)

    def _comm_timeout_handler(self, evt, ack_id):
        """
        DQP Communication timeout handler.  Triggered after we have waited too long for a response from our peer.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered the timeout handler
        :param ack_id: int
            The comms_seq number corresponding to the communication that timed out
        """
        if ack_id in self.waitAddAcks:
            logger.warning("Timed out waiting for communication response for comms_seq {}!".format(ack_id))
            qid, queue_seq, request, num_attempts = self.waitAddAcks.get(ack_id)

            # Check if we exceeded number of allowable attempts
            if num_attempts >= self.max_add_attempts:
                logger.warning("Exceeded maximum number of add attempts, removing item")

                # Remove the item from the local queue
                self.waitAddAcks.pop(ack_id)
                if self.master:
                    self.queueList[qid].remove_item(queue_seq)

                # If our peer failed to add our item we should remove any Acks we should provide for
                # subsequent items they attempted to add
                if self.has_subsequent_acks(qid=qid, qseq=queue_seq):
                    self.reject_outstanding_acks(qid)

                # Pass error information upwards
                if self.add_callback:
                    self.add_callback(result=(self.DQ_TIMEOUT, qid, queue_seq, request))

            else:
                # Otherwise retransmit the add message
                logger.debug("Retransmitting ADD for comms seq {} qid {} qseq {}".format(ack_id, qid, queue_seq))

                # Update the number of attempts
                self.waitAddAcks[ack_id] = [qid, queue_seq, request, num_attempts + 1]

                # Construct the add message using the same comms seq as used originally
                add_msg = (self.myID, ack_id, qid, queue_seq, request)

                # Send and setup a communication timeout to retry
                self.send_msg(self.CMD_ADD, add_msg)
                self.schedule_comm_timeout(ack_id=ack_id)

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
        self.conn.put_from(self.myID, [(cmd, data)])

    def send_ADD_ACK(self, cseq, qseq, qid=0):
        """
        Sends an add ack to the other side.
        Also calls post processing function
        :param cseq: int
            Communication sequence number
        :param qseq: int
            Queue item sequence number
        :param qid: int
            The queue ID (not part of the message but used for post_processing
        :return: None
        """
        self.send_msg(self.CMD_ADD_ACK, (self.myID, cseq, qseq))
        self._post_process_send_ADD_ACK(qid, qseq)

    def _post_process_send_ADD_ACK(self, qid, qseq):
        """
        Entry point (to be overridden) for post processing queue items that were added by the remote node
        :param qid: int
            Queue ID
        :param qseq: int
            Queue item sequence number
        :return: None
        """
        pass

    def send_error(self, error, error_data=0):
        """
        Send error message to the other side.

        Parameters
        ----------
        error : int
            Error code
        """
        self.send_msg(error, error_data)

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

    def cmd_ERR_REJ(self, data):
        """
        Handle rejected add requests.
        :param data: tuple of (ack_id, qid, qseq, request)
            The data that was rejected by the other node
        """
        [ack_id, qid, qseq, request] = data
        logger.warning("ADD ERROR REJECT from {} comms seq {} queue ID {} queue seq {}"
                       .format(self.otherID, ack_id, qid, qseq))

        if self.queueList[qid].contains(qseq) and self.master:
            self.remove_item(qid, qseq)

        self.waitAddAcks.pop(ack_id)
        self.acksWaiting -= 1
        if self.add_callback:
            self.add_callback(result=(self.DQ_REJECT, qid, qseq, request))

    def validate_ADD(self, data):
        """
        Applies all validation filters on incoming ADD request for queue item
        :param data: tuple of (int, int, int, int, obj `~qlinklayer.egp.EGPRequest)
            Contains the nodeID, comms_seq, qid, qseq, and request for the queue item
        :return: bool
            Whether or not data passes validation
        """
        # Apply all validators and fail early
        for validator in self.add_validators:
            if not validator(data):
                return False

        return True

    def _validate_otherID(self, data):
        """
        Checks that the data was submitted by the remote node on our connection
        :param data: Validation data
        :return: bool
            If data passes validation
        """
        [nodeID, cseq, qid, qseq, request] = data
        if nodeID != self.otherID:
            logger.debug("ADD ERROR Got ADD request from node that isn't our peer!")
            self.send_error(self.CMD_ERR_UNKNOWN_ID)
            return False
        return True

    def _validate_comms_seq(self, data):
        """
        Validates the communication sequence number in data
        :param data: Validation data
        :return: bool
            If data passes validation
        """
        [nodeID, cseq, qid, qseq, request] = data

        # We got what we expected or a message may have been lost
        if cseq >= self.expectedSeq:
            if cseq > self.expectedSeq:
                logger.warning("Got comms seq {} ahead of expected seq, updating".format(cseq))
            # Update our expectedSeq as necessary
            self.expectedSeq = (cseq + 1) % self.maxSeq
            return True

        # A message was delayed or retransmitted upon loss
        elif cseq < self.expectedSeq:
            logger.warning("Got comms seq {} behind our expected seq".format(cseq))

            # If we have not seen this before we should add the item to the queue normally
            if (not self.contains_item(qid, qseq) and not self.master) or \
                    (self.master and cseq not in self.transmitted_aid):
                logger.warning("Adding request")
                return True

            # If we have seen this comms_seq before retransmit the absolute queue id
            elif cseq in self.transmitted_aid:
                tqseq, tqid = self.transmitted_aid[cseq]
                logger.debug("Retransmitting ADD ACK for comms seq {} queue ID {} queue seq {}"
                             .format(cseq, tqid, tqseq))
                self.send_ADD_ACK(cseq, tqseq, tqid)
                return False

            else:
                # We have already seen this number
                # TODO is this what we want?
                logger.debug("ADD ERROR Duplicate sequence number from {} comms seq {} queue ID {} queue seq {}"
                             .format(nodeID, cseq, qid, qseq))
                self.send_error(self.CMD_ERR_DUPLICATE_SEQ)
                return False

        return True

    def _validate_qid(self, data):
        """
        Validates the qid in data
        :param data: Validation data
        :return: bool
            If data passes validation
        """
        [nodeID, cseq, qid, qseq, request] = data
        if not (self._valid_qid(qid)):
            logger.debug("ADD ERROR No such queue from {} comms seq {} queue ID {} queue seq {}"
                         .format(nodeID, cseq, qid, qseq))
            self.send_error(self.CMD_ERR_NOSUCH_Q)
            return False

        elif self.is_full(qid):
            logger.debug("ADD ERROR from {} comms seq {} queue ID {} is full!".format(nodeID, cseq, qid))
            self.send_msg(self.CMD_ERR_REJ, (cseq, qid, qseq, request))
            return False

        return True

    def _validate_aid(self, data):
        """
        Validates the absolute queue id in data
        :param data: Validation data
        :return: bool
            If data passes validation
        """
        [nodeID, cseq, qid, qseq, request] = data
        if not self.master:
            # Duplicate sequence number
            # TODO is this what we want?
            if self.contains_item(qid, qseq):
                logger.debug("ADD ERROR duplicate sequence number from {} comms seq {} queue ID {} queue seq {}"
                             .format(nodeID, cseq, qid, qseq))
                self.send_error(self.CMD_ERR_DUPLICATE_QSEQ)
                return False
        return True

    def _validate_request(self, data):
        """
        Validates that request exists in data
        :param data: Validation data
        :return: bool
            If data passes validation
        """
        [nodeID, cseq, qid, qseq, request] = data
        if request is None:
            # Request details missing
            # TODO is this what we want?
            logger.debug("ADD ERROR missing request from {} comms seq {} queue ID {} queue seq {}"
                         .format(nodeID, cseq, qid, qseq))
            self.send_error(self.CMD_ERR_NOREQ)
            return False
        return True

    def cmd_ADD(self, data):
        """
        Handle incoming add request.
        """
        # Parse data
        [nodeID, cseq, qid, qseq, request] = data

        # Sanity checking
        if not self.validate_ADD(data):
            return

        if self.master:
            # We are the node in control of the queue
            qseq = self._master_remote_add(nodeID, cseq, qid, request)

        else:
            # We are not in control, and must add as instructed
            self.queueList[qid].add_with_id(nodeID, qseq, request)

            # Send ack
            self.send_ADD_ACK(cseq, qseq, qid)

        # Received valid ADD: Process add request
        logger.debug("ADD from {} comms seq {} queue ID {} queue seq {}".format(nodeID, cseq, qid, qseq))

        # Store the absolute queue ID under the comms seq in case the message is lost
        self.store_transmitted_info(cseq, qid, qseq)

        # Post process
        self._post_process_cmd_ADD(qid, qseq)

        if self.add_callback:
            self.add_callback((self.DQ_OK, qid, qseq, copy(request)))

    def store_transmitted_info(self, cseq, qid, qseq):
        """
        Stores a mapping between the comms seq and absolute queue id temporarily in case ack message
        is lost and peer attempts re-adding
        :param cseq: int
            The comms seq corresponding to the absolute queue id
        :param qid: int
            The local queue id where the item was added
        :param qseq: int
            The sequence number in the local queue where the item was added
        """
        self.transmitted_aid[cseq] = (qseq, qid)

        # Set up a handler to clear the stored data when we believe our peer got the ack
        evt = self._schedule_after(self.max_add_attempts * self.timeout_factor * self.comm_delay,
                                   self._EVT_COMM_TIMEOUT)
        handler = EventHandler(partial(self.clear_transmitted_info, cseq=cseq))
        self._wait_once(handler, event=evt)

    def clear_transmitted_info(self, evt, cseq):
        """
        Clears local information mapping comms sequence to transmitted absolute queue id
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered the handler
        :param cseq: int
            The comms sequence to clear temporary info for
        """
        # Remove if still available locally
        if cseq in self.transmitted_aid:
            aid = self.transmitted_aid.pop(cseq)
            logger.debug("Clearing transmitted queue id {} for comms seq {}".format(aid, cseq))

    def _post_process_cmd_ADD(self, qid, qseq):
        """
        Entry point (to be overridden) for post processing queue items that were added by the remote node
        :param qid: int
            (Local) Queue ID where the item will be added
        :param qseq: int
            Sequence number within local queue of item
        """
        pass

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
            return

        # Received valid ADD ACK

        # Check which queue id and which queue seq was ackd hereby
        # Note that if we are not the master node then we hold no prior queue id
        [qid, rec_qseq, request, _] = self.waitAddAcks[ackd_id]

        # Check whether we are in control of the queue
        if self.master:
            # We are in control
            logger.debug("ADD ACK from {} acking comms seq {} claiming queue seq {}".format(nodeID, ackd_id, rec_qseq))
            agreed_qseq = rec_qseq

            # If this is the last queue item before our backlog of the slave's request then ready/schedule and send an
            # ack to the slave
            if self.has_subsequent_acks(qid, agreed_qseq):
                self.release_acks(qid)

        else:
            logger.debug("ADD ACK from {} acking comms seq {} claiming queue seq {}".format(nodeID, ackd_id, qseq))

            # We are not in control but merely hold a copy of the queue
            # We can now add
            self.queueList[qid].add_with_id(nodeID, qseq, request)
            agreed_qseq = qseq

        self._post_process_cmd_ADD_ACK(qid, agreed_qseq)

        # Return the results if told to
        if self.add_callback:
            self.add_callback((self.DQ_OK, qid, agreed_qseq, copy(request)))

        # Remove item from waiting acks
        self.waitAddAcks.pop(ackd_id, None)

        # We are now waiting for one ack less
        self.acksWaiting = self.acksWaiting - 1

        # Process backlog, and go idle if applicable
        self._try_go_idle()

    def _post_process_cmd_ADD_ACK(self, qid, qseq):
        """
        Entry point (to be overridden) for post processing queue items that were acknowledged by the remote node
        :param qid: int
            (Local) Queue ID where the item will be added
        :param qseq: int
            Sequence number within local queue of item
        """
        pass

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
        if self.addAckBacklog[qid]:
            cseq, next_qid, next_qseq, request = self.addAckBacklog[qid][0]
            return next_qid == qid and next_qseq == qseq + 1
        else:
            return False

    def reject_outstanding_acks(self, qid):
        """
        Deletes an ACK we should provide and any subsequent ACKs
        """
        logger.warning("Rejecting outstanding acks")
        # Grab item info, ack, and schedule
        cseq, qid, qseq, request = self.addAckBacklog[qid].popleft()
        self.send_msg(self.CMD_ERR_REJ, (cseq, qid, qseq, request))

        # Check if the following item(s) belong to the same queue and release them as well
        while self.has_subsequent_acks(qid=qid, qseq=qseq):
            # Check if this item is a subsequent item in the same queue
            cseq, qid, qseq, request = self.addAckBacklog[qid].popleft()
            self.send_msg(self.CMD_ERR_REJ, (cseq, qid, qseq, request))

    def release_acks(self, qid):
        """
        Releases a stored ack and any stored items that are subsequent queue items
        """
        # Grab item info, ack, and schedule
        cseq, qid, qseq, request = self.addAckBacklog[qid].popleft()
        self.send_ADD_ACK(cseq, qseq, qid)
        self._post_process_release(qid, qseq)

        # Check if the following item(s) belong to the same queue and release them as well
        while self.has_subsequent_acks(qid=qid, qseq=qseq):
            cseq, qid, qseq, request = self.addAckBacklog[qid].popleft()
            self.send_ADD_ACK(cseq, qseq, qid)
            self._post_process_release(qid, qseq)

    def _post_process_release(self, qid, qseq):
        """
        Entry point (to be overridden) for post processing queue items that had ADD_ACKs just released by master
        :param qid: int
            (Local) Queue ID where the item will be added
        :param qseq: int
            Sequence number within local queue of item
        """
        self.queueList[qid].ack(qseq)

    def has_queue_id(self, qid):
        """
        Returns True if queue ID exists otherwise False
        :param qid:
        :return:
        """
        try:
            self.queueList[qid]
            return True
        except IndexError:
            return False

    # API to add to Queue

    def add(self, request, qid=0):
        """
        Add a request to create entanglement.
        """
        if not self.has_queue_id(qid):
            if self.add_callback:
                logger.warning("Tried to add to non-existing queue ID")
                self.add_callback(result=(self.DQ_REJECT, qid, 0, request))
                return

        if self.is_full(qid):
            logger.error("Specified local queue is full, cannot add request")
            if self.add_callback:
                self.add_callback(result=(self.DQ_ERR, qid, request))
            raise LinkLayerException()

        if (self.acksWaiting < self.myWsize) and (len(self.backlogAdd) == 0):
            # Still in window, and no backlog left to process, go add
            self._general_do_add(request, qid)

        else:
            # Add to backlog for later processing
            logger.debug("ADD to backlog")
            self.backlogAdd.append((request, qid))

    def contains_item(self, qid, qseq):
        """
        Checks if this distributed queue contains an item in the specified qid, qseq combo
        :param qid: int
            The local queue id to check
        :param qseq: int
            The sequence number within the local queue
        :return: bool
            Whether we have the item or not
        """
        if qid < len(self.queueList):
            return self.queueList[qid].contains(qseq)
        return False

    def is_full(self, qid):
        """
        Checks whether the specified qid is full
        :param qid: int
            The local queue ID to check
        :return: bool
            True/False
        """

        return self.queueList[qid].is_full()

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

    def local_peek(self, qid=0, qseq=None):
        """
        Get top item from the queue locally without removing it from the queue.
        :param qid: int or tuple(int, int)
            The queue ID or aid
        :param qseq: int
            The queue sequence number
        :return: :obj:`~_LocalQueueItem`
            The corresponding queue item
        """
        # Check if first argument is actually the full aid
        if isinstance(qid, tuple):
            return self.local_peek(qid[0], qid[1])

        if not (self._valid_qid(qid)):
            # Not a valid Queue ID
            raise LinkLayerException("Invalid Queue ID")

        return self.queueList[qid].peek(seq=qseq)

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
                oldRequest, qid = self.backlogAdd.popleft()
                self._general_do_add(oldRequest, qid)

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
        if not self.waitingForAcks(qid):
            self.send_ADD_ACK(cseq, queue_seq, qid)

        # Otherwise wait on a response for our ADDs we have in flight before we ack
        else:
            self.addAckBacklog[qid].append((cseq, qid, queue_seq, request))

        return queue_seq

    def waitingForAcks(self, qid):
        for data in self.waitAddAcks.values():
            if qid == data[0]:
                return True

        return False

    def _general_do_add(self, request, qid=0):

        # Check if we are the master node in control of the queue
        # and perform the appropriate actions to add the item
        # Check if the queue is full
        if self.is_full(qid):
            logger.error("Specified local queue is full, cannot add request")
            if self.add_callback:
                self.add_callback(result=(self.DQ_ERR, qid, request))
            return

        logger.debug("{} Adding new item to queue".format(self.node.nodeID))
        if self.master:
            self._master_do_add(request, qid)

        else:
            # We are not in control of the queue and must make a request to the other side
            # first
            self._request_add(request, qid)

    def _construct_add_msg(self, qid, qseq, req):
        """
        Constructs the message data when performing ADD/ADD_ACKs
        :param qid: int
            (Local) Queue ID where the item will be stored
        :param qseq: int
            Sequence number within Local Queue corresponding to item
        :param req: obj
            The request item to be stored in the queue
        :return:
        """
        return (self.myID, self.comms_seq, qid, qseq, req)

    def _master_do_add(self, request, qid):
        """
        Master node: Perform addition to queue as master node, assuming we are cleared to do so.
        """
        # Add to the queue and get queue sequence number
        queue_seq = self.queueList[qid].add(self.myID, request)

        # Send an add message to the other side
        add_msg = self._construct_add_msg(qid, queue_seq, copy(request))
        self.send_msg(self.CMD_ADD, add_msg)
        logger.debug("{} Communicated absolute queue id ({}, {}) to slave".format(self.node.nodeID, qid, queue_seq))

        # Mark that we are waiting for an ack for this, store attempt 1 for initial transmission
        self.waitAddAcks[self.comms_seq] = [qid, queue_seq, request, 1]
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
        add_msg = self._construct_add_msg(qid, 0, copy(request))
        self.send_msg(self.CMD_ADD, add_msg)
        logger.debug("{} Sent ADD request to master".format(self.node.nodeID))

        # Mark that we are waiting for an ack for this, store attempt 1 for initial transmission
        self.waitAddAcks[self.comms_seq] = [qid, 0, request, 1]
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


class FilteredDistributedQueue(DistributedQueue):
    def __init__(self, node, connection=None, master=None, myWsize=100, otherWsize=100, numQueues=1, maxSeq=2 ** 32,
                 throw_local_queue_events=False, accept_all=False):
        """
        A queue that supports filtering out requests based on rules.  This base implementation simply filters
        EGPRequests based on the attached purpose_id (treated like a port)
        :param node: obj `~easysquid.qnode.QuantumNode`
            The node attached to this distributed queue
        :param connection: obj `~easysquid.easyfibre.ClassicalFibreConnection`
            The connection to perform communication over
        :param master: bool
            Whether we are the master or not
        :param myWsize: int
            Window size for queue item handling
        :param otherWsize: int
            Remote window size
        :param numQueues: int
            Number of local queues composing the distributed queue
        :param maxSeq: int
            Maximum sequence number
        :param throw_local_queue_events: bool
            Specifies whether to schedule local queue events
        :param accept_all: bool
            Specifies whether to accept all requests by default
        """
        super(FilteredDistributedQueue, self).__init__(node=node, connection=connection, master=master, myWsize=myWsize,
                                                       otherWsize=otherWsize, numQueues=numQueues, maxSeq=maxSeq,
                                                       throw_local_queue_events=throw_local_queue_events)

        # Request accept/reject rules
        self.accept_all = accept_all
        self.accept_rules = defaultdict(set)

        # Add rule validator
        self.add_validators.append(self._validate_acceptance)

    def add_accept_rule(self, nodeID, purpose_id):
        """
        Adds a queue item ADD rule
        :param nodeID: int
            The nodeID of the remote node to accept adds from
        :param purpose_id: int
            The purpose id of the queue item
        """
        self.accept_rules[nodeID].add(purpose_id)

    def remove_accept_rule(self, nodeID, purpose_id):
        """
        Removes a queue item ADD rule
        :param nodeID: int
            The nodeID of the remote node to accept adds from
        :param purpose_id: int
            The purpose id of the queue item
        """
        try:
            self.accept_rules[nodeID].remove(purpose_id)
        except KeyError:
            logger.error("Attempted to remove nonexistent rule for node {} purpose id {}".format(nodeID, purpose_id))

    def load_accept_rules(self, accept_rules):
        """
        Loads a dictionary of add rules
        :param accept_rules: dict of k=int, v=list
            A dictionary of nodeID to list of accepted purpose ids for queue items originating from the nodeIDs
        """
        self.accept_rules.update(accept_rules)

    def _validate_acceptance(self, data):
        """
        Validates the acceptance of the queue item data
        :param data: Validation data
        :return: bool
            If data passes validation
        """
        [nodeID, cseq, qid, qseq, request] = data
        # Are we accepting request adds from this peer?
        if not self.accept_all and request.purpose_id not in self.accept_rules[nodeID]:
            logger.debug("ADD ERROR not accepting requests with purpose id {} from node {}".format(request.purpose_id,
                                                                                                   nodeID))
            self.send_msg(self.CMD_ERR_REJ, (cseq, qid, qseq, request))
            return False

        return True


class EGPDistributedQueue(FilteredDistributedQueue):
    def __init__(self, node, connection=None, master=None, myWsize=100, otherWsize=100, numQueues=1, maxSeq=2 ** 32,
                 throw_local_queue_events=False, accept_all=False, timeout_callback=None):
        """
        A distributed queue that implements EGP specific features (MHP trigger offsets, MHP cycle time, etc.)
        :param node: obj `~easysquid.qnode.QuantumNode`
            The node this queue belongs to
        :param connection: obj `~easysquid.easyfibre.classicalFibreConnection`
            The connection that communication with peer queue occurs over
        :param master: bool
            Specifies whether this queue is the master
        :param myWsize: int
            Local processing window size
        :param otherWsize: int
            Remote processing window size
        :param numQueues: int
            Number of local queues composing the distributed queue
        :param maxSeq: int
            Max sequence number allowable in the queues
        :param throw_local_queue_events: bool
            Enables local queue events
        :param accept_all: bool
            Specifies acceptance of all add requests
        :param timeout_callback: func
            Function to be called upon timeout, taking an _LocalQueueItem as argument
        """
        # Function to be called upon timeout, taking an _LocalQueueItem as argument
        self.timeout_callback = timeout_callback

        super(EGPDistributedQueue, self).__init__(node=node, connection=connection, master=master, myWsize=myWsize,
                                                  otherWsize=otherWsize, numQueues=numQueues, maxSeq=maxSeq,
                                                  throw_local_queue_events=throw_local_queue_events,
                                                  accept_all=accept_all)

    def set_timeout_callback(self, timeout_callback):
        """
        Sets the timeout callback function for timeout of queue items
        :param timeout_callback: func
            Function to be called upon timeout, taking an _LocalQueueItem as argument
        :return:
        """
        for q in self.queueList:
            q.timeout_callback = timeout_callback

    def _init_queues(self, numQueues=1, maxSeq=2 ** 8, throw_local_queue_events=False):
        """
        Initializes the local queues
        :param numQueues: int
            Number of queues
        :param throw_local_queue_events: bool
            Whether the local queues should throw events for data collection or not
        :return:
        """
        # Initialize queues
        self.queueList = []
        self.numLocalQueues = numQueues
        for j in range(numQueues):
            q = EGPLocalQueue(qid=j, maxSeq=maxSeq, throw_events=throw_local_queue_events,
                              timeout_callback=self.timeout_callback)
            self.queueList.append(q)

    def update_mhp_cycle_number(self, current_cycle, max_cycle):
        """
        Goes over the elements in all the queue and checks if they are ready to be scheduled or have timed out.
        :return: None
        """
        logger.debug("Updating to MHP cycle {}".format(current_cycle))
        for queue in self.queueList:
            queue.update_mhp_cycle_number(current_cycle, max_cycle)

    def _post_process_send_ADD_ACK(self, qid, qseq):
        """
        Entry point (to be overridden) for post processing queue items that were added by the remote node
        :param qid: int
            (Local) Queue ID where the item will be added
        :param qseq: int
            Sequence number within local queue of item
        """
        self.queueList[qid].ack(qseq)

    def _post_process_cmd_ADD_ACK(self, qid, qseq):
        """
        Entry point (to be overridden) for post processing queue items that were acknowledged by the remote node
        :param qid: int
            (Local) Queue ID where the item will be added
        :param qseq: int
            Sequence number within local queue of item
        """
        self.queueList[qid].ack(qseq)


class WFQDistributedQueue(EGPDistributedQueue):
    def _init_queues(self, numQueues=1, maxSeq=2 ** 8, throw_local_queue_events=False):
        """
        Initializes the local queues
        :param numQueues: int
            Number of queues
        :param throw_local_queue_events: bool
            Whether the local queues should throw events for data collection or not
        :return:
        """
        # Initialize queues
        self.queueList = []
        self.numLocalQueues = numQueues
        for j in range(numQueues):
            q = WFQLocalQueue(qid=j, maxSeq=maxSeq, throw_events=throw_local_queue_events,
                              timeout_callback=self.timeout_callback)
            self.queueList.append(q)
