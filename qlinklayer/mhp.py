import abc
from collections import defaultdict
from easysquid.services import Service, TimedServiceProtocol
from easysquid.simpleLink import NodeCentricMHP
from easysquid.easyfibre import HeraldedFibreConnection
from easysquid.toolbox import EasySquidException, create_logger
from netsquid.qubits.qubitapi import create_qubits
from netsquid.pydynaa import DynAASim, EventType, EventHandler

logger = create_logger("logger")


class MHPMessage:
    def __init__(self, classical_data=None, quantum_data=None):
        """
        Message object for abstracting the passed information between MHP nodes and the heralding station
        :param classical_data: obj any
            Classical data of the message
        :param quantum_data: obj or list of `~netsquid.qubits.qubit.Qubit`
            Quantum data part of the message
        """
        self.classical = classical_data
        self.quantum = quantum_data


class MHPRequest(MHPMessage):
    def __init__(self, request_data=None, pass_data=None, quantum_data=None):
        """
        Request messages send to the heralding station
        :param request_data: obj any
            Request specification interpretted by heralding station
        :param pass_data: obj any
            Information to pass through to other end of heralded connection
        :param quantum_data: obj or list of `~netsquid.qubits.qubit.Qubit`
            Qubit(s) being sent to the heralding station
        """
        classical_data = (request_data, pass_data)
        super(MHPRequest, self).__init__(classical_data=classical_data, quantum_data=quantum_data)

    @property
    def request_data(self):
        """
        Returns information stored in request data
        :return: Request data of the message
        """
        return self.classical[0] if self.classical else None

    @property
    def pass_data(self):
        """
        Returns information stored in pass data
        :return: Data to be passed that is included in the message
        """
        return self.classical[1] if self.classical else None

    @property
    def quantum_data(self):
        """
        Returns the quantum information corresponding to this message
        :return: obj or list of `~netsquid.qubits.qubit.Qubit`
            The qubit(s) part of the message
        """
        return self.quantum if self.quantum else None

    def channel_data(self):
        """
        Returns the message in item form that works with calls to `~netsquid.components.connections` "put_from"
        :return: list of [classical_data, quantum_data]
        """
        return [(self.request_data, self.pass_data), self.quantum_data]


class MHPReply(MHPMessage):
    def __init__(self, response_data=None, pass_data=None, quantum_data=None):
        """
        Response message sent from heralding station to connected nodes
        :param response_data: obj any
            The response message interpretted by the MHP nodes
        :param pass_data: obj any
            Information that was passed through by peer on the connection
        :param quantum_data: obj or list of `~netsquid.qubits.qubit.Qubit`
            Qubits sent back from the heralding station
        """
        classical_data = (response_data, pass_data)
        super(MHPReply, self).__init__(classical_data=classical_data, quantum_data=quantum_data)

    @property
    def response_data(self):
        """
        Returns the information stored in the response data
        :return: Response data that is part of the message
        """
        return self.classical[0] if self.classical else None

    @property
    def pass_data(self):
        """
        Returns information stored in pass data
        :return: Data to be passed that is included in the message
        """
        return self.classical[1] if self.classical else None

    @property
    def quantum_data(self):
        """
        Returns the quantum information corresponding to this message
        :return: obj or list of `~netsquid.qubits.qubit.Qubit`
            The qubit(s) part of the message
        """
        return self.quantum if self.quantum else None

    def channel_data(self):
        """
        Returns the message in item form that works with calls to `~netsquid.components.connections` "put_from"
        :return: list of [classical_data, quantum_data]
        """
        return [(self.response_data, self.pass_data), self.quantum_data]


class MHPHeraldedConnection(HeraldedFibreConnection):
    """
    Generic connection to be used with MHP protocols, to be overloaded
    """
    # Production outcomes
    VALID_OUTCOMES = [0, 1, 2]

    def __init__(self, *args, **kwargs):
        self.node_requests = {}
        self.mhp_seq = 0
        self.max_seq = 2**32 - 1
        super(MHPHeraldedConnection, self).__init__(*args, **kwargs)

    def _handle_cq(self, classical, qubit, sender):
        """
        Handles the Classical/Quantum messages from either end of the connection
        :param classical: obj any
            Classical data sent by sender
        :param qubit: list `~netsquid.qubits.qubit.Qubit`
            The qubits sent by the sender
        :param sender: int
            NodeID of the sender of the information
        :return: None
        """
        logger.debug("Handling CQ from {}, got classical: {} and qubit {}".format(sender, classical, qubit))

        # Check whether we are in time window
        if not self._in_window:
            logger.warning("Received CQ out of detection time window")
            # Outside window, drop qubit
            self._drop_qubit(qubit)

            # Notify out of window error
            self._send_notification_to_one(self.ERR_OUT_OF_WINDOW, sender)
            self._reset_incoming()
            return

        incoming_request = self._construct_request(sender, classical, qubit)
        self._process_incoming_request(sender, incoming_request)

    @abc.abstractmethod
    def _construct_request(self, sender, classical, qubit):
        """
        Reconstructs a MHPRequest object from the classical/qubit data sent by a node.  To be overloaded
        :param sender: int
            NodeID of the sender of the data
        :param classical: obj any
            Classical data associated with this message
        :param qubit: obj or list of `~netsquid.qubits.qubit.Qubit`
            Qubits sent from the nodes
        """
        pass

    @abc.abstractmethod
    def _process_incoming_request(self, sender, request):
        """
        Processes an incoming request from a node.  To be overloaded
        :param sender: int
            NodeID of the request sender
        :param request: obj `~qlinklayer.mhp.MHPRequest`
            Request object containing the request_data, pass_data, and quantum data to be processed
        """
        pass

    def _send_notification_to_one(self, notification_type, receiver):
        """
        Sends a notification to one end of the connection
        :param msg: obj any
            The message to send to the receiver
        :param receiver: int
            The node ID of the receiver of the message
        """
        channel_data = self._get_notification_data(notification_type, receiver)

        if channel_data is None:
            raise EasySquidException("Missing control data")

        # Send messages back to the node
        if receiver == self.idA:
            self.channel_M_to_A.put(channel_data)
        elif receiver == self.idB:
            self.channel_M_to_B.put(channel_data)
        else:
            raise EasySquidException("Unknown receiver")

    @abc.abstractmethod
    def _get_notification_data(self, notification_type, receiver):
        """
        Given a notification type to be sent to the receiver, constructs the corresponding data. To be overloaded
        :param notification_type: int
            ID of the notification type we are sending
        :param receiver: int
            NodeID of the node receiving the notification
        """
        pass

    def _send_notification_to_both(self, outcome):
        """
        Sends a notification to both ends of the connection, usually after _do_swap
        :param outcome: int
            Outcome result of performing the entanglement
        :return: Non
        """
        logger.debug("{} sending notification to both".format(DynAASim().current_time))

        # Send notification messages back.
        if outcome not in self.VALID_OUTCOMES:
            logger.debug("Sending error information to both")
            dataA, dataB = self._get_error_data(outcome)

        else:
            logger.debug("Sending generation outcome information to both")
            dataA, dataB = self._get_outcome_data(outcome)

        # Make sure we have something to send
        if dataA is None or dataB is None:
            raise EasySquidException("Missing control data.")

        # Send messages back to the nodes
        logger.debug("Sending messages to A: {} and B: {}".format(dataA, dataB))
        self._send_to_node(self.nodeA, dataA)
        self._send_to_node(self.nodeB, dataB)

        if outcome in [1, 2]:
            self.mhp_seq = self._get_next_mhp_seq()
            logger.debug("New MHP Sequence Number is {}".format(self.mhp_seq))

        else:
            logger.debug("Entanglement failed at heralding station")

    def _send_to_node(self, node, data):
        """
        Sends data out from the heralding station to a connected end node
        :param node: obj `~easysquid.qnode.QuantumNode`
            The node we want to send the data to
        :param data: obj any
            The data to place on the channel to the node
        """
        if node.nodeID == self.nodeA.nodeID:
            self.channel_M_to_A.put(data)

        elif node.nodeID == self.nodeB.nodeID:
            self.channel_M_to_B.put(data)

        else:
            raise EasySquidException("Tried to send to unconnected node")

    def _get_next_mhp_seq(self):
        """
        Computes the next MHP Sequence number we should be using
        :return: int
            The next MHP Sequence number to send for a successful entanglement result
        """
        return (self.mhp_seq + 1) % self.max_seq

    @abc.abstractmethod
    def _get_outcome_data(self, outcome):
        """
        Given the outcome of an entanglement attempt, constructs data to be sent to nodes
        :param outcome: int
            Status code of the entanglement outcome
        """
        return [], []


class NodeCentricMHPHeraldedConnection(MHPHeraldedConnection):
    """
    Node Centric Middle Heralded Protocol Connection
    """
    # Commands
    CMD_PRODUCE = 0
    CMD_ERR = 1
    CMD_INFO = 2

    # Errors
    ERR_QUEUE_MISMATCH = 11

    def _construct_request(self, sender, classical, qubit):
        """
        Reconstructs a MHPRequest object from the classical/qubit data sent by a node.
        :param sender: int
            NodeID of the sender of the data
        :param classical: obj any
            Classical data associated with this message
        :param qubit: obj or list of `~netsquid.qubits.qubit.Qubit`
            Qubits sent from the nodes
        :return: obj `~qlinklayer.mhp.MHPRequest`
            Contains request information from node
        """
        request_data, pass_data, quantum_data = self._extract_cq_data(sender, classical, qubit)
        request = MHPRequest(request_data=request_data, pass_data=pass_data, quantum_data=quantum_data)
        return request

    def _extract_cq_data(self, sender, classical, qubit):
        """
        Translates the incoming cq data from the connection into the request data, pass data, and quantum data
        :param sender: int
            NodeID of the cq sender
        :param classical: obj any
            Classical data sent
        :param qubit: obj or list of `~netsquid.qubits.qubit.Qubit`
            Qubit(s) sent by the sender
        :return: Extracted data for reconstructing an MHPRequest object
        """
        request_data, pass_data = classical
        quantum_data = qubit
        self.classical_data[sender] = pass_data
        self.qubits[sender] = qubit
        return request_data, pass_data, quantum_data

    def _get_notification_data(self, notification_type, receiver):
        """
        Given a notification type to be sent to the receiver, constructs the corresponding data
        :param notification_type: int
            ID of the notification type we are sending
        :param receiver: int
            NodeID of the node receiving the notification
        :return: tuple of (resp, pass)
        """
        peer = self.nodeB.nodeID if receiver == self.nodeA.nodeID else self.nodeA.nodeID
        logger.debug("Getting notification data to send to {}, has peer {}".format(receiver, peer))
        notification_data = {
            self.CMD_INFO: self.node_requests[peer].pass_data
        }

        return [[notification_type, notification_data.get(notification_type)]]

    def _get_outcome_data(self, outcome):
        """
        Given the outcome of an entanglement attempt, constructs data to be sent to nodes
        :param outcome: int
            Status code of the entanglement outcome
        :return: tuples of (respM, passM) to each of the nodes connected
        """
        requestA = self.node_requests.get(self.nodeA.nodeID)
        pass_AM = requestA.pass_data
        aid_A = pass_AM[1]
        pass_BM = self.node_requests[self.nodeB.nodeID].pass_data
        aid_B = pass_BM[1]

        resp_MA = (outcome, self.mhp_seq, aid_A)
        resp_MB = (outcome, self.mhp_seq, aid_B)

        respA = MHPReply(response_data=resp_MA, pass_data=pass_BM)
        respB = MHPReply(response_data=resp_MB, pass_data=pass_AM)
        return respA.channel_data(), respB.channel_data()

    def _get_error_data(self, err):
        """
        Collects error information to be propagated upwards.  If the provided err is ERR_GENERAL then we try
        to look at the state of the heralding station to discover what the error is.  If a specific error is provided
        then we already know what happened and simply pass this on.
        :param err: obj any
            Error information to pass
        :return: Error data to send to both nodes
        """
        # Check if we need to analyze the station's state to discover the error
        if err == self.ERR_GENERAL:
            proto_err = self._discover_error()

        # Otherwise pass whatever was discovered
        else:
            proto_err = err

        # Construct the reply
        data = MHPReply(response_data=self.ERR_GENERAL, pass_data=proto_err).channel_data()

        # Return the data that should go on the channel
        logger.debug("Sending error messages to A and B ({})".format(data))
        return data, data

    def _discover_error(self):
        """
        Analyzes the state of the heralding station to attempt to diagnose the error that occurred.
        :return:
        """
        err = []
        # Check if we received classical information from the endnodes
        if None in self.node_requests.values():
            err.append(self.ERR_NO_CLASSICAL_OTHER)

        # Default to a general error
        if not err:
            return self.ERR_GENERAL

        # Return a single error if only one otherwise return a list of errors
        elif len(err) == 1:
            return err[0]

        return err

    def _process_incoming_request(self, sender, request):
        """
        Parses incoming MHPRequest and passes to the appropriate handler
        :param sender: int
            NodeID of the request sender
        :param request: obj `~qlinklayer.mhp.MHPRequest`
            Contains the request data for the heralding station
        :return: None
        """
        self._store_request_data(sender, request)
        # Handle the message
        self.request_handlers = {
            self.CMD_PRODUCE: self.produce_entanglement,
            self.CMD_INFO: self.pass_information
        }

        self.request_handlers[request.request_data](sender)

    def _store_request_data(self, sender, request):
        """
        Stores the request data in case it is needed during processing
        :param sender: int
            NodeID of the request sender
        :param request: obj `~qlinklayer.mhp.MHPRequest`
            Container of request information
        :return: None
        """
        self.node_requests[sender] = request

    def produce_entanglement(self, sender):
        """
        Handler for a message requesting the production of entangled pairs. Only performs a swap
        if the heralding station has received a qubit from both peers.
        :param classical: obj any
            Classical data sent by the sender
        :param qubit: list `~netsquid.qubits.qubit.Qubit`
            The qubits sent by the sender
        :param sender: int
            NodeID of the sender of the information
        :return: None
        """
        logger.debug("Producing entanglement")
        qubit = self.node_requests[sender].quantum_data
        # Check if we have a qubit from other end of connection
        if self._has_both_qubits():
            # There is a qubit available from Bob already to swap with
            logger.debug("Have qubits from both A and B with request for production")

            # Check the absolute queue id's from both ends of the connection
            if not self._has_same_aid():
                logger.debug("Absolute queue IDs don't match!")
                self._drop_qubit(qubit)
                self._reset_incoming()
                self._send_notification_to_both(self.ERR_QUEUE_MISMATCH)
                return

    def _has_both_qubits(self):
        """
        Checks if we have qubits from both ends of the channel
        :return: bool
            Indicates whether (or not) we have both qubits
        """
        return None not in self.qubits.values()

    def _has_same_aid(self):
        """
        Checks if the last requests sent by each node contains the same absolute queue ID
        :return: bool
            Indicates whether (or not) the absolute queue IDs match
        """
        aid_A = self.node_requests[self.nodeA.nodeID].pass_data[1]
        aid_B = self.node_requests[self.nodeB.nodeID].pass_data[1]
        logger.debug("Comparing absolute queue IDs {} and {}".format(aid_A, aid_B))
        return aid_A == aid_B

    def _reset_incoming(self):
        """
        Resets stored data from nodes to isolate request rounds
        :return: None
        """
        self.classical_data[self.nodeA.nodeID] = None
        self.qubits[self.nodeA.nodeID] = None
        self.node_requests[self.nodeA.nodeID] = None
        self.classical_data[self.nodeB.nodeID] = None
        self.qubits[self.nodeB.nodeID] = None
        self.node_requests[self.nodeB.nodeID] = None

    def pass_information(self, sender):
        """
        Handler for a message requesting the passthrough of information to the other end of the connection
        :param classical: obj any
            Classical data sent by the sender
        :param qubit: list `~netsquid.qubits.qubit.Qubit`
            The qubits sent by the sender
        :param sender: int
            NodeID of the sender of the information
        :return: None
        """
        # Send info message to other end of connection
        receiver = self.nodeB.nodeID if sender == self.nodeA.nodeID else self.nodeA.nodeID
        logger.debug("Passing {}'s information to {}".format(sender, receiver))
        self._send_notification_to_one(self.CMD_INFO, receiver)

    def _do_swap(self):
        # Performs entanglement swapping, if two qubits are available
        num_missing_qubits = list(self.qubits.values()).count(None)
        logger.debug("Missing {} qubits for swapping".format(num_missing_qubits))

        # Error if we only received on qubit during this cycle
        if num_missing_qubits == 1:

            raise EasySquidException("Missing qubit from one node!")

        # If missing both qubits then nodes may have passed information between each other
        elif num_missing_qubits == 2:
            return

        for (id, q) in self.qubits.items():
            if q is None:
                q = create_qubits(1)[0]
                q.is_number_state = True
                self.qubits[id] = q

        outcome = self.midPoint.measure(self.qubits[self.idA], self.qubits[self.idB])

        # Check current classical messages
        present = self._check_current_messages()
        if present:
            self._send_notification_to_both(outcome)

        # Reset incoming data
        self._reset_incoming()


class MHPServiceProtocol(TimedServiceProtocol):
    """
    Generic MHP protocol to be overloaded
    """
    def __init__(self, timeStep, t0=0.0, node=None, connection=None, callback=None):
        super(MHPServiceProtocol, self).__init__(timeStep=timeStep, t0=t0, node=node, connection=connection,
                                                 callback=callback)
        self.reset_protocol()

    @abc.abstractmethod
    def reset_protocol(self):
        """
        To be used for resetting the protocol for each incoming request.  To be overloaded
        """
        pass

    def run_protocol(self):
        """
        Generic protocol, either accepts incoming requests (if any) or continues processing current one
        """
        try:
            logger.debug("{} Node {} running protocol".format(DynAASim().current_time, self.node.nodeID))
            if self._in_progress():
                self._continue_request_handling()

            elif self._has_resources() and self.stateProvider():
                request_data = self.service.get_as(self.node.nodeID)
                self._handle_request(request_data)

        except Exception as err_data:
            logger.exception("Exception occurred while running protocol")
            result = self._construct_error_result(err_data)
            self.callback(result=result)

    @abc.abstractmethod
    def _in_progress(self):
        """
        To be overloaded and used to check if MHP needs to continue processing existing requests
        :return:
        """
        pass

    @abc.abstractmethod
    def _continue_request_handling(self):
        """
        To be overloaded and used to continue the processing of requests that take multiple rounds
        """
        pass

    @abc.abstractmethod
    def _has_resources(self):
        """
        To be overloaded and used to check if this protocol has resources to serve new requests
        """
        pass

    @abc.abstractmethod
    def _handle_request(self, request_data):
        """
        To be overloaded and used for handling various types of request data that comes in
        """
        pass

    @abc.abstractmethod
    def _construct_error_result(self, err_data, **kwargs):
        """
        Constructs a result message that can be interpretted by the callback
        :param err_data: obj any
            Error information to include within the result
        :return:
        """
        pass

    def process_data(self):
        """
        Receives incoming messages on the connection and constructs a reply object to pass into
        the reply processing method.
        :return: None
        """
        try:
            [msg, deltaT] = self.conn.get_as(self.node.nodeID)
            logger.debug("{} Received message {}".format(DynAASim().current_time, msg))
            respM, passM = msg
            reply_message = MHPReply(response_data=respM, pass_data=passM)
            self._process_reply(reply_message)

        except Exception as err_data:
            logger.exception("Exception occurred processing data")
            result = self._construct_error_result(err_data)
            self.callback(result=result)

    @abc.abstractmethod
    def _process_reply(self, reply_message):
        """
        Processes the reply message constructed from the connection data.
        :param reply_message: obj `~qlinklayer.mhp.MHPReply`
            The reply message to process
        """
        pass


class NodeCentricMHPServiceProtocol(MHPServiceProtocol, NodeCentricMHP):
    """
    Service Protocol version of the Node Centric MHP
    """
    STAT_IDLE = 0
    STAT_BUSY = 1

    PROTO_OK = 0
    NO_GENERATION = 0
    ERR_LOCAL = 31
    ERR_TIMEOUT = 32

    def __init__(self, timeStep, t0, node, connection):
        self.status = self.STAT_IDLE
        self.timeout_handler = None
        self._EVT_COMM_TIMEOUT = EventType("COMM TIMEOUT", "Communication timeout")
        super(NodeCentricMHPServiceProtocol, self).__init__(timeStep=timeStep, t0=t0, node=node, connection=connection)

    def reset_protocol(self):
        """
        Resets the protocol to an unitialized state so that we don't produce entanglement
        :return:
        """
        self.electron_physical_ID = 0
        self.storage_physical_ID = 0
        self.status = self.STAT_IDLE
        self.aid = None

        if self.timeout_handler:
            logger.debug("Clearing timeout handler!")
            self._dismiss(self.timeout_handler)
            self.timeout_handler = None

    def _has_resources(self):
        """
        Check if the protocol is not handling any requests
        :return:
        """
        return self.status == self.STAT_IDLE

    def _in_progress(self):
        """
        Check if the protocol is busy
        :return:
        """
        return self.status == self.STAT_BUSY

    def _continue_request_handling(self):
        """
        Continues handling in progress requests for entanglement.  This will run if we are waiting for communication
        from the midpoint
        """
        logger.debug("Continuing process of current request, current pair: ({}, {})".format(self.electron_physical_ID,
                                                                                            self.storage_physical_ID))

    def _handle_request(self, request_data):
        """
        Handles a request when the scheduler has one available
        :param request_data: tuple
            Contains information about the incoming request to service
        :return: None
        """
        logger.debug("MHP_NC Protocol at node {} got request data: {}".format(self.node.nodeID, request_data))

        # Mark the protocol as busy
        self.status = self.STAT_BUSY

        # Extract request information
        flag, aid, comm_q, storage_q, param, free_memory_size = request_data
        self.aid = aid
        self.free_memory_size = free_memory_size

        # If the flag is true then we attempt entanglement generation
        if flag:
            # Set up for generating entanglement
            logger.debug("Flag set to true processing entanglement request")
            self.schedule_comm_timeout()
            self.init_entanglement_request(comm_q, storage_q)
            self.run_entanglement_protocol()

        # Otherwise we are passing information through to our peer
        else:
            logger.debug("Flag set to false, passing information through heralding station")
            self.conn.put_from(self.node.nodeID, [[self.conn.CMD_INFO, self.free_memory_size], []])
            self.reset_protocol()

    def schedule_comm_timeout(self):
        """
        Schedules a communication timeout event and attaches a handler that resets the protocol
        :return:
        """
        self.timeout_handler = EventHandler(self.comm_timeout_handler)
        self.timeout_event = self._schedule_after(2 * self.timeStep, self._EVT_COMM_TIMEOUT)
        self._wait_once(self.timeout_handler, event=self.timeout_event)

    def comm_timeout_handler(self, evt):
        """
        MHP Communication timeout handler.  Triggered after we have waited too long for a response from the midpoint.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered the timeout handler
        """
        logger.debug("Timeout handler triggered!")

        self.release_qubits()
        if self.status == self.STAT_BUSY:
            logger.warning("Timed out waiting for communication response!")
            self._handle_error(None, self.ERR_TIMEOUT)
            self.reset_protocol()

    def release_qubits(self):
        """
        Releases qubits that were used for an entanglement generation attempt.  To be used in error scenarios where the
        stored qubits are not entangled with our peer's.
        """
        if self.node.qmem.in_use(self.electron_physical_ID):
            self.node.qmem.release_qubit(self.electron_physical_ID)

        if self.node.qmem.in_use(self.storage_physical_ID):
            self.node.qmem.release_qubit(self.storage_physical_ID)

    def init_entanglement_request(self, comm_q, storage_q):
        """
        Initializes the protocol to handle the first entanglement generation within a request
        :param comm_q: int
            Address of the communication qubit to be used for the protocol
        :param storage_q: list of int
            Addresses of storage qubits to generate entanglement into
        :return: None
        """
        self.electron_physical_ID = comm_q
        self.storage_physical_ID = storage_q

    def run_entanglement_protocol(self):
        """
        Executes the primary entanglement protocol, calls back to return an error if one occurs
        :return: None
        """
        try:
            logger.debug("{} Beginning entanglement attempt".format(self.node.nodeID))
            NodeCentricMHP.run_protocol(self)

        except Exception as e:
            logger.exception("Error occured attempting entanglement")
            self._handle_error(None, self.ERR_LOCAL)

    def _process_reply(self, reply_message):
        """
        Processes replies from the heralding station and forwards them to the appropriate handler
        :param reply_message: obj `~qlinklayer.mhp.MHPReply`
            The reply message from the heralding station
        """
        # Extract info from the message
        respM, passM = reply_message.response_data, reply_message.pass_data

        # Got some kind of command
        if isinstance(respM, int):
            # Handle INFO passing, otherwise report an error
            handlers = {
                self.conn.CMD_INFO: self._handle_passed_info
            }

            # Handle the message
            handlers.get(respM, self._handle_error)(respM, passM)

        # Receiving result of production attempt
        else:
            # Only process production replies when we are expecting them
            if self.status == self.STAT_BUSY:
                self._handle_production_reply(respM, passM)
            else:
                logger.warning("Received unexpected production reply from midpoint!")
                self.release_qubits()
                self.reset_protocol()

    def _handle_passed_info(self, respM, passM):
        """
        Handles peer info passed via the midpoint station, passes back for update at EGP level
        :param respM: int
            Indicates what type of reply we are handling
        :param passM: any
            Information to pass back up to the EGP
        """
        result = (self.NO_GENERATION, passM, -1, self.aid, self.PROTO_OK)
        self.callback(result=result)

    def _handle_error(self, respM, passM):
        """
        Handles error messages returned from the midpoint station, passes back error info to EGP level
        :param respM: int
            Indicates what type of error reply we are handling
        :param passM: any
            Error information to pass back to EGP
        """
        self.release_qubits()
        result = self._construct_error_result(err_data=passM)
        self.callback(result=result)
        self.reset_protocol()

    def _construct_error_result(self, err_data, **kwargs):
        """
        Creates a result to pass up to the EGP that contains error information for errors that may have
        occurred
        :param err_data: obj any
            Data related to the error that occurred
        :return: tuple
            Result information to be interpretted by higher layers
        """
        result = (self.NO_GENERATION, None, -1, self.aid, err_data)
        return result

    def _handle_production_reply(self, respM, passM):
        """
        Handles entanglement attempt messages from the midpoint station.  Creates the response for EGP and passes info
        back up
        :param respM: tuple of (outcome, MHP Sequence Number, Absolute Queue ID)
            Info to pass up to EGP
        :param passM: any
            Pass information to include in response
        """
        # Extract components of the response
        outcome, mhp_seq, aid = respM
        other_free_memory, other_aid = passM

        # Clear used qubit ID if we failed, otherwise increment our successful generatoin
        if outcome == 0:
            logger.debug("Generation attempt failed, releasing qubit {}".format(self.storage_physical_ID))
            self.node.qmem.release_qubit(self.storage_physical_ID)

        if aid != self.aid:
            logger.warning("Received midpoint reply for aid {} while MHP has local aid {}".format(aid, self.aid))
            self._handle_error(passM=self.ERR_LOCAL)

        result = (outcome, other_free_memory, mhp_seq, other_aid, self.PROTO_OK)
        logger.debug("Finished running protocol, returning results: {}".format(result))

        # Call back with the results
        self.callback(result=result)
        self.reset_protocol()

    def handle_photon_emission(self, photon):
        """
        Handles the photon when created.
        """
        try:
            logger.debug("{} Storing entangled qubit into location {}".format(self.node.nodeID,
                                                                              self.storage_physical_ID))
            if self.electron_physical_ID != self.storage_physical_ID:
                self.node.qmem.move_qubit(self.electron_physical_ID, self.storage_physical_ID)

            pass_info = (self.free_memory_size, self.aid)
            logger.debug("Sending pass info: {}".format(pass_info))
            self.conn.put_from(self.node.nodeID, [[self.conn.CMD_PRODUCE, pass_info], photon])

        except Exception as err_data:
            logger.exception("Error occurred while handling photon emission")
            result = self._construct_error_result(err_data=err_data)
            self._handle_error(None, result)


class SimulatedNodeCentricMHPService(Service):
    """
    Simulated Node Centric MHP Service
    """
    protocol = NodeCentricMHPServiceProtocol

    def __init__(self, name, nodeA, nodeB, conn=None, lengthA=1e-5, lengthB=1e-5):
        """
        Node Centric MHP Service that creates the desired protocol for nodes.  Passes request information down to the
        protocols for retrieval and execution
        :param name: str
            The name of the service
        :param nodeA: obj `~easysquid.qnode.QuantumNode`
            First of the pair of nodes using the service
        :param nodeB: obj `~easysquid.qnode.QuantumNode`
            Second the pair of nodes using the service
        :param conn: obj `~easysquid.heraldedGeneration`
            The connection with heralding midpoint to be used.  If None supplied a default is used.
        :param lengthA: int
            Length for the default connection from node A to the midpoint
        :param lengthB: int
            Length for the default connection from node B to the midpoint
        """
        super(SimulatedNodeCentricMHPService, self).__init__(name=name)

        # Set up a default connection if not specified
        if not conn:
            conn = NodeCentricMHPHeraldedConnection(nodeA=nodeA, nodeB=nodeB, lengthA=lengthA, lengthB=lengthB,
                                                    use_time_window=True, time_window=1)

        # Create the MHP node protocols
        nodeAProto = self.protocol(timeStep=conn.t_cycle, node=nodeA, connection=conn, t0=conn.trigA)
        nodeBProto = self.protocol(timeStep=conn.t_cycle, node=nodeB, connection=conn, t0=conn.trigB)

        # Store them for retrieval by the nodes
        self.node_info = {
            nodeA.nodeID: nodeAProto,
            nodeB.nodeID: nodeBProto
        }

        # Request tracking
        self.seq_num = defaultdict(int)
        self.seq_to_process = {}

    def get_node_proto(self, node):
        """
        Returns the protocol associated with the node
        :param node: obj `~easysquid.qnode.QuantumNode`
            The node we want to retrieve the protocol for
        :return:
        """
        nodeID = node.nodeID
        if nodeID not in self.node_info:
            raise Exception
        else:
            return self.node_info[nodeID]

    def _put_process(self, nodeID, request):
        """
        Tracks the added requests from a given node
        :param nodeID: int
            Node ID of the node that performed the request put
        :param request: obj `~easysquid.services.ServiceRequest`
            The service request object corresponding to the original node request
        :return:
        """
        request.seq_num = self.seq_num[nodeID]
        self.seq_num[nodeID] += 1