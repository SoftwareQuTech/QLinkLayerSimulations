import abc
from collections import defaultdict
from easysquid.services import Service, TimedServiceProtocol
from easysquid.simpleLink import NodeCentricMHP
from easysquid.easyfibre import HeraldedFibreConnection
from easysquid.toolbox import EasySquidException, create_logger
from netsquid.qubits.qubitapi import create_qubits
from netsquid.pydynaa import DynAASim
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
    # Sequence number
    mhp_seq = 0

    def __init__(self, *args, **kwargs):
        self.node_requests = {}
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
        # Send notification messages back.
        logger.debug("Sending notification to both")
        if outcome == self.ERR_GENERAL:
            dataA, dataB = self._get_error_data(self.ERR_GENERAL)
        else:
            dataA, dataB = self._get_outcome_data(outcome)

        if dataA is None or dataB is None:
            raise EasySquidException("Missing control data.")

        # Send messages back to the nodes
        logger.debug("Sending messages to A ({}) and B ({})".format(dataA, dataB))
        self.channel_M_to_A.put(dataA)
        self.channel_M_to_B.put(dataB)
        if outcome in [1, 2]:
            self.mhp_seq += 1
        logger.debug("Incremented MHP Sequence Number to {}".format(self.mhp_seq))

    @abc.abstractmethod
    def _get_outcome_data(self, outcome):
        """
        Given the outcome of an entanglement attempt, constructs data to be sent to nodes
        :param outcome: int
            Status code of the entanglement outcome
        """
        pass


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

        resp_MA = [outcome, self.mhp_seq, aid_A]
        resp_MB = [outcome, self.mhp_seq, aid_B]

        respA = MHPReply(response_data=resp_MA, pass_data=pass_BM)
        respB = MHPReply(response_data=resp_MB, pass_data=pass_AM)
        return respA.channel_data(), respB.channel_data()

    def _get_error_data(self, err):
        if err == self.ERR_GENERAL:
            proto_err = self._discover_error()
        else:
            proto_err = err

        data = MHPReply(response_data=self.ERR_GENERAL, pass_data=proto_err).channel_data()

        logger.debug("Sending error messages to A and B ({})".format(data))
        return data, data

    def _discover_error(self):
        err = []
        if None in self.node_requests.values():
            err.append(self.ERR_NO_CLASSICAL_OTHER)
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
                self._send_notification_to_one(self.ERR_QUEUE_MISMATCH, sender)
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
        logger.debug("{} Running protocol".format(DynAASim().current_time))
        if self._in_progress():
            self._continue_request_handling()

        elif self._has_resources() and self.stateProvider():
            request_data = self.service.get_as(self.node.nodeID)
            self._handle_request(request_data)

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

    def process_data(self):
        """

        :return:
        """
        [msg, deltaT] = self.conn.get_as(self.node.nodeID)
        logger.debug("{} Received message {}".format(self.node.nodeID, msg))
        respM, passM = msg
        reply_message = MHPReply(response_data=respM, pass_data=passM)
        self._process_reply(reply_message)

    @abc.abstractmethod
    def _process_reply(self, reply_message):
        pass


class NodeCentricMHPServiceProtocol(MHPServiceProtocol, NodeCentricMHP):
    """
    Service Protocol version of the Node Centric MHP
    """
    STAT_IDLE = 0
    STAT_BUSY = 1
    STAT_WAIT_ENT = 2

    def __init__(self, timeStep, t0, node, connection):
        super(NodeCentricMHPServiceProtocol, self).__init__(timeStep=timeStep, t0=t0, node=node, connection=connection)
        self.status = self.STAT_IDLE

    def reset_protocol(self):
        """
        Resets the protocol to an unitialized state so that we don't produce entanglement
        :return:
        """
        self.electron_physical_ID = 0
        self.storage_IDs = []
        self.curPairs = 0
        self.numPairs = 0
        self.storage_physical_ID = 0
        self.status = self.STAT_IDLE
        self.request_data = None

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
        return self.status == self.STAT_BUSY or self.status == self.STAT_WAIT_ENT

    def _continue_request_handling(self):
        """
        Continues handling in progress requests for entanglement.  This will run if attempts at entanglement
        generation fail or if we need to generate multiple pairs of entangled qubits.
        """
        logger.debug("Continuing process of current request, current pair: {}".format(self.curPairs))

        if self.status == self.STAT_BUSY:
            # Update the storage location of the entangled qubit
            self.storage_physical_ID = self.storage_IDs[self.curPairs]

            self.run_entanglement_protocol()

        elif self.status == self.STAT_WAIT_ENT:
            logger.debug("Waiting for heralding reply for storage id {}".format(self.storage_physical_ID))

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
        self.request_data = request_data
        self.aid = aid
        self.free_memory_size = free_memory_size

        # If the flag is true then we attempt entanglement generation
        if flag:
            # Set up for generating entanglement
            logger.debug("Flag set to true processing entanglement request")
            self.init_entanglement_request(comm_q, storage_q)
            self.run_entanglement_protocol()

        # Otherwise we are passing information through to our peer
        else:
            logger.debug("Flag set to false, passing information through heralding station")
            self.conn.put_from(self.node.nodeID, [[self.conn.CMD_INFO, self.free_memory_size], []])
            self.status = self.STAT_IDLE

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
        self.storage_IDs = storage_q
        self.curPairs = 0
        self.numPairs = len(self.storage_IDs)
        self.storage_physical_ID = self.storage_IDs[self.curPairs]
        self.reserved_qubits = [comm_q] + storage_q
        logger.debug("Storage IDs: {}".format(self.storage_IDs))

    def run_entanglement_protocol(self):
        """
        Executes the primary entanglement protocol, calls back to return an error if one occurs
        :return: None
        """
        try:
            logger.debug("Beginning entanglement attempt")
            NodeCentricMHP.run_protocol(self)

        except Exception as e:
            logger.error("Error occured attempting entanglement: {}".format(e))
            result = (self.outcome, self.other_free_memory, self.mhp_seq, self.other_aid, 1)

            logger.debug("Passing back results: {}".format(result))
            self.callback(result=result)

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
            self._handle_production_reply(respM, passM)

    def _handle_passed_info(self, respM, passM):
        """
        Handles peer info passed via the midpoint station, passes back for update at EGP level
        :param respM: int
            Indicates what type of reply we are handling
        :param passM: any
            Information to pass back up to the EGP
        """
        result = (0, passM, 0, (0, 0), 0)
        self.callback(result=result)

    def _handle_error(self, respM, passM):
        """
        Handles error messages returned from the midpoint station, passes back error info to EGP level
        :param respM: int
            Indicates what type of error reply we are handling
        :param passM: any
            Error information to pass back to EGP
        """
        self.node.qmem.release_qubit(self.storage_physical_ID)
        result = (0, 0, 0, (0, 0), passM)
        self.callback(result=result)

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

        # Store information for extraction if necessary
        self.mhp_seq = mhp_seq
        self.outcome = outcome
        self.other_free_memory = other_free_memory
        self.other_aid = other_aid

        # Clear used qubit ID if we failed, otherwise increment our successful generatoin
        if outcome == 0:
            self.node.qmem.release_qubit(self.storage_physical_ID)
        elif outcome in [1, 2]:
            self.curPairs += 1

        result = (self.outcome, self.other_free_memory, self.mhp_seq, self.other_aid, 0)
        logger.debug("Finished running protocol, returning results: {}".format(result))

        # Call back with the results
        self.callback(result=result)

        # We have successfully generated all pairs and can reset the protocol
        if self.curPairs >= self.numPairs:
            logger.debug("Completed servicing current request, returning to idle state")
            self.reset_protocol()

        else:
            self.status = self.STAT_BUSY

    def handle_photon_emission(self, photon):
        """
        Handles the photon when created.
        """
        logger.debug("{} Storing entangled qubit into location {}".format(self.node.nodeID, self.storage_physical_ID))
        if self.electron_physical_ID != self.storage_physical_ID:
            self.node.qmem.move_qubit(self.electron_physical_ID, self.storage_physical_ID)

        pass_info = (self.free_memory_size, self.aid)
        logger.debug("Sending pass info: {}".format(pass_info))
        self.conn.put_from(self.node.nodeID, [[self.conn.CMD_PRODUCE, pass_info], photon])
        self.status = self.STAT_WAIT_ENT


class SimulatedNodeCentricMHPService(Service):
    """
    Simulated
    """
    protocol = NodeCentricMHPServiceProtocol
    PROTO_OK = 0
    PROTO_INFO = 2

    def __init__(self, name, nodeA, nodeB, lengthA=1e-5, lengthB=1e-5):
        super(SimulatedNodeCentricMHPService, self).__init__(name=name)
        self.conn = NodeCentricMHPHeraldedConnection(nodeA=nodeA, nodeB=nodeB, lengthA=lengthA, lengthB=lengthB,
                                                     time_window=1)

        nodeAProto = self.protocol(timeStep=self.conn.t_cycle, node=nodeA, connection=self.conn, t0=self.conn.trigA)
        nodeBProto = self.protocol(timeStep=self.conn.t_cycle, node=nodeB, connection=self.conn, t0=self.conn.trigB)

        self.node_info = {
            nodeA.nodeID: nodeAProto,
            nodeB.nodeID: nodeBProto
        }

        self.seq_num = defaultdict(int)
        self.seq_to_process = {}

    def get_node_proto(self, node):
        nodeID = node.nodeID
        if nodeID not in self.node_info:
            raise Exception
        else:
            return self.node_info[nodeID]

    def _put_process(self, nodeID, request):
        request.seq_num = self.seq_num[nodeID]
        self.seq_num[nodeID] += 1
