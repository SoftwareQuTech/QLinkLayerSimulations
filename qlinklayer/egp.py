import abc
import netsquid as ns
from netsquid.pydynaa import DynAASim
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easyprotocol import EasyProtocol
from qlinklayer.general import LinkLayerException
from qlinklayer.scheduler import RequestScheduler
from qlinklayer.distQueue import DistributedQueue
from qlinklayer.qmm import QuantumMemoryManagement
from qlinklayer.feu import FidelityEstimationUnit
from qlinklayer.mhp import SimulatedNodeCentricMHPService
from easysquid.toolbox import create_logger

logger = create_logger("logger")


class EGPRequest:
    def __init__(self, otherID, num_pairs, min_fidelity, max_time, purpose_id, priority):
        """
        Stores required parameters of Entanglement Generation Protocol Request
        :param otherID: int
            ID of the other node we are attempting to generate entanglement with
        :param num_pairs: int
            The number of entangled pairs we are trying to generate
        :param min_fidelity: float
            The minimum acceptable fidelity for the pairs we are generating
        :param max_time: float
            The maximum amount of time we are permitted to take when generating the pairs
        :param purpose_id: int
            Identifier for the purpose of this entangled pair
        :param priority: obj
            Priority on the request
        """
        self.otherID = otherID
        self.num_pairs = num_pairs
        self.min_fidelity = min_fidelity
        self.max_time = max_time
        self.purpose_id = purpose_id
        self.priority = priority
        self.create_id = None
        self.create_time = None

    def __copy__(self):
        """
        Allows the copy of a request, specifically for adding to the distributed queue so that
        both nodes are not operating on the same object instance when tracking entanglement
        progress.
        :return: obj `~qlinklayer.egp.EGPRequest`
            A copy of the EGPRequest object
        """
        c = type(self)(self.otherID, self.num_pairs, self.min_fidelity, self.max_time, self.purpose_id, self.priority)
        c.assign_create_id(self.create_id, self.create_time)
        return c

    def assign_create_id(self, create_id, create_time):
        """
        Sets the sequence number of this request
        :param seq_id: int
            The sequence number associated with this request
        """
        self.create_id = create_id
        self.create_time = create_time


class EGP(EasyProtocol):
    def __init__(self, node, conn=None, err_callback=None, ok_callback=None):
        """
        Base abstract class for an EGP, to be overloaded for specific EGP implementations
        :param node: obj `~easysquid.qnode.QuantumNode`
            The node this protocol runs on behalf of
        :param err_callback: func
            Callback function for errors that occur during the EGP
        :param ok_callback: func
            Callback function for successful EGP
        """
        super(EGP, self).__init__(node=node, connection=conn)

        # Hook up user defined callbacks for passing results
        self.err_callback = err_callback
        self.ok_callback = ok_callback
        self.next_creation_id = 0
        self.max_creation_id = 2**32 - 1

    @abc.abstractmethod
    def connect_to_peer_protocol(self, other_egp):
        """
        Performs any set up and connection of protocols.
        :param other_egp: obj `~qlinklayer.egp.EGP`
            The other EGP protocol instance we wish to connect to
        """
        pass

    @abc.abstractmethod
    def create(self, creq):
        """
        Primary interface used by higher layer protocols for requesting entanglement production.  Takes in a
        requests and processes it internally.  To be overloaded by subclasses of specific EGP implementations
        :param creq: obj `~qlinklayer.egp.EGPRequest`
            The request information needed
        """
        pass

    def get_current_time(self):
        """
        Gets the current time
        :return: float
            The current time
        """
        return DynAASim().current_time

    def process_data(self):
        """
        Handles classical communication exchanged during EGP (From specifications it appears
        we only need to pass around QMM free memory size, other protocols take care of remaining
        communications)
        """
        # Fetch message from the other side
        [content, t] = self.conn.get_as(self.node.nodeID)
        logger.debug("Processing message from peer: {}".format(content))
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
        """
        Internal method for processing commands
        :param cmd: int
            Command number corresponding to command
        :param data: obj
            Associated data for the specified command
        :return:
        """
        # First, let's check its of the right form, which is [message type, more data]
        if cmd is None or data is None:
            self.send_error(self.CMD_ERR)
            return

        # Process message demanding on command
        self.commandHandlers[cmd](data)

    def issue_err(self, err, err_data=None):
        """
        Issues an error back to higher layer protocols with the error code that prevented
        successful generation of entanglement
        :param err: obj any
            The error info that occurred while processing the request
        :return: obj any
            The error info
        """
        if self.err_callback:
            self.err_callback(result=(err, err_data))
        else:
            return err

    def issue_ok(self, result):
        """
        Issues an OK message back to higher layer protocols with information regarding the
        successful generation of entanglement.
        :param result: obj any
            Result to pass back as an OK to higher layer protocols
        :return: obj any
            OK Info
        """

        if self.ok_callback:
            self.ok_callback(result=result)
        else:
            return result


class NodeCentricEGP(EGP):
    # Commands for getting the QMM Free Memory
    CMD_REQ_E = 0
    CMD_ACK_E = 1
    CMD_EXPIRE = 2

    ERR_UNSUPP = 40
    ERR_NOTIME = 41
    ERR_NORES = 42
    ERR_TIMEOUT = 43
    ERR_REJECTED = 44
    ERR_OTHER = 45

    def __init__(self, node, conn=None, err_callback=None, ok_callback=None):
        """
        Node Centric Entanglement Generation Protocol.  Uses a Distributed Queue Protocol and Scheduler to coordinate
        the execution of requests of entanglement production between two peers.
        :param node: `~easysquid.qnode.QuantumNode`
            The node we are running the protocol for
        :param conn: `~easysquid.connection.ClassicalConnection`
            Connection used for exchanging QMM free memory info
        :param dqp: `~qlinklayer.distQueue.DistributedQueue`
            This node's share of the DistributedQueue shared with peer
        :param mhp: `~easysquid.services.mhp`
            Middle Heralded Protocol for generating entanglement
        """
        super(NodeCentricEGP, self).__init__(node=node, conn=conn, err_callback=err_callback, ok_callback=ok_callback)

        # Quantum Memory Management, track our own free memory
        self.qmm = QuantumMemoryManagement(node=self.node)

        # Fidelity Estimation Unit used to estimate the fidelity of produced entangled pairs
        self.feu = FidelityEstimationUnit()

        # Request tracking
        self.expected_seq = 0
        self.requests = {}
        self.outstanding_generations = []
        self.curr_gen = 0
        self.curr_request = None

        # Communication handlers
        self.commandHandlers = {
            self.CMD_REQ_E: self.cmd_REQ_E,
            self.CMD_ACK_E: self.cmd_ACK_E,
            self.CMD_EXPIRE: self.cmd_EXPIRE
        }

        # Create local share of distributed queue
        self.dqp = DistributedQueue(node=self.node)
        self.dqp.add_callback = self._add_to_queue_callback

        # Create the request scheduler
        self.scheduler = RequestScheduler(distQueue=self.dqp, qmm=self.qmm)
        self.my_free_memory = self.scheduler.my_free_memory
        self.other_free_memory = None

    def connect_to_peer_protocol(self, other_egp, egp_conn=None, mhp_conn=None, dqp_conn=None):
        """
        Sets up underlying protocols and connections between EGP's
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        if not self.conn:
            # Set up MHP Service
            self._connect_mhp(other_egp, mhp_conn)

            # Set up distributed queue
            self._connect_dqp(other_egp, dqp_conn)

            # Set up communication channel for EGP level messages
            self._connect_egp(other_egp, egp_conn)

        else:
            logger.warning("Attempted to configure EGP with new connection while already connected")

    def _connect_mhp(self, other_egp, mhp_conn=None):
        """
        Creates the MHP Service and sets up the MHP protocols running at each of the nodes
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        peer_node = other_egp.node

        # Create the service and set it for both protocols
        self.mhp_service = SimulatedNodeCentricMHPService(name="MHPService", nodeA=self.node, nodeB=peer_node,
                                                          conn=mhp_conn)
        other_egp.mhp_service = self.mhp_service

        # Set up local MHPs part of the service
        self._setup_local_mhp()
        other_egp._setup_local_mhp()

    def _setup_local_mhp(self):
        """
        Sets up the local mhp protocol that checks for entanglement requests periodically
        """
        # Get our protocol from the service
        self.mhp = self.mhp_service.get_node_proto(self.node)

        # Set the callback for request handling
        self.mhp.callback = self.handle_reply_mhp

        # Add the protocol to the service
        self.mhp_service.add_node(node=self.node, defaultProtocol=self.mhp, stateProvider=self.trigger_pair_mhp)

    def _connect_dqp(self, other_egp, dqp_conn=None):
        """
        Sets up the DQP between the nodes
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        # Call DQP's connect to peer
        self.dqp.connect_to_peer_protocol(other_egp.dqp, dqp_conn)

        # Get the MHP timing offsets
        myTrig = self.mhp_service.get_node_proto(self.node).t0
        otherTrig = self.mhp_service.get_node_proto(other_egp.node).t0

        # Store the offsets for correct request scheduling
        self.dqp.set_triggers(myTrig=myTrig, otherTrig=otherTrig)
        other_egp.dqp.set_triggers(myTrig=otherTrig, otherTrig=myTrig)

    def _connect_egp(self, other_egp, egp_conn=None):
        """
        Sets up the communication channel for EGP level messages
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        peer_node = other_egp.node

        # Create the fibre connection
        if not egp_conn:
            egp_conn = ClassicalFibreConnection(nodeA=self.node, nodeB=peer_node, length=0.01)

        self.setConnection(egp_conn)
        other_egp.setConnection(egp_conn)

    def request_other_free_memory(self):
        """
        Requests our peer for their QMM's free memory
        :return:
        """
        logger.debug("Requesting other node's free memory advertisement")
        my_free_mem = self.qmm.get_free_mem_ad()
        self.conn.put_from(self.node.nodeID, [[self.CMD_REQ_E, my_free_mem]])

    def send_expire_notification(self, aid):
        logger.debug("Sending EXPIRE notification to peer")
        self.conn.put_from(self.node.nodeID, [[self.CMD_EXPIRE, aid]])

    def cmd_REQ_E(self, data):
        """
        Command handler when requested for free memory of QMM.  Sends a message to our peer with
        information from our QMM
        :param data: obj
            Data associated with command
        """
        self.other_free_memory = data
        self.scheduler.update_other_mem_size(data)
        free_memory_size = self.qmm.get_free_mem_ad()
        logger.debug("{} Got request for free memory advertisement, sending: {}".format(self.node.nodeID,
                                                                                        free_memory_size))
        self.conn.put_from(self.node.nodeID, [[self.CMD_ACK_E, free_memory_size]])

    def cmd_ACK_E(self, data):
        """
        Command handler when our peer has responded with their QMM's free memory.  Stores locally
        :param data: int
            Free memory information from our peer
        """
        logger.debug("Got acknowledgement for free memory ad request, storing: {}".format(data))
        self.other_free_memory = data
        self.scheduler.update_other_mem_size(self.other_free_memory)

    def cmd_EXPIRE(self, data):
        """
        Command handler when our peer alerts of an inconsistency in the MHP Sequence numbers.  We should issue an
        ERR_EXPIRE to higher layer protocols to throw away qubits associated with a specific absolute queue id
        :param data: tuple (qid, qseq)
            Absolute queue id corresponding to the request that has an error
        """
        logger.debug("Got EXPIRE command from peer, clearing request and issuing error")
        self.mhp.reset_protocol()
        self.issue_err(err=self.ERR_EXPIRE, data=data)

    # Primary EGP Protocol Methods
    def create(self, creq):
        """
        Main user interface when requesting entanglement.  Adds the request to our queue.
        :param creq: `~qlinklayer.egp.EGPRequest`
            A request containing (otherID, num_pairs, min_fidelity, max_time, purpose_id, priority)
            information for use with the EGP
        """
        try:
            self._assign_creation_information(creq)

            # Check if we can support this request
            err = self.check_supported_request(creq)
            if err:
                logger.error("Create request failed {}".format(err))
                self.issue_err(err)
                return

            # Track our peer's available memory
            logger.debug("EGP at node {} processing request: {}".format(self.node.nodeID, creq.__dict__))
            if self.other_free_memory is None:
                self.request_other_free_memory()

            # Add the request to the DQP
            self._add_to_queue(creq)
            return (creq.create_id, creq.create_time)

        except Exception as err:
            logger.error("Failed to issue create: {}".format(err))
            self.issue_err(self.ERR_OTHER, err_data=err)

    def _assign_creation_information(self, creq):
        create_time = self.get_current_time()
        create_id = self.next_creation_id
        creq.assign_create_id(create_id=create_id, create_time=create_time)
        self.next_creation_id = self.next_creation_id + 1
        logger.debug("Assigned creation id {} creation time {} to request".format(creq.create_id, creq.create_time))

    def check_supported_request(self, creq):
        """
        Performs resource and fulfillment checks to see if the provided request can be satisfied by the EGP.
        :param creq: obj `~qlinklayer.egp.EGPRequest`
            The EGP Request that we want to check
        :return: Error code if request fails a check, otherwise 0
        """
        # Check if we have the resources for this request
        if self.qmm.get_free_mem_ad() < creq.num_pairs:
            logger.debug("Not enough free qubits to satisfy request")
            return self.ERR_NORES

        # Check if we can achieve the minimum fidelity
        if self.feu.estimate_fidelity(alpha=self.mhp.alpha) < creq.min_fidelity:
            logger.debug("Requested fidelity is too high to be satisfied")
            return self.ERR_UNSUPP

        # Check if we can satisfy the request within the given time frame
        expected_time = self.mhp.timeStep * creq.num_pairs
        if expected_time > creq.max_time:
            logger.debug("Requested max time is too short")
            return self.ERR_UNSUPP

        return 0

    # Queues a request
    def _add_to_queue(self, creq):
        """
        Stores the request in the distributed queue
        :param creq: `~qlinklayer.egp.EGPRequest`
            The request we want to store in the distributed queue
        :return: tuple(int, int)
            Tuple containing the qid where requests was stored and the request identifier within that
            queue
        """
        # Get the qid we should store the request in from the scheduler
        qid = self.scheduler.get_queue(creq)
        logger.debug("Got QID {} from scheduler".format(qid))

        # Store the request into the distributed queue
        self.dqp.add(creq, qid)

    def _add_to_queue_callback(self, result):
        """
        Callback to be given to the DQP since adding an item takes a round of communication.  Issues an
        error to higher layer protocols if there was a failure
        :param result: tuple
            Result of performing add of request, contains add error code, absolute qid and the request
        :return:
        """
        try:
            # Store the request locally if DQP ADD was successful
            logger.debug("Completed adding item to Distributed Queue, got result: {}".format(result))
            status, qid, qseq, creq = result
            if status == self.dqp.DQ_OK:
                self.requests[(qid, qseq)] = creq

            # Handle errors that may have occurred while adding the request
            elif status == self.dqp.DQ_TIMEOUT:
                self.issue_err(self.ERR_NOTIME)

            elif status == self.dqp.DQ_REJECT:
                self.issue_err(self.ERR_REJECTED)

            else:
                self.issue_err(self.ERR_OTHER)

        except Exception as err_data:
            logger.error("Error occurred processing DQP add callback! {}".format(err_data))
            self.issue_err(err=self.ERR_OTHER, err_data=err_data)

    # Handler to be given to MHP as a stateProvider
    def trigger_pair_mhp(self):
        """
        Handler to be given to the MHP service that allows it to ask the scheduler
        if there are any requests and to receive a request to process
        """
        try:
            # Pass request information back up to higher layer protocol
            stale_requests = self.scheduler.timeout_stale_requests()
            for request in stale_requests:
                self.issue_err(err=self.ERR_TIMEOUT, err_data=request)

            # Process any currently outstanding generations
            if self.outstanding_generations:
                generation_request = self.outstanding_generations[self.curr_gen]
                logger.debug("Have outstanding generation, passing {} to MHP".format(generation_request))
                self.mhp_service.put_ready_data(self.node.nodeID, generation_request)

            else:
                incoming_generations = self.scheduler.next()
                if incoming_generations:
                    self.outstanding_generations = incoming_generations
                    self.curr_gen = 0
                    generation_request = self.outstanding_generations[self.curr_gen]
                    logger.debug("Have outstanding generation, passing {} to MHP".format(generation_request))
                    self.mhp_service.put_ready_data(self.node.nodeID, generation_request)

                # Otherwise have the MHP send free memory advertisement
                else:
                    logger.debug("Scheduler has no requests ready")
                    free_memory_size = self.qmm.get_free_mem_ad()
                    request = (False, None, None, None, None, free_memory_size)
                    logger.debug("Constructed INFO request {} for MHP_NC".format(request))
                    self.mhp_service.put_ready_data(self.node.nodeID, request)

            return True

        except Exception as err_data:
            logger.error("Error occurred when triggering MHP! {}".format(err_data))
            self.issue_err(err=self.ERR_OTHER, err_data=err_data)

    # Callback handler to be given to MHP so that EGP updates when request is satisfied
    def handle_reply_mhp(self, result):
        """
        Handler for processing the reply from the MHP service
        :param result: tuple
            Contains the processing result information from attempting entanglement generation
        """
        try:
            # Get the MHP results
            logger.debug("Handling MHP Reply: {}".format(result))
            r, other_free_memory, mhp_seq, aid, proto_err = self._extract_mhp_reply(result=result)

            # Check if there was memory information included in the reply
            if other_free_memory is not None:
                logger.debug("Updating scheduler with other mem size {}".format(other_free_memory))
                self.scheduler.update_other_mem_size(mem=other_free_memory)

            # If no absolute queue id is included then info was passed or an error occurred
            if aid is None:
                logger.debug("Handling reply that does not contain an absolute queue id")
                self._handle_reply_without_aid(proto_err)

            # Otherwise this response is associated with a generation attempt
            else:
                # Check if we need to time out this request
                now = self.get_current_time()
                creq = self.requests[aid]
                deadline = creq.create_time + creq.max_time

                # Check if we need to timeout this request
                if now > deadline:
                    logger.error("Timeout occurred processing request!")
                    self._clear_request(aid)
                    self.issue_err(err=self.ERR_TIMEOUT, err_data=creq)
                    return

                # Check if an error occurred while processing a request
                if proto_err:
                    logger.error("Protocol error occured in MHP: {}".format(proto_err))
                    self.issue_err(proto_err)

                # No entanglement generation
                if r == 0:
                    logger.warning("Failed to produce entanglement with other node")
                    return

                # Check if we need to time out this request
                logger.debug("Processing MHP SEQ {}".format(mhp_seq))
                valid_mhp = self._process_mhp_seq(mhp_seq)

                if valid_mhp:
                    logger.debug("Handling reply corresponding to absolute queue id {}".format(aid))
                    self._handle_generation_reply(r, mhp_seq, aid)

            logger.debug("Finished handling MHP Reply")

        except Exception as err_data:
            logger.error("An error occurred handling MHP Reply! {}".format(err_data))
            self.issue_err(err=self.ERR_OTHER, err_data=err_data)

    def _extract_mhp_reply(self, result):
        """
        Extracts the MHP processing results from the passed data.
        :param result: tuple containing generation results
        :return:
        """
        try:
            r, other_free_memory, mhp_seq, aid, proto_err = result
            return r, other_free_memory, mhp_seq, aid, proto_err
        except Exception as err:
            self.issue_err(err=self.ERR_OTHER, err_data=err)
            raise LinkLayerException("Malformed MHP reply received: {}".format(result))

    def _handle_reply_without_aid(self, proto_err):
        """
        If a request doesn't have an absolute queue ID it may be that an error occurred in the MHP or that we received
        a pass through of information from our peer node in which case our only action was to update our memory size.
        We should check if any errors occurred in the MHP
        :param proto_err: int
            Error number corresponding to the error in the MHP (if nonzero)
        :return:
        """
        # Check if an error occurred while processing a request
        if proto_err:
            logger.error("Protocol error occured in MHP: {}".format(proto_err))
            self.issue_err(proto_err)

        return

    def _handle_generation_reply(self, r, mhp_seq, aid):
        """
        Handles a successful generation reply from the heralding midpoint.  If we are the entanglement request
        originator then we also correct the qubit locally if necessary.
        :param r: int
            Outcome of the generation attempt
        :param mhp_seq: int
            MHP Sequence number corresponding to this outcome
        :param aid: tuple of (int, int)
            Absolute Queue ID corresponding to the request this generation attempt belongs to
        :return:
        """
        now = self.get_current_time()
        creq = self.requests[aid]

        # Check if we need to correct the qubit
        if r == 2 and self.node.nodeID == creq.otherID:
            logger.debug("Applying correction to qubit")
            self._apply_correction_to_qubit(self.mhp.storage_physical_ID)

        # Get the fidelity estimate from FEU
        logger.debug("Estimating fidelity")
        fidelity_estimate = self.feu.estimate_fidelity(alpha=self.mhp.alpha)

        # Create entanglement identifier
        logical_id = self.qmm.physical_to_logical(self.mhp.storage_physical_ID)
        creatorID = self.conn.idA if self.conn.idB == creq.otherID else self.conn.idB
        ent_id = (creatorID, creq.otherID, mhp_seq, logical_id)

        # Issue OK
        logger.debug("Issuing okay to caller")
        result = (creq.create_id, ent_id, fidelity_estimate, now, now)
        self.issue_ok(result)

        # Update number of remaining pairs on request, remove if completed
        if creq.num_pairs == 1:
            logger.debug("Generated final pair, removing request")
            self.requests.pop(aid)
            self.outstanding_generations = []

        elif creq.num_pairs >= 2:
            logger.debug("Decrementing number of remaining pairs")
            creq.num_pairs -= 1
            self.curr_gen += 1

        else:
            raise LinkLayerException("Request has invalid number of remaining pairs!")

    def _process_mhp_seq(self, mhp_seq):
        """
        Processes the MHP Sequence number from the midpoint.  If it is ahead of our expected sequence number then we
        update our expected sequence number and stop handling the current request and issue errors upwards.  If we
        receive an old sequence number we stop request handling.  Otherwise we increment our expected
        :param mhp_seq: int
            The MHP Sequence number from the midpoint
        :return:
        """
        logger.debug("Checking received MHP_SEQ")
        # Sanity check the MHP sequence number
        if mhp_seq > self.expected_seq:
            logger.error("MHP_SEQ {} greater than expected SEQ {}".format(mhp_seq, self.expected_seq))

            for generation in self.outstanding_generations:
                self.issue_err(err=self.ERR_OTHER, err_data=generation)

            self.outstanding_generations = []
            self.expected_seq = (mhp_seq + 1) % self.mhp.conn.max_seq
            return False

        elif mhp_seq < self.expected_seq:
            logger.warning("MHP SEQ {} while expecting SEQ {}".format(mhp_seq, self.expected_seq))
            return False

        else:
            self.expected_seq = (self.expected_seq + 1) % self.mhp.conn.max_seq
            logger.debug("Incrementing our expected MHP SEQ to {}".format(self.expected_seq))
            return True

    def _clear_request(self, aid):
        logger.error("Took too long to generate pair, removing request and resetting")

        # Free all the qubits that were associated with this request
        used_qs = set()
        for gen in self.outstanding_generations:
            comm_q, storage_q = gen[2:4]
            used_qs |= {comm_q, storage_q}

        # Clear out memory that may have been used
        logger.debug("Freeing qubit ids {}".format(used_qs))
        self.qmm.free_qubits(used_qs)

        # Clear outstanding generations
        logger.debug("Clearing outsanding generations {} for request".format(self.outstanding_generations))
        self.outstanding_generations = []

        self.requests.pop(aid)

        # Reset the protocol for the next request
        self.mhp.reset_protocol()

    def _apply_correction_to_qubit(self, storage_qubit):
        """
        Applies a Z gate to specified storage qubit in the case that the entanglement generation
        result was 2
        :return:
        """
        logger.debug("Applying correction to storage_qubit {}".format(storage_qubit))
        self.node.qmem.apply_unitary(ns.Z, [storage_qubit])
