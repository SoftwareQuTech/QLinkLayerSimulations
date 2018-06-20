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
        c.create_time = self.create_time
        c.create_id = self.create_id
        return c

    def assign_seq_id(self, create_id):
        """
        Sets the sequence number of this request
        :param seq_id: int
            The sequence number associated with this request
        """
        self.create_id = create_id


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

    ERR_UNSUPP = 10
    ERR_NOTIME = 11
    ERR_NORES = 12
    ERR_TIMEOUT = 13
    ERR_REJECTED = 14
    ERR_OTHER = 15

    def __init__(self, node, err_callback=None, ok_callback=None, length_to_midpoint=0.0):
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
        super(NodeCentricEGP, self).__init__(node=node, err_callback=err_callback, ok_callback=ok_callback)

        # Quantum Memory Management, track our own free memory
        self.qmm = QuantumMemoryManagement(node=self.node)

        # Fidelity Estimation Unit used to estimate the fidelity of produced entangled pairs
        self.feu = FidelityEstimationUnit()

        # Request tracking
        self.expected_seq = 0
        self.requests = {}
        self.outstanding_generations = []

        # Communication handlers
        self.commandHandlers = {
            self.CMD_REQ_E: self.cmd_REQ_E,
            self.CMD_ACK_E: self.cmd_ACK_E
        }

        # Create local share of distributed queue
        self.dqp = DistributedQueue(node=self.node)
        self.dqp.add_callback = self._add_to_queue_callback

        # Create the request scheduler
        self.scheduler = RequestScheduler(distQueue=self.dqp, qmm=self.qmm)
        self.my_free_memory = self.scheduler.my_free_memory
        self.other_free_memory = None

        self.length_to_midpoint = length_to_midpoint

    def connect_to_peer_protocol(self, other_egp):
        """
        Sets up underlying protocols and connections between EGP's
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        # Set up MHP Service
        self._connect_mhp(other_egp)

        # Set up distributed queue
        self._connect_dqp(other_egp)

        # Set up communication channel for EGP level messages
        self._connect_egp(other_egp)

    def _connect_mhp(self, other_egp):
        """
        Creates the MHP Service and sets up the MHP protocols running at each of the nodes
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        peer_node = other_egp.node

        # Create the service and set it for both protocols
        self.mhp_service = SimulatedNodeCentricMHPService(name="MHPService", nodeA=self.node, nodeB=peer_node,
                                                          lengthA=self.length_to_midpoint,
                                                          lengthB=other_egp.length_to_midpoint)
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

    def _connect_dqp(self, other_egp):
        """
        Sets up the DQP between the nodes
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        # Call DQP's connect to peer
        self.dqp.connect_to_peer_protocol(other_egp.dqp)

        # Get the MHP timing offsets
        myTrig = self.mhp_service.get_node_proto(self.node).t0
        otherTrig = self.mhp_service.get_node_proto(other_egp.node).t0

        # Store the offsets for correct request scheduling
        self.dqp.set_triggers(myTrig=myTrig, otherTrig=otherTrig)
        other_egp.dqp.set_triggers(myTrig=otherTrig, otherTrig=myTrig)

    def _connect_egp(self, other_egp):
        """
        Sets up the communication channel for EGP level messages
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        peer_node = other_egp.node

        # Create the fibre connection
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

    # Primary EGP Protocol Methods
    def create(self, creq):
        """
        Main user interface when requesting entanglement.  Adds the request to our queue.
        :param creq: `~qlinklayer.egp.EGPRequest`
            A request containing (otherID, num_pairs, min_fidelity, max_time, purpose_id, priority)
            information for use with the EGP
        """
        try:
            creq.create_time = self.get_current_time()

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

        except Exception as err:
            logger.error("Failed to issue create: {}".format(err))
            self.issue_err(self.ERR_OTHER, err_data=err)

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

    # Handler to be given to MHP as a stateProvider
    def trigger_pair_mhp(self):
        """
        Handler to be given to the MHP service that allows it to ask the scheduler
        if there are any requests and to receive a request to process
        """
        # Pass request information back up to higher layer protocol
        stale_requests = self.scheduler.timeout_stale_requests()
        for request in stale_requests:
            self.issue_err(err=self.ERR_TIMEOUT, err_data=request)

        # Check if scheduler has any ready requests
        if self.scheduler.request_ready():
            # Grab the request
            request = self.scheduler.next()
            if not request:
                return False

            logger.debug("Scheduler has request {} ready".format(request))

            # Get necessary information for handling the request at MHP
            (flag, (qid, qseq), req, param, comm_q, storage_q) = request
            free_memory_size = self.qmm.get_free_mem_ad()

            # Add this request to outstanding requests
            self.outstanding_generations.append((qid, qseq))

            # Ready the data for retrieval by the protocol
            request = (flag, (qid, qseq), comm_q, storage_q, param, free_memory_size)
            logger.debug("Constructed request info {} for MHP_NC".format(request))
            self.mhp_service.put_ready_data(self.node.nodeID, request)

            # Tell the protocol that data is ready
            return True

        else:
            logger.debug("Scheduler has no requests ready")
            request = (False, (0, 0), None, None, 0, [])
            (flag, (qid, qseq), req, param, comm_q, storage_q) = request
            free_memory_size = self.qmm.get_free_mem_ad()
            request = (flag, (qid, qseq), comm_q, storage_q, param, free_memory_size)
            logger.debug("Constructed request info {} for MHP_NC".format(request))
            self.mhp_service.put_ready_data(self.node.nodeID, request)

            return True

    # Callback handler to be given to MHP so that EGP updates when request is satisfied
    def handle_reply_mhp(self, result):
        """
        Handler for processing the reply from the MHP service
        :param result: tuple
            Contains the processing result information from attempting entanglement generation
        """
        # Get the MHP results
        logger.debug("Handling MHP Reply: {}".format(result))
        r, other_free_memory, mhp_seq, (qid, qseq), proto_err = self._extract_mhp_reply(result=result)

        # Check if there were any errors that occurred
        if proto_err != self.mhp_service.PROTO_OK:
            logger.error("Protocol error occured in MHP: {}".format(proto_err))
            self.issue_err(proto_err)

        # Update scheduler with other nodes new memory size
        logger.debug("Updating scheduler with other mem size")
        self.scheduler.update_other_mem_size(other_free_memory)

        # No entanglement generation
        if r == 0:
            logger.warning("Failed to produce entanglement with other node")
            return

        # Check the MHP Sequence Number
        valid_mhp = self._handle_mhp_sequence(mhp_seq)

        # Check if the protocol executed simple info passing
        if proto_err == self.mhp_service.PROTO_INFO:
            self.scheduler.update_other_mem_size(other_free_memory)
            return

        # Check if we took too long to create the pair
        creq = self.requests[(qid, qseq)]
        now = self.get_current_time()
        if creq.create_time + creq.max_time < now:
            logger.error("Took too long to generate pair, removing request and resetting")
            # Drop the request
            self.requests.pop((qid, qseq))
            self.outstanding_generations.remove((qid, qseq))

            # Clear out memory that may have been used
            self.qmm.free_qubits(self.mhp.reserved_qubits)

            # Reset the protocol for the next request
            self.mhp.reset_protocol()
            self.issue_err(self.ERR_TIMEOUT)

        elif valid_mhp:
            self.expected_seq = (self.expected_seq + 1) % self.dqp.maxSeq

            # Check if we need to correct the qubit
            if r == 2 and self.node.nodeID == creq.otherID:
                logger.debug("Applying correction to qubit")
                self._apply_correction_to_qubit(self.mhp.storage_physical_ID)

            # Get the fidelity estimate from FEU
            logger.debug("Estimating fidelity")
            fidelity_estimate = self.feu.estimate_fidelity(alpha=self.mhp.alpha)

            # Create entanglement identifier
            logical_id = self.qmm.physical_to_logical(self.mhp.storage_physical_ID)
            ent_id = (self.conn.idA, self.conn.idB, mhp_seq, logical_id)

            # Issue OK
            logger.debug("Issuing okay to caller")
            result = (ent_id, fidelity_estimate, now, now)
            self.issue_ok(result)

            # Update number of remaining pairs on request, remove if completed
            if creq.num_pairs == 1:
                logger.debug("Generated final pair, removing request")
                self.requests.pop((qid, qseq))

                # Remove (j, idj) from outstanding generation list
                self.outstanding_generations.remove((qid, qseq))

            elif creq.num_pairs >= 2:
                logger.debug("Decrementing number of remaining pairs")
                creq.num_pairs -= 1

            else:
                raise LinkLayerException("Request has invalid number of remaining pairs!")

        logger.debug("Finished handling MHP Reply")

    def _extract_mhp_reply(self, result):
        try:
            r, other_free_memory, mhp_seq, (qid, qseq), proto_err = result
            return r, other_free_memory, mhp_seq, (qid, qseq), proto_err
        except Exception as err:
            self.issue_err(err=self.ERR_OTHER, err_data=err)
            raise LinkLayerException("Malformed MHP reply received: {}".format(result))

    def _handle_mhp_sequence(self, mhp_seq):
        # Sanity check the MHP sequence number
        if mhp_seq > self.expected_seq:
            self.outstanding_generations = []
            logger.error("Returned MHP_SEQ {} greater than expected SEQ {}".format(mhp_seq, self.expected_seq))
            self.issue_err(self.ERR_OTHER)
            return False

        elif mhp_seq < self.expected_seq:
            logger.warning("Received old MHP SEQ {} while expecting SEQ {}".format(mhp_seq, self.expected_seq))
            return False

        else:
            return True

    def _apply_correction_to_qubit(self, storage_qubit):
        """
        Applies a Z gate to specified storage qubit in the case that the entanglement generation
        result was 2
        :return:
        """
        logger.debug("Applying correction to storage_qubit {}".format(storage_qubit))
        self.node.qmem.apply_unitary(ns.Z, [storage_qubit])
