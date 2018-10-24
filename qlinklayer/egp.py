import abc
from netsquid.pydynaa import EventType, EventHandler
from netsquid.simutil import sim_time
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easyprotocol import EasyProtocol
from easysquid.quantumProgram import QuantumProgram
from easysquid import qProgramLibrary as qprgms
from qlinklayer.toolbox import LinkLayerException
from qlinklayer.scheduler import RequestScheduler
from qlinklayer.distQueue import DistributedQueue
from qlinklayer.qmm import QuantumMemoryManagement
from qlinklayer.feu import SingleClickFidelityEstimationUnit
from qlinklayer.mhp import SimulatedNodeCentricMHPService
from easysquid.toolbox import logger
import random


class EGPRequest:
    def __init__(self, otherID, num_pairs, min_fidelity, max_time, purpose_id=0, priority=None, store=True,
                 measure_directly=False):
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
        :param store: bool
            Specifies whether entangled qubits should be stored within a storage qubit or left within the communication
            qubit
        :param measure_directly: bool
            Specifies whether to measure the communication qubit directly after the photon is emitted
        """
        self.otherID = otherID
        self.num_pairs = num_pairs
        self.min_fidelity = min_fidelity
        self.max_time = max_time
        self.purpose_id = purpose_id
        self.priority = priority
        self.create_id = None
        self.create_time = None
        self.store = store
        self.measure_directly = measure_directly

    def __copy__(self):
        """
        Allows the copy of a request, specifically for adding to the distributed queue so that
        both nodes are not operating on the same object instance when tracking entanglement
        progress.
        :return: obj `~qlinklayer.egp.EGPRequest`
            A copy of the EGPRequest object
        """
        c = type(self)(otherID=self.otherID, num_pairs=self.num_pairs, min_fidelity=self.min_fidelity,
                       max_time=self.max_time, purpose_id=self.purpose_id, priority=self.priority, store=self.store,
                       measure_directly=self.measure_directly)
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

    def get_create_info(self):
        return self.create_id, self.create_time


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
        self.max_creation_id = 2 ** 32 - 1
        self._EVT_ERROR = EventType("ERROR", "An error occurred in the EGP")

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
        return sim_time()

    def process_data(self):
        """
        Handles classical communication exchanged during EGP (From specifications it appears
        we only need to pass around QMM free memory size, other protocols take care of remaining
        communications)
        """
        # Fetch message from the other side
        [content, t] = self.conn.get_as(self.node.nodeID)
        logger.debug("Processing message from peer: {}".format(content))
        for item in content:
            if len(item) != 2:
                raise ValueError("Unexpected format of classical message.")
            cmd = item[0]
            data = item[1]

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
        logger.debug("Issuing error {} with data {}".format(err, err_data))
        if err_data is None:
            err_data = 0

        if self.err_callback:
            self.err_callback(result=(err, err_data))
            logger.debug("Scheduling error event now.")
            self._schedule_now(self._EVT_ERROR)
        else:
            logger.debug("Scheduling error event now.")
            self._schedule_now(self._EVT_ERROR)
            return err, err_data

    def issue_ok(self, result):
        """
        Issues an OK message back to higher layer protocols with information regarding the
        successful generation of entanglement.
        :param result: obj any
            Result to pass back as an OK to higher layer protocols
        :return: obj any
            OK Info
        """
        logger.debug("Issuing OK with data {}".format(result))
        if self.ok_callback:
            self.ok_callback(result=result)
        else:
            return result


class NodeCentricEGP(EGP):
    # Commands for getting the QMM Free Memory
    CMD_REQ_E = 0
    CMD_ACK_E = 1
    CMD_EXPIRE = 2
    CMD_EXPIRE_ACK = 3

    ERR_UNSUPP = 40
    ERR_NOTIME = 41
    ERR_NORES = 42
    ERR_TIMEOUT = 43
    ERR_REJECTED = 44
    ERR_OTHER = 45
    ERR_EXPIRE = 46
    ERR_CREATE = 47

    def __init__(self, node, conn=None, err_callback=None, ok_callback=None, throw_local_queue_events=False):
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

        # Peer move/correction delay to allow for resynchronization
        self.max_move_delay = 0
        self.this_corr_delay = 0
        self.peer_corr_delay = 0

        # Request tracking
        self.expected_seq = 0

        # Communication handlers
        self.commandHandlers = {
            self.CMD_REQ_E: self.cmd_REQ_E,
            self.CMD_ACK_E: self.cmd_ACK_E,
            self.CMD_EXPIRE: self.cmd_EXPIRE,
            self.CMD_EXPIRE_ACK: self.cmd_EXPIRE_ACK
        }

        # Create local share of distributed queue
        self.dqp = DistributedQueue(node=self.node, throw_local_queue_events=throw_local_queue_events)
        self.dqp.add_callback = self._add_to_queue_callback

        # Create the request scheduler
        self.scheduler = RequestScheduler(distQueue=self.dqp, qmm=self.qmm)
        self.request_timeout_handler = EventHandler(self._request_timeout_handler)
        self._wait(self.request_timeout_handler, entity=self.scheduler, event_type=self.scheduler._EVT_REQ_TIMEOUT)

        # Pydynaa events
        self._EVT_CREATE = EventType("CREATE", "Call to create has completed")
        self._EVT_BIT_COMPLETED = EventType("BIT COMPLETE", "Successfully generated a bit from entangled pair")
        self._EVT_ENT_COMPLETED = EventType("ENT COMPLETE", "Successfully generated an entangled pair of qubits")
        self._EVT_REQ_COMPLETED = EventType("REQ COMPLETE", "Successfully completed a request")

        # Measure directly storage and handler
        self.basis_choice = []
        self.measurement_in_progress = False
        self.measure_directly_reply = None
        self.measurement_results = []
        self.corrected_measurements = []

    def connect_to_peer_protocol(self, other_egp, egp_conn=None, mhp_service=None, mhp_conn=None, dqp_conn=None,
                                 alphaA=0.1, alphaB=0.1):
        """
        Sets up underlying protocols and connections between EGP's
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        if not self.conn:
            # Set up MHP Service
            self._connect_mhp(other_egp, mhp_service, mhp_conn, alphaA=alphaA, alphaB=alphaB)

            # Set up distributed queue
            self._connect_dqp(other_egp, dqp_conn)

            # Set up communication channel for EGP level messages
            self._connect_egp(other_egp, egp_conn)

        else:
            logger.warning("Attempted to configure EGP with new connection while already connected")

    def _connect_mhp(self, other_egp, mhp_service=None, mhp_conn=None, alphaA=0.1, alphaB=0.1):
        """
        Creates the MHP Service and sets up the MHP protocols running at each of the nodes
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
            The peer EGP we want to share an MHP Service with
        :param mhp_service: obj `~qlinklayer.mhp.SimulatedNodeCentricMHPService`
            The MHP Service the EGP will rely on for entanglement generation attempts
        :param mhp_conn: obj `~easysquid.easyfibre.HeraldedFibreConnection`
            The heralding connection that the entanglement attempts occur over
        """
        peer_node = other_egp.node

        # Create the service and set it for both protocols
        if mhp_service is None:
            self.mhp_service = SimulatedNodeCentricMHPService(name="MHPService", nodeA=self.node, nodeB=peer_node,
                                                              conn=mhp_conn, alphaA=alphaA, alphaB=alphaB)
        else:
            self.mhp_service = mhp_service

        other_egp.mhp_service = self.mhp_service

        # Set up local MHPs part of the service
        self._setup_local_mhp()
        other_egp._setup_local_mhp()

        # Give scheduler information about timing
        self.scheduler.configure_mhp_timings(full_cycle=self.mhp.conn.full_cycle)

    def _setup_local_mhp(self):
        """
        Sets up the local mhp protocol that checks for entanglement requests periodically
        """
        # Get our protocol from the service
        self.mhp = self.mhp_service.configure_node_proto(node=self.node, stateProvider=self.trigger_pair_mhp,
                                                         callback=self.handle_reply_mhp)

        # Set a handler for emissions in the measure directly case
        self._emission_handler = EventHandler(self._handle_photon_emission)
        self._wait(self._emission_handler, entity=self.mhp, event_type=self.mhp._EVT_ENTANGLE_ATTEMPT)

        # Fidelity Estimation Unit used to estimate the fidelity of produced entangled pairs
        self.feu = SingleClickFidelityEstimationUnit(node=self.node, mhp_service=self.mhp_service)

    def _connect_dqp(self, other_egp, dqp_conn=None):
        """
        Sets up the DQP between the nodes
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        # Get the MHP timing offsets
        scheduling_offsets = self.mhp_service.get_timing_offsets([self.node, other_egp.node])

        # Call DQP's connect to peer
        self.dqp.connect_to_peer_protocol(other_egp.dqp, dqp_conn, scheduling_offsets)

    def _connect_egp(self, other_egp, egp_conn=None):
        """
        Sets up the communication channel for EGP level messages
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        peer_node = other_egp.node

        # Create the fibre connection
        if not egp_conn:
            egp_conn = ClassicalFibreConnection(nodeA=self.node, nodeB=peer_node, length=1e-5)

        self.setConnection(egp_conn)
        other_egp.setConnection(egp_conn)

        # Store the peer's delay information
        # TODO: assuming that the ID of the communication qubit is 0
        this_move_delays = self.qmm.get_move_delays(0)
        max_this_move_delay = max([time for time in this_move_delays.values() if time != float('inf')])
        peer_move_delays = other_egp.qmm.get_move_delays(0)
        max_peer_move_delay = max([time for time in peer_move_delays.values() if time != float('inf')])
        max_move_delay = max(max_this_move_delay, max_peer_move_delay)

        self.max_move_delay = max_move_delay
        other_egp.max_move_delay = max_move_delay

        this_corr_delay = self.qmm.get_correction_delay(0)
        peer_corr_delay = other_egp.qmm.get_correction_delay(0)

        self.this_corr_delay = this_corr_delay
        self.peer_corr_delay = peer_corr_delay
        other_egp.this_corr_delay = peer_corr_delay
        other_egp.peer_corr_delay = this_corr_delay

    def start(self):
        super(NodeCentricEGP, self).start()
        self.mhp_service.start()

    def request_other_free_memory(self):
        """
        Requests our peer for their QMM's free memory
        """
        logger.debug("Requesting other node's free memory advertisement")
        my_free_mem = self.qmm.get_free_mem_ad()
        self.conn.put_from(self.node.nodeID, [[self.CMD_REQ_E, my_free_mem]])

    def send_expire_notification(self, aid, old_seq, new_seq):
        """
        Sends and expiration notification to our peer if MHP Sequence ordering becomes inconsistent
        :param aid: tuple (int, int)
            Absolute queue ID corresponding to the request we were handling when sequence numbers became inconsistent
        """
        logger.error("Sending EXPIRE notification to peer")
        self.conn.put_from(self.node.nodeID, [[self.CMD_EXPIRE, (aid, old_seq, new_seq)]])

    def cmd_EXPIRE(self, data):
        """
        MHP Sequence numbers became inconsistent with our peer, halt execution of the specified request if we are still
        handling it.  Let our peer know what our expected MHP Sequence number is so that we may both agree on the
        largest
        :param data: tuple (tuple(int, int), int)
            The absolute queue id of the request in which the error occurred and expected mhp sequence number of peer
        :return:
        """
        logger.error("Got EXPIRE command from peer for request {}".format(data))
        aid, old_seq, new_seq = data

        # If our peer is ahead of us we should update
        if new_seq > self.expected_seq:
            logger.debug("Updated expected sequence to {}".format(new_seq))
            self.expected_seq = new_seq

        # If we are still processing the expired request clear it
        self.scheduler.clear_request(aid)

        # Let our peer know we expired
        self.conn.put_from(self.node.nodeID, [[self.CMD_EXPIRE_ACK, self.expected_seq]])

        # Alert higher layer protocols
        self.issue_err(err=self.ERR_EXPIRE, err_data=(old_seq, new_seq))

    def cmd_EXPIRE_ACK(self, data):
        """
        Process acknowledgement of expiration.  Update our local expected mhp sequence number if our peer was ahead
        of us
        :param data: int
            Expected mhp sequence number of our peer
        :return:
        """
        logger.error("Got EXPIRE ACK command from peer")

        # Check if our peer was ahead and we need to update
        other_expected_seq = data
        if other_expected_seq > self.expected_seq:
            logger.debug("Updated expected sequence to {}".format(other_expected_seq))
            self.expected_seq = other_expected_seq

    def cmd_REQ_E(self, data):
        """
        Command handler when requested for free memory of QMM.  Sends a message to our peer with
        information from our QMM
        :param data: obj
            Data associated with command
        """
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
        self.scheduler.update_other_mem_size(data)

    # Primary EGP Protocol Methods
    def create(self, creq):
        """
        Main user interface when requesting entanglement.  Adds the request to our queue.
        :param creq: `~qlinklayer.egp.EGPRequest`
            A request containing (otherID, num_pairs, min_fidelity, max_time, purpose_id, priority)
            information for use with the EGP
        """
        try:
            # Check if we can support this request
            err = self.check_supported_request(creq)
            if err:
                logger.error("Create request failed {}".format(err))
                self.issue_err(err=err)
                return None

            self._assign_creation_information(creq)

            # Track our peer's available memory
            logger.debug("EGP at node {} processing request: {}".format(self.node.nodeID, creq.__dict__))
            if not self.scheduler.other_has_resources():
                self.request_other_free_memory()

            # Add the request to the DQP
            self._add_to_queue(creq)
            logger.debug("Scheduling create event now.")
            self._schedule_now(self._EVT_CREATE)
            return creq.create_id, creq.create_time

        except Exception:
            logger.exception("Failed to issue create")
            self.issue_err(err=self.ERR_CREATE)

    def _assign_creation_information(self, creq):
        """
        Stores creation information onto the provided creation request.  Stores the creation time, and the internally
        tracked local creation id for the request
        :param creq: obj `~qlinklayer.egp.EGPRequest`
            The request that we are updating with creation information
        """
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
        if creq.otherID == self.node.nodeID:
            logger.error("Attempted to submit request for entanglement with self!")
            return self.ERR_CREATE

        if creq.otherID != self.get_otherID():
            logger.error("Attempted to submit request for entanglement with unknown ID!")
            return self.ERR_CREATE

        # Check if we can achieve the minimum fidelity
        if self.feu.estimated_fidelity < creq.min_fidelity:
            logger.error("Requested fidelity is too high to be satisfied")
            return self.ERR_UNSUPP

        # Check if we can satisfy the request within the given time frame
        attempt_latency = self.mhp_service.get_cycle_time(self.node)
        min_time = attempt_latency * creq.num_pairs
        if (min_time > creq.max_time) and (creq.max_time != 0):
            logger.error("Requested max time is too short")
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
            status, qid, qseq, creq = result
            if status == self.dqp.DQ_OK:
                logger.debug("Completed adding item to Distributed Queue, got result: {}".format(result))

            # Otherwise bubble up the DQP error
            else:
                logger.error("Error occurred adding request to distributed queue!")
                self.issue_err(err=status, err_data=creq.create_id)

        except Exception:
            logger.exception("Error occurred processing DQP add callback!")
            self.issue_err(err=self.ERR_OTHER)

    # Handler to be given to MHP as a stateProvider
    def trigger_pair_mhp(self):
        """
        Handler to be given to the MHP service that allows it to ask the scheduler
        if there are any requests and to receive a request to process
        """
        self.scheduler.configure_mhp_timings(full_cycle=self.mhp.conn.full_cycle)
        try:
            # Get scheduler's next gen task
            gen = self.scheduler.next()

            # Store the gen for pickup by mhp
            self.mhp_service.put_ready_data(self.node.nodeID, gen)

            return True

        except Exception:
            logger.exception("Error occurred when triggering MHP!")
            self.issue_err(err=self.ERR_OTHER)
            return False

    # Callback handler to be given to MHP so that EGP updates when request is satisfied
    def handle_reply_mhp(self, result):
        """
        Handler for processing the reply from the MHP service
        :param result: tuple
            Contains the processing result information from attempting entanglement generation
        """
        try:
            logger.debug("Handling MHP Reply: {}".format(result))
            # Check if the reply came in before our measurement completed, defer processing
            if self.measurement_in_progress and self.scheduler.handling_measure_directly():
                self.measure_directly_reply = result
                return

            # Otherwise we are ready to process the reply now
            midpoint_outcome, other_free_memory, mhp_seq, aid, proto_err = self._extract_mhp_reply(result=result)

            # Check if there was memory information included in the reply
            if other_free_memory is not None:
                logger.debug("Updating scheduler with other mem size {}".format(other_free_memory))
                self.scheduler.update_other_mem_size(mem=other_free_memory)

            # If no absolute queue id is included then info was passed or an error occurred
            if aid is None:
                logger.debug("Handling reply that does not contain an absolute queue id")
                self._handle_reply_without_aid(proto_err)

            # Check if this aid may have been expired or timed out while awaiting reply
            elif not self.scheduler.get_request(aid=aid):
                # If we have never seen this aid before we should throw a warning
                if not self.scheduler.previous_request(aid=aid):
                    logger.warning("Got MHP Reply containing aid {} for no current or old request!".format(aid))

                else:
                    logger.debug("Got MHP reply containing aid {} for an old request".format(aid))
                    # Update the MHP Sequence number as necessary
                    if midpoint_outcome in [1, 2]:
                        logger.debug("Updating MHP Seq")
                        self._process_mhp_seq(mhp_seq, aid)

            # Otherwise this response is associated with a generation attempt
            else:
                # Check if an error occurred while processing a request
                if proto_err:
                    logger.error("Protocol error occured in MHP: {}".format(proto_err))
                    comm_q = self.mhp_service.get_comm_qubit_id(self.node)
                    self.qmm.vacate_qubit(comm_q)
                    self.issue_err(err=proto_err)

                # No entanglement generated
                if midpoint_outcome == 0:
                    logger.debug("Failed to produce entanglement with other node")
                    creq = self.scheduler.get_request(aid=aid)

                    # If handling a measure directly request we need to throw away the measurement result
                    if creq.measure_directly and self.scheduler.curr_aid() == aid:
                        m, basis = self.measurement_results.pop(0)
                        logger.debug("Removing measurement outcome {} in basis {} from stored results".format(m, basis))

                else:
                    # Check if we need to time out this request
                    logger.debug("Processing MHP SEQ {}".format(mhp_seq))
                    valid_mhp = self._process_mhp_seq(mhp_seq, aid)

                    if valid_mhp:
                        logger.debug("Handling reply corresponding to absolute queue id {}".format(aid))
                        self._handle_generation_reply(midpoint_outcome, mhp_seq, aid)

            logger.debug("Finished handling MHP Reply")

        except Exception:
            logger.exception("An error occurred handling MHP Reply!")
            self.issue_err(err=self.ERR_OTHER, err_data=self.ERR_OTHER)

    def _extract_mhp_reply(self, result):
        """
        Extracts the MHP processing results from the passed data.
        :param result: tuple containing generation results
        :return:
        """
        try:
            r, other_free_memory, mhp_seq, aid, proto_err = result
            return r, other_free_memory, mhp_seq, aid, proto_err
        except Exception:
            self.issue_err(err=self.ERR_OTHER)
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
            self.issue_err(err=proto_err)

    def _handle_generation_reply(self, r, mhp_seq, aid):
        """
        Handles a successful generation reply from the heralding midpoint.  If we are the entanglement request
        originator then we also correct the qubit locally if necessary.
        If the request says measure directly, the qubit is simply meaured and
        :param r: int
            Outcome of the generation attempt
        :param mhp_seq: int
            MHP Sequence number corresponding to this outcome
        :param aid: tuple of (int, int)
            Absolute Queue ID corresponding to the request this generation attempt belongs to
        :return:
        """
        creq = self.scheduler.get_request(aid=aid)
        if creq.measure_directly:
            # Grab the result and correct
            m, basis = self.measurement_results.pop(0)

            # Flip this outcome in the case we need to apply a correction
            if self.node.nodeID != creq.otherID:
                # Measurements in computational basis are always anti-correlated for the entangled state
                if basis == 0:
                    m ^= 1

                # Measurements in hadamard basis are only anti-correlated when r == 2
                elif basis == 1 and r == 2:
                    m ^= 1

            # Pass up the meaurement info to higher layers
            self.corrected_measurements.append((m, basis))
            self._return_ok(mhp_seq, aid)

        else:
            suspend_time = 0
            # Check if we need to suspend for extra delay for our peer to move to a storage qubit
            if creq.store:
                logger.debug("Moving qubit, suspending generation")
                suspend_time += self.max_move_delay

            # Check if we need to correct the qubit
            if r == 2:
                logger.debug("Applying correction, suspending generation")

                if self.node.nodeID != creq.otherID:
                    # Suspend for an estimated amount of time until our peer is ready to continue generation
                    suspend_time += self.this_corr_delay
                    self.scheduler.suspend_generation(t=suspend_time)

                    self._apply_correction_and_move(mhp_seq, aid)

                else:
                    # Suspend for an estimated amount of time until our peer is ready to continue generation
                    suspend_time += self.peer_corr_delay
                    self.scheduler.suspend_generation(t=suspend_time)
                    self._move_comm_to_storage(mhp_seq, aid)

            else:
                # Suspend for an estimated amount of time until our peer is ready to continue generation
                self.scheduler.suspend_generation(t=suspend_time)
                self._move_comm_to_storage(mhp_seq, aid)

    def _handle_photon_emission(self, evt):
        """
        Catches the event produced when MHP has emitted a photon to the midpoint.  The EGP then checks if the current
        request requires measurement of the qubit immediately and acts as such.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered this handler
        """
        if self.scheduler.curr_request.measure_directly:
            logger.debug("Beginning measurement of qubit for measure directly")
            # Grab the current generation information
            comm_q = self.scheduler.curr_gen[2]

            # Constuct a quantum program
            prgm = QuantumProgram()
            q = prgm.load_mem_qubits(qmem=self.node.qmem)[comm_q]

            # Make a random basis choice
            basis = random.randint(0, 1)

            if basis:
                logger.debug("Measuring comm_q {} in Hadamard basis".format(comm_q))
                q.H()
            else:
                logger.debug("Measuring comm_q {} in Standard basis".format(comm_q))

            self.basis_choice.append(basis)

            # Set a flag to make sure we catch replies that occur during the measurement
            self.measurement_in_progress = True
            q.measure(callback=self._handle_measurement_outcome)
            self.node.qmem.execute_program(prgm)

    def _handle_measurement_outcome(self, outcome):
        """
        Handles the measurement outcome from measureing the communication qubit
        directly after the photon was emitted.
        Calls back to complete MHP reply handling.
        :param outcome: int
            The measurement outcome
        :return: None
        """
        # Saves measurement outcome
        self.measurement_in_progress = False
        logger.debug("Measured {} on qubit".format(outcome))

        # If the request did not time out during the measurement then store the result
        if self.scheduler.curr_gen:
            # Free the communication qubit
            comm_q = self.scheduler.curr_gen[2]
            self.qmm.vacate_qubit(comm_q)

            # Store the measurement result
            basis = self.basis_choice.pop(0)
            self.measurement_results.append((outcome, basis))

            # If we received a reply for this attempt during measurement we can handle it immediately
            if self.measure_directly_reply:
                self.measure_directly_reply = None
                self.handle_reply_mhp(self.measure_directly_reply)

    def _process_mhp_seq(self, mhp_seq, aid):
        """
        Processes the MHP Sequence number from the midpoint.  If it is ahead of our expected sequence number then we
        update our expected sequence number and stop handling the current request and issue errors upwards.  If we
        receive an old sequence number we stop request handling.  Otherwise we increment our expected
        :param mhp_seq: int
            The MHP Sequence number from the midpoint
        :return:
        """
        # Sanity check the MHP sequence number
        logger.debug("Checking received MHP_SEQ")
        if mhp_seq > self.expected_seq:
            logger.error("MHP_SEQ {} greater than expected SEQ {}".format(mhp_seq, self.expected_seq))

            # Collect expiration information to send to our peer
            new_mhp_seq = (mhp_seq + 1) % self.mhp_service.get_max_mhp_seq(self.node)
            self.send_expire_notification(aid=aid, old_seq=self.expected_seq, new_seq=new_mhp_seq)

            # Clear the request
            self.scheduler.clear_request(aid=aid)

            # Update expected sequence number
            self.expected_seq = new_mhp_seq

            return False

        elif mhp_seq < self.expected_seq:
            logger.warning("MHP SEQ {} while expecting SEQ {}".format(mhp_seq, self.expected_seq))
            return False

        else:
            self.expected_seq = (self.expected_seq + 1) % self.mhp_service.get_max_mhp_seq(self.node)
            logger.debug("Incrementing our expected MHP SEQ to {}".format(self.expected_seq))
            return True

    def _apply_correction_and_move(self, mhp_seq, aid):
        """
        Applies a Z gate to specified storage qubit in the case that the entanglement generation
        result was 2 and moves it into the destination location
        :param mhp_seq: int
            The MHP Sequence number of the reply (to pass to the callback)
        :param aid: tuple of (int, int)
            Absolute queue identifier corresponding to this generation (to pass to the callback)
        """

        # Grab the current generation information
        comm_q, storage_q = self.scheduler.curr_gen[2:4]

        logger.debug("Applying correction to comm_q {} and moving to {}".format(comm_q, storage_q))

        # Construct a quantum program to correct and move
        prgm = QuantumProgram()
        qs = prgm.load_mem_qubits(self.node.qmem)
        qs[comm_q].Z()

        # Check if we need to move the qubit into storage
        if comm_q != storage_q:
            qs[storage_q].init()
            qprgms.move_using_CNOTs(prgm, comm_q, storage_q)

        # Set the callback of the program
        prgm.set_callback(callback=self._return_ok, mhp_seq=mhp_seq, aid=aid)
        self.node.qmem.execute_program(prgm)

    def _move_comm_to_storage(self, mhp_seq, aid):
        """
        Moves communication qubit from entanglement generation attempt to a storage location in the memory.
        Calls back to complete MHP reply handling
        :param mhp_seq: int
            The MHP Sequence number corresponding to the generation (to pass to callback)
        :param aid: tuple of (int, int)
            The absolute queue ID corresponding to the generation (to pass to callback)
        """
        # Grab the current generation information
        comm_q, storage_q = self.scheduler.curr_gen[2:4]

        # Check if a move operation is required
        if comm_q != storage_q:
            logger.debug("Moving comm_q {} to storage {}".format(comm_q, storage_q))

            # Construct a quantum program to move the qubit
            prgm = QuantumProgram()
            qs = prgm.load_mem_qubits(qmem=self.node.qmem)
            qs[storage_q].init()
            qprgms.move_using_CNOTs(prgm, comm_q, storage_q)

            # Set the callback
            prgm.set_callback(callback=self._return_ok, mhp_seq=mhp_seq, aid=aid)
            self.node.qmem.execute_program(prgm)

        # Otherwise proceed to return the okay
        else:
            logger.debug("Leaving entangled qubit in comm_q {}".format(comm_q))
            self._return_ok(mhp_seq, aid)

    def get_measurement_outcome(self, creq):
        """
        Returns the oldest measurement outcome along with its basis.
        :return: tuple of int, int
            The measurement outcome and basis in which it was measured
        """
        # Retrieve the measurement outcome and basis
        m, basis = self.corrected_measurements.pop(0)
        return m, basis

    def _create_ok(self, creq, mhp_seq):
        """
        Crafts an OK to issue to higher layers depending on the current generation.  If measure_directly we construct
        and ent_id that excludes the logical_id.  The ok for a measure_directly request contains the measurement
        outcome and basis.
        :param creq: obj ~qlinklayer.EGPRequest
            The request to construct the okay for
        :param mhp_seq: int
            MHP Sequence corresponding to the communication that signaled success for this generation
        :return: tuple
            Contains ok information for the request
        """
        # Get the fidelity estimate from FEU
        logger.debug("Estimating fidelity")
        fidelity_estimate = self.feu.estimated_fidelity

        # Create entanglement identifier
        logical_id = self.scheduler.curr_storage_id()
        creatorID = self.conn.idA if self.conn.idB == creq.otherID else self.conn.idB

        # Construct result information
        now = self.get_current_time()
        t_create = now - self.mhp_service.get_midpoint_comm_delay(self.node)

        # Craft okay based on the request type
        if self.scheduler.handling_measure_directly():
            ent_id = (creatorID, creq.otherID, mhp_seq)
            m, basis = self.get_measurement_outcome(creq)
            result = (creq.create_id, ent_id, m, basis, t_create)

        else:
            ent_id = (creatorID, creq.otherID, mhp_seq, logical_id)
            t_goodness = t_create
            result = (creq.create_id, ent_id, fidelity_estimate, t_goodness, t_create)

        return result

    def _return_ok(self, mhp_seq, aid):
        """
        Constructs the OK message corresponding to the generation attempt and passes it to higher layer
        protocols
        :param mhp_seq: int
            The MHP Sequence number corresponding to this generation
        :param aid: tuple of (int, int)
            The absolute queue ID corresponding to this generation attempt
        """
        logger.debug("Returning entanglement okay")

        # Get the current request
        creq = self.scheduler.get_request(aid=aid)
        if creq is None:
            logger.error("Request not found!")
            self.issue_err(err=self.ERR_OTHER)

        # Check that aid actually corresponds to the current request
        if not self.scheduler.curr_aid() == aid:
            logger.error("Request absolute queue IDs mismatch!")
            self.issue_err(err=self.ERR_OTHER)

        # Pass back the okay and clean up
        logger.debug("Issuing okay to caller")
        ok_data = self._create_ok(creq, mhp_seq)
        self.issue_ok(ok_data)

        # Schedule different events depending on the type of gen we completed
        if self.scheduler.handling_measure_directly():
            self._schedule_now(self._EVT_BIT_COMPLETED)

        else:
            # Schedule event for entanglement completion
            self._schedule_now(self._EVT_ENT_COMPLETED)

        logger.debug("Marking generation completed")
        self.scheduler.mark_gen_completed(aid=aid)

        # Schedule event if request completed
        if self.scheduler.get_request(aid) is None:
            logger.debug("Finished measure directly request, clearing stored results")
            self.measurement_results = []
            self._schedule_now(self._EVT_REQ_COMPLETED)

    def _request_timeout_handler(self, evt):
        """
        Handler for requests that were not serviced within their alotted time.  Passes an error along with the
        request up to higher layers.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered this handler
        """
        scheduler = evt.source
        request = scheduler.timed_out_requests.pop(0)
        self.issue_err(err=self.ERR_TIMEOUT, err_data=request.create_id)
