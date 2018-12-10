import abc
from collections import namedtuple
import random

from netsquid.pydynaa import EventType, EventHandler
from netsquid.simutil import sim_time
from netsquid.components.instructions import INSTR_Z, INSTR_INIT, INSTR_H, INSTR_MEASURE
from netsquid.components.qprogram import QuantumProgram
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easyprotocol import EasyProtocol
from easysquid import qProgramLibrary as qprgms
from qlinklayer.toolbox import LinkLayerException
from qlinklayer.scheduler import StrictPriorityRequestScheduler
from qlinklayer.distQueue import EGPDistributedQueue
from qlinklayer.qmm import QuantumMemoryManagement
from qlinklayer.feu import SingleClickFidelityEstimationUnit
from qlinklayer.mhp import SimulatedNodeCentricMHPService
from easysquid.toolbox import logger
from SimulaQron.cqc.backend.cqcHeader import CQCHeader, CQCEPRRequestHeader, CQC_HDR_LENGTH, CQC_CMD_HDR_LENGTH, \
    CQC_VERSION, CQC_TP_EPR_OK, CQCCmdHeader, CQC_TP_COMMAND, CQC_CMD_EPR, CQC_EPR_REQ_LENGTH, CQCXtraQubitHeader,\
    CQC_XTRA_QUBIT_HDR_LENGTH
from SimulaQron.cqc.backend.entInfoHeader import EntInfoCreateKeepHeader, EntInfoMeasDirectHeader,\
    ENT_INFO_MEAS_DIRECT_LENGTH, ENT_INFO_CREATE_KEEP_LENGTH


EGPRequest = namedtuple("EGP_request",
                        ["purpose_id", "other_id", "num_pairs", "min_fidelity", "max_time", "priority", "store",
                         "measure_directly", "atomic"])
EGPRequest.__new__.__defaults__ = (0, 0, 0, 0, 0, 0, True, False, False)


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
    def create(self, cqc_request_raw):
        """
        Primary interface used by higher layer protocols for requesting entanglement production.  Takes in a
        requests and processes it internally.  To be overloaded by subclasses of specific EGP implementations
        :param cqc_request_raw: bytes
            The raw CQC request for generating entanglement
        """
        pass

    @staticmethod
    def _get_egp_request(cqc_request_raw):
        """
        Creates a EGP request from a full CQC request for generating entanglement.

        :param cqc_request_raw: bytes
            The cqc request consisting of CQCHeader, CQCCmdHeader, CQCEPRRequestHeader
        :return: :obj:`~qlinklayer.egp.EGPRequest`
        """
        try:
            cqc_header = CQCHeader(cqc_request_raw[:CQC_HDR_LENGTH])
            if not cqc_header.tp == CQC_TP_COMMAND:
                raise LinkLayerException("raw CQC request is not of type command")

            cqc_request_raw = cqc_request_raw[CQC_HDR_LENGTH:]
            cqc_cmd_header = CQCCmdHeader(cqc_request_raw[:CQC_CMD_HDR_LENGTH])
            if not cqc_cmd_header.instr == CQC_CMD_EPR:
                raise LinkLayerException("raw CQC request is not a command for EPR")

            cqc_request_raw = cqc_request_raw[CQC_CMD_HDR_LENGTH:]
            cqc_epr_req_header = CQCEPRRequestHeader(cqc_request_raw[:CQC_EPR_REQ_LENGTH])
        except IndexError:
            raise LinkLayerException("Could not unpack raw CQC request")
        egp_request = EGPRequest(purpose_id=cqc_header.app_id, other_id=cqc_epr_req_header.remote_ip,
                                 num_pairs=cqc_epr_req_header.num_pairs,
                                 min_fidelity=cqc_epr_req_header.min_fidelity,
                                 max_time=cqc_epr_req_header.max_time,
                                 priority=cqc_epr_req_header.priority, store=cqc_epr_req_header.store,
                                 measure_directly=cqc_epr_req_header.measure_directly,
                                 atomic=cqc_epr_req_header.atomic)

        return egp_request

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

    # Error codes
    ERR_UNSUPP = 40
    ERR_NOTIME = 41
    ERR_NORES = 42
    ERR_TIMEOUT = 43
    ERR_REJECTED = 44
    ERR_OTHER = 45
    ERR_EXPIRE = 46
    ERR_CREATE = 47

    def __init__(self, node, conn=None, err_callback=None, ok_callback=None, throw_local_queue_events=False,
                 accept_all_requests=False, num_priorities=1):
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
        self.max_measurement_delay = 0

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
        self.dqp = EGPDistributedQueue(node=self.node, throw_local_queue_events=throw_local_queue_events,
                                       accept_all=accept_all_requests, numQueues=num_priorities)
        self.dqp.add_callback = self._add_to_queue_callback

        # Create the request scheduler
        self.scheduler = StrictPriorityRequestScheduler(distQueue=self.dqp, qmm=self.qmm)
        self.scheduler.set_timeout_callback(self.request_timeout_handler)

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
                                 alphaA=None, alphaB=None):
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

    def _connect_mhp(self, other_egp, mhp_service=None, mhp_conn=None, alphaA=None, alphaB=None):
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

    def _setup_local_mhp(self):
        """
        Sets up the local mhp protocol that checks for entanglement requests periodically
        """
        # Get our protocol from the service
        self.mhp = self.mhp_service.configure_node_proto(node=self.node, stateProvider=self.trigger_pair_mhp,
                                                         callback=self.handle_reply_mhp)

        # Give scheduler information about timing
        remote_node = self.mhp_service.get_node(nodeID=self.mhp.get_otherID())
        scheduling_offsets = self.mhp_service.get_timing_offsets([self.node, remote_node])
        local_trigger = scheduling_offsets[self.node.nodeID]
        remote_trigger = scheduling_offsets[self.mhp.get_otherID()]
        self.scheduler.configure_mhp_timings(cycle_period=self.mhp_service.get_cycle_time(self.node),
                                             full_cycle=self.mhp_service.get_full_cycle_time(self.node),
                                             local_trigger=local_trigger, remote_trigger=remote_trigger)

        # Set a handler for emissions in the measure directly case
        self._emission_handler = EventHandler(self._handle_photon_emission)
        self._wait(self._emission_handler, entity=self.mhp, event_type=self.mhp._EVT_ENTANGLE_ATTEMPT)

        # Fidelity Estimation Unit used to estimate the fidelity of produced entangled pairs
        self.feu = SingleClickFidelityEstimationUnit(node=self.node, mhp_service=self.mhp_service)
        self.scheduler.add_feu(self.feu)

    def _connect_dqp(self, other_egp, dqp_conn=None):
        """
        Sets up the DQP between the nodes
        :param other_egp: obj `~qlinklayer.egp.NodeCentricEGP`
        """
        # Call DQP's connect to peer
        self.dqp.connect_to_peer_protocol(other_egp.dqp, dqp_conn)

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

        local_measurement_delay = self.qmm.get_measurement_delay(0)
        remote_measurement_delay = other_egp.qmm.get_measurement_delay(0)
        self.max_measurement_delay = max(local_measurement_delay, remote_measurement_delay)
        other_egp.max_measurement_delay = self.max_measurement_delay
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

    def add_queue_rule(self, node, purpose_id):
        """
        Adds a rule to the queue
        :param node: obj `~easysquid.qnode.QuantumNode`
            Node to add acception rule for
        :param purpose_id: int
            ID (port) that we are routing this entanglement for
        """
        self.dqp.add_accept_rule(node.nodeID, purpose_id)

    def remove_queue_rule(self, node, purpose_id):
        """
        Removes a rule from the queue
        :param node: obj `~easysquid.qnode.QuantumNode`
            Node to remove acception rule for
        :param purpose_id: int
            ID (port) we are closing from incoming entanglement requests
        """
        self.dqp.remove_accept_rule(node.nodeID, purpose_id)

    def load_queue_config(self, queue_config):
        """
        Loads a rule config into the queue
        :param queue_config: dict
            A dictionary of k=nodeID, v=purpose_id values for requests to accept
        """
        self.dqp.load_accept_rules(queue_config)

    def request_other_free_memory(self):
        """
        Requests our peer for their QMM's free memory
        """
        logger.debug("Requesting other node's free memory advertisement")
        my_free_mem = self.qmm.get_free_mem_ad()
        self.conn.put_from(self.node.nodeID, [(self.CMD_REQ_E, my_free_mem)])

    def send_expire_notification(self, aid, createID, originID, new_seq):
        """
        Sends and expiration notification to our peer if MHP Sequence ordering becomes inconsistent
        :param aid: tuple (int, int)
            Absolute queue ID corresponding to the request we were handling when sequence numbers became inconsistent
        :param createID: int
            The createID of the request to be expired
        :param originID: int
            ID of the node that the request originates from
        :param new_seq: int
            The new sequence number of the local node to be informed to peer
        """
        logger.error("Sending EXPIRE notification to peer")
        self.conn.put_from(self.node.nodeID, [(self.CMD_EXPIRE, (aid, createID, originID, new_seq))])

    def cmd_EXPIRE(self, data):
        """
        MHP Sequence numbers became inconsistent with our peer, halt execution of the specified request if we are still
        handling it.  Let our peer know what our expected MHP Sequence number is so that we may both agree on the
        largest
        :param data: tuple (tuple(int, int), int, int, int)
            The absolute queue id of the request in which the error occurred,
             the createID of the request to be expired,
             the originID of the node that created the request,
             and expected mhp sequence number of peer
        :return:
        """
        logger.error("Got EXPIRE command from peer for request {}".format(data))
        aid, createID, originID, new_seq = data

        # If our peer is ahead of us we should update
        if new_seq > self.expected_seq:
            logger.debug("Updated expected sequence to {}".format(new_seq))
            self.expected_seq = new_seq

        # If we are still processing the expired request clear it
        self.scheduler.clear_request(aid)

        # Let our peer know we expired
        self.conn.put_from(self.node.nodeID, [(self.CMD_EXPIRE_ACK, self.expected_seq)])

        # Alert higher layer protocols
        self.issue_err(err=self.ERR_EXPIRE, err_data=(createID, originID))

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
        self.conn.put_from(self.node.nodeID, [(self.CMD_ACK_E, free_memory_size)])

    def cmd_ACK_E(self, data):
        """
        Command handler when our peer has responded with their QMM's free memory.  Stores locally
        :param data: int
            Free memory information from our peer
        """
        logger.debug("Got acknowledgement for free memory ad request, storing: {}".format(data))
        self.scheduler.update_other_mem_size(data)

    # Primary EGP Protocol Methods
    def create(self, cqc_request_raw):
        """
        Main user interface when requesting entanglement.  Adds the request to our queue.
        :param cqc_request_raw: bytes
            The raw CQC request for generating entanglement
        ":return: int
            The create ID
        """
        try:
            # Unpack the request
            egp_request = self._get_egp_request(cqc_request_raw)
            logger.debug("EGP at node {} processing request: {}".format(self.node.nodeID, egp_request))

            # Check if we can support this request
            err = self.check_supported_request(egp_request)
            if err:
                logger.error("Create request failed {}".format(err))
                self.issue_err(err=err)
                return None

            create_id = self._get_next_create_id()

            # Track our peer's available memory
            if not self.scheduler.other_has_resources():
                self.request_other_free_memory()

            # Add the request to the DQP
            success = self._add_to_queue(egp_request, create_id)
            if success:
                logger.debug("Scheduling create event now.")
                self._schedule_now(self._EVT_CREATE)
                return create_id
            else:
                logger.warning("Request was rejected by scheduler.")
                self.issue_err(err=self.ERR_REJECTED)
                return None

        except Exception as err:
            logger.exception("Failed to issue create, due to error {}".format(err))
            self.issue_err(err=self.ERR_CREATE)

    def _get_next_create_id(self):
        """
        Returns the next create ID and increments the counter
        :return:
        """
        create_id = self.next_creation_id
        self.next_creation_id = self.next_creation_id + 1
        logger.debug("Assigned creation id {} to request".format(create_id))

        return create_id

    def check_supported_request(self, creq):
        """
        Performs resource and fulfillment checks to see if the provided request can be satisfied by the EGP.
        :param creq: obj `~qlinklayer.toolbox.CQC_EPR_request_tuple
            The EGP Request that we want to check
        :return: Error code if request fails a check, otherwise 0
        """
        # TODO other ID should be checked before the request is handed to the EGP
        if creq.other_id == self.node.nodeID:
            logger.error("Attempted to submit request for entanglement with self!")
            return self.ERR_CREATE

        if creq.other_id != self.get_otherID():
            logger.error("Attempted to submit request for entanglement with unknown ID!")
            return self.ERR_CREATE

        # Check if we can achieve the minimum fidelity
        if self.feu.get_max_fidelity() < creq.min_fidelity:
            logger.error("Requested fidelity is too high to be satisfied")
            return self.ERR_UNSUPP

        # Check if we can satisfy the request within the given time frame
        attempt_latency = self.mhp_service.get_cycle_time(self.node)

        # Minimum amount of time for measure directly corresponds to the cycle time, attempts can overlap
        if creq.measure_directly:
            min_time = attempt_latency * creq.num_pairs

        # Minimum amount of time for stored entanglement requires obtaining info from midpoint, requests cannot overlap
        else:
            min_time = self.mhp.conn.full_cycle * creq.num_pairs

        # Check against request, max_time of 0 means will wait indefinitely
        if (min_time > creq.max_time or self.dqp.comm_delay > creq.max_time) and (creq.max_time != 0):
            logger.error("Requested max time is too short")
            return self.ERR_UNSUPP

        return 0

    # Queues a request
    def _add_to_queue(self, egp_request, create_id):
        """
        Stores the request in the distributed queue
        :param egp_request: `~qlinklayer.egp.EGPRequest`
            The request we want to store in the distributed queue
        :param create_id: int
            The assigned create ID of this request
        :return: bool
            Whether the add was successful.
        """
        success = self.scheduler.add_request(egp_request, create_id)

        return success

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
            if status == EGPDistributedQueue.DQ_OK:
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
        try:
            if not self.scheduler.other_has_resources():
                self.request_other_free_memory()

            # Get scheduler's next gen task
            gen = self.scheduler.next()
            self.scheduler.inc_cycle()

            if gen.flag:
                # Store the gen for pickup by mhp
                self.mhp_service.put_ready_data(self.node.nodeID, gen)
                return True

            else:
                return False

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

            # Otherwise we are ready to process the reply now
            midpoint_outcome, mhp_seq, aid, proto_err = self._extract_mhp_reply(result=result)

            # # If no absolute queue id is included then info was passed or an error occurred
            # if aid is None:
            #     logger.debug("Handling reply that does not contain an absolute queue id")
            #     self._handle_reply_without_aid(proto_err)

            # Check if an error occurred while processing a request
            if proto_err:
                logger.error("Protocol error occured in MHP: {}".format(proto_err))
                self.issue_err(err=proto_err)
                self._handle_mhp_err(result)
                return

            # Check if this aid may have been expired or timed out while awaiting reply
            elif not self.scheduler.has_request(aid=aid):
                # If we have never seen this aid before we should throw a warning
                if not self.scheduler.previous_request(aid=aid):
                    logger.warning("Got MHP Reply containing aid {} for no current or old request!".format(aid))

                else:
                    logger.debug("Got MHP reply containing aid {} for an old request".format(aid))
                    # Update the MHP Sequence number as necessary
                    if midpoint_outcome in [1, 2]:
                        logger.debug("Updating MHP Seq")
                        self._process_mhp_seq(mhp_seq, aid)

            # Check if the reply came in before our measurement completed, defer processing
            elif self.measurement_in_progress and self.scheduler.is_measure_directly(aid):
                self.measure_directly_reply = result
                return

            # Otherwise this response is associated with a generation attempt
            else:

                # No entanglement generated
                if midpoint_outcome == 0:
                    logger.debug("Failed to produce entanglement with other node")
                    creq = self.scheduler.get_request(aid)
                    if creq is None:
                        logger.error("Request not found!")
                        self.issue_err(err=self.ERR_OTHER)

                    else:
                        # Free the resources for the next attempt
                        self.scheduler.free_gen_resources(aid)

                        # If handling a measure directly request we need to throw away the measurement result
                        if creq.measure_directly and self.scheduler.has_request(aid):
                            m, basis = self.measurement_results.pop(0)
                            logger.debug("Removing measurement outcome {} in basis {} from stored results"
                                         .format(m, basis))

                elif midpoint_outcome in [1, 2]:
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
            r, mhp_seq, aid, proto_err = result
            return r, mhp_seq, aid, proto_err
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

    def _handle_mhp_err(self, result):
        midpoint_outcome, mhp_seq, aid, proto_err = result
        if proto_err == self.mhp.conn.ERR_QUEUE_MISMATCH:
            aidA, aidB = aid
            local_aid = aidA if self.node.nodeID == self.mhp.conn.nodeA.nodeID else aidB
            if mhp_seq >= self.expected_seq:
                self.expected_seq = mhp_seq
                request = self.scheduler.get_request(local_aid)
                if self.dqp.master ^ request.master_request:
                    originID = self.get_otherID()
                else:
                    originID = self.node.nodeID
                createID = request.create_id
                self.send_expire_notification(aid=local_aid, createID=createID, originID=originID,
                                              new_seq=self.expected_seq)

                # Clear the request
                self.scheduler.clear_request(aid=local_aid)

                # Alert higher layer protocols
                self.issue_err(err=self.ERR_EXPIRE, err_data=(createID, originID))

        elif proto_err == self.mhp.conn.ERR_NO_CLASSICAL_OTHER:
            local_aid = aid
            if mhp_seq >= self.expected_seq:
                self.expected_seq = mhp_seq
                request = self.scheduler.get_request(local_aid)
                if self.dqp.master ^ request.master_request:
                    originID = self.get_otherID()
                else:
                    originID = self.node.nodeID
                createID = request.create_id
                self.send_expire_notification(aid=local_aid, createID=createID, originID=originID,
                                              new_seq=self.expected_seq)

                # Clear the request
                self.scheduler.clear_request(aid=local_aid)

                # Alert higher layer protocols
                self.issue_err(err=self.ERR_EXPIRE, err_data=(createID, originID))

        return

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
        creq = self.scheduler.get_request(aid)
        if creq is None:
            logger.error("Request not found!")
            self.issue_err(err=self.ERR_OTHER)

        if creq.measure_directly:
            # Grab the result and correct
            m, basis = self.measurement_results.pop(0)

            # Flip this outcome in the case we need to apply a correction
            creator = not (self.dqp.master ^ creq.master_request)
            if creator:  # True if we're master and request was from master etc.
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
                creator = not (self.dqp.master ^ creq.master_request)
                if creator:  # True if we're master and request was from master etc.
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
        if self.scheduler.is_handling_measure_directly():
            logger.debug("Beginning measurement of qubit for measure directly")
            # Grab the current generation information
            comm_q = self.scheduler.curr_gen.comm_q

            # Constuct a quantum program
            prgm = QuantumProgram()
            q = prgm.get_qubit_indices(1)[0]

            # Make a random basis choice
            basis = random.randint(0, 1)

            if basis:
                logger.debug("Measuring comm_q {} in Hadamard basis".format(comm_q))
                prgm.apply(INSTR_H, q)
            else:
                logger.debug("Measuring comm_q {} in Standard basis".format(comm_q))

            self.basis_choice.append(basis)

            # Set a flag to make sure we catch replies that occur during the measurement
            self.measurement_in_progress = True
            self.scheduler.suspend_generation(self.max_measurement_delay)
            prgm.apply(INSTR_MEASURE, q, output_key="m")
            self.node.qmem.set_program_done_callback(self._handle_measurement_outcome, prgm=prgm)
            self.node.qmem.execute_program(prgm, qubit_mapping=[comm_q])

    def _handle_measurement_outcome(self, prgm):
        """
        Handles the measurement outcome from measureing the communication qubit
        directly after the photon was emitted.
        Calls back to complete MHP reply handling.
        :param prgm: :obj:`netsquid.QuantumProgram`
            The quantum program to be able to access the measurement outcome
        :return: None
        """
        outcome = prgm.output["m"][0]
        # Saves measurement outcome
        self.measurement_in_progress = False
        logger.debug("Measured {} on qubit".format(outcome))

        # If the request did not time out during the measurement then store the result
        if self.scheduler.curr_gen:
            # Free the communication qubit
            comm_q = self.scheduler.curr_gen.comm_q
            self.qmm.vacate_qubit(comm_q)

            # Store the measurement result
            basis = self.basis_choice.pop(0)
            self.measurement_results.append((outcome, basis))

            # If we received a reply for this attempt during measurement we can handle it immediately
            if self.measure_directly_reply:
                self.handle_reply_mhp(self.measure_directly_reply)
                self.measure_directly_reply = None

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
            request = self.scheduler.get_request(aid)
            if self.dqp.master ^ request.master_request:
                creatorID = self.get_otherID()
            else:
                creatorID = self.node.nodeID
            self.send_expire_notification(aid=aid, createID=request.create_id, originID=creatorID, new_seq=new_mhp_seq)

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
        comm_q = self.scheduler.curr_gen.comm_q
        storage_q = self.scheduler.curr_gen.storage_q

        logger.debug("Applying correction to comm_q {} and moving to {}".format(comm_q, storage_q))

        # Construct a quantum program to correct and move
        prgm = QuantumProgram()
        qs = prgm.get_qubit_indices(2)
        prgm.apply(INSTR_Z, qs[0])

        # Check if we need to move the qubit into storage
        if comm_q != storage_q:
            prgm.apply(INSTR_INIT, qs[1])
            qprgms.move_using_CNOTs(prgm, qs[0], qs[1])

        # Set the callback of the program
        self.node.qmem.set_program_done_callback(self._return_ok, mhp_seq=mhp_seq, aid=aid)
        self.node.qmem.execute_program(prgm, qubit_mapping=[comm_q, storage_q])

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
        comm_q = self.scheduler.curr_gen.comm_q
        storage_q = self.scheduler.curr_gen.storage_q

        # Check if a move operation is required
        if comm_q != storage_q:
            logger.debug("Moving comm_q {} to storage {}".format(comm_q, storage_q))

            # Construct a quantum program to move the qubit
            prgm = QuantumProgram()
            qs = prgm.get_qubit_indices(2)
            prgm.apply(INSTR_INIT, qs[1])
            qprgms.move_using_CNOTs(prgm, qs[0], qs[1])

            # Set the callback
            self.node.qmem.set_program_done_callback(self._return_ok, mhp_seq=mhp_seq, aid=aid)
            self.node.qmem.execute_program(prgm, qubit_mapping=[comm_q, storage_q])

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
        fidelity_estimate = self.feu.estimate_fidelity_of_request(creq)

        # Create entanglement identifier
        logical_id = self.scheduler.curr_storage_id()
        if self.dqp.master ^ creq.master_request:
            creatorID = self.get_otherID()
            otherID = self.node.nodeID
        else:
            creatorID = self.node.nodeID
            otherID = self.get_otherID()

        # Construct result information
        now = self.get_current_time()
        t_create = now - self.mhp_service.get_midpoint_comm_delay(self.node)

        # Craft okay based on the request type
        if self.scheduler.is_handling_measure_directly():
            ent_id = (creatorID, otherID, mhp_seq)
            m, basis = self.get_measurement_outcome(creq)
            result = self.construct_cqc_ok_message(EntInfoMeasDirectHeader.type, creq.create_id, ent_id,
                                                   fidelity_estimate, t_create, m=m, basis=basis)

        else:
            ent_id = (creatorID, otherID, mhp_seq)
            t_goodness = t_create
            result = self.construct_cqc_ok_message(EntInfoCreateKeepHeader.type, creq.create_id, ent_id,
                                                   fidelity_estimate, t_create, logical_id=logical_id,
                                                   t_goodness=t_goodness)

        return result

    @staticmethod
    def construct_cqc_ok_message(type, create_id, ent_id, fidelity_estimate, t_create, logical_id=None, t_goodness=None,
                                 m=None, basis=None):
        """
        Construct a CQC message for returning OK of created EPR pair.
        :return: bytes
        """
        # TODO only using appID/port 0 for now
        if type == EntInfoCreateKeepHeader.type:
            cqc_header = CQCHeader()
            length = CQC_XTRA_QUBIT_HDR_LENGTH + ENT_INFO_CREATE_KEEP_LENGTH
            cqc_header.setVals(version=CQC_VERSION, tp=CQC_TP_EPR_OK, app_id=0, length=length)

            cqc_xtra_qubit_header = CQCXtraQubitHeader()
            cqc_xtra_qubit_header.setVals(logical_id)

            creatorID, otherID, mhp_seq = ent_id
            cqc_ent_info_header = EntInfoCreateKeepHeader()
            cqc_ent_info_header.setVals(ip_A=creatorID, port_A=0, ip_B=otherID, port_B=0, mhp_seq=mhp_seq,
                                        t_create=t_create, t_goodness=t_goodness, goodness=fidelity_estimate,
                                        DF=0, create_id=create_id)
            cqc_ok_message = cqc_header.pack() + cqc_xtra_qubit_header.pack() + cqc_ent_info_header.pack()

        elif type == EntInfoMeasDirectHeader.type:
            cqc_header = CQCHeader()
            length = CQC_XTRA_QUBIT_HDR_LENGTH + ENT_INFO_MEAS_DIRECT_LENGTH
            cqc_header.setVals(version=CQC_VERSION, tp=CQC_TP_EPR_OK, app_id=0, length=length)

            creatorID, otherID, mhp_seq = ent_id
            cqc_ent_info_header = EntInfoMeasDirectHeader()
            cqc_ent_info_header.setVals(ip_A=creatorID, port_A=0, ip_B=otherID, port_B=0, mhp_seq=mhp_seq,
                                        meas_out=m, basis=basis, t_create=t_create, goodness=fidelity_estimate,
                                        DF=0, create_id=create_id)
            cqc_ok_message = cqc_header.pack() + cqc_ent_info_header.pack()

        else:
            raise ValueError("Unknown EPR OK message type")

        return cqc_ok_message

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
        creq = self.scheduler.get_request(aid)
        if creq is None:
            logger.error("Request not found!")
            self.issue_err(err=self.ERR_OTHER)

        # Check that aid actually corresponds to a request, measure directly requests may have in-flight messages
        # which arrived after we switched to servicing another request
        if not self.scheduler.is_generating_aid(aid) and not self.scheduler.is_measure_directly(aid):
            logger.error("Request absolute queue IDs mismatch!")
            self.issue_err(err=self.ERR_OTHER)

        # Pass back the okay and clean up
        logger.debug("Issuing okay to caller")
        ok_data = self._create_ok(creq, mhp_seq)
        self.issue_ok(ok_data)

        # Schedule different events depending on the type of gen we completed
        if self.scheduler.is_handling_measure_directly():
            self._schedule_now(self._EVT_BIT_COMPLETED)

        else:
            # Schedule event for entanglement completion
            self._schedule_now(self._EVT_ENT_COMPLETED)

        logger.debug("Marking generation completed")
        self.scheduler.mark_gen_completed(aid=aid)

        # Schedule event if request completed
        if self.scheduler.curr_aid is None:
            logger.debug("Finished request, clearing stored results")
            self.measurement_results = []
            self._schedule_now(self._EVT_REQ_COMPLETED)

    def request_timeout_handler(self, request):
        """
        Handler for requests that were not serviced within their alotted time.  Passes an error along with the
        request up to higher layers.
        :param evt: obj `~qlinklayer.scheduler.SchedulerRequest`
            The request (used to get the Create ID)
        """
        self.issue_err(err=self.ERR_TIMEOUT, err_data=request.create_id)
