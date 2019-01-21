import abc
import numpy as np
from math import floor, ceil
from collections import namedtuple, defaultdict
from functools import partial
from netsquid.pydynaa import EventType, EventHandler
from netsquid.simutil import sim_time
from netsquid.components.instructions import INSTR_Z, INSTR_INIT, INSTR_H, INSTR_ROT_X, INSTR_MEASURE, INSTR_SWAP
from netsquid.components.qprogram import QuantumProgram
from netsquid.qubits import qubitapi as qapi
from easysquid.easyfibre import ClassicalFibreConnection
from easysquid.easyprotocol import EasyProtocol
from easysquid import qProgramLibrary as qprgms
from qlinklayer.toolbox import LinkLayerException
from qlinklayer.scheduler import WFQRequestScheduler
from qlinklayer.distQueue import WFQDistributedQueue
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

    def issue_err(self, err, create_id=None, origin_id=None, old_exp_mhp_seq=None, new_exp_mhp_seq=None):
        """
        Issues an error back to higher layer protocols with the error code that prevented
        successful generation of entanglement
        :param err: obj any
            The error info that occurred while processing the request
        :return: obj any
            The error info
        """
        err_data = []
        for d in [create_id, origin_id, old_exp_mhp_seq, new_exp_mhp_seq]:
            if d is None:
                err_data.append(-1)
            else:
                err_data.append(d)
        logger.debug("Issuing error {} with data {}".format(err, err_data))

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

    # Commands for request expiration
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

    # Emission handler types
    EMIT_HANDLER_NONE = 50
    EMIT_HANDLER_CK = 51
    EMIT_HANDLER_MD = 52

    # Operation types
    OP_NONE = 60
    OP_MOVE = 61
    OP_MEAS = 62
    OP_INIT = 63

    def __init__(self, node, conn=None, err_callback=None, ok_callback=None, throw_local_queue_events=False,
                 accept_all_requests=False, num_priorities=1, scheduler_weights=None):
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
        self.max_memory_init_delay = 0

        # Information to keep track of whether memory qubits requires initalization
        self._next_init_cycle = {}
        self._cycles_per_initialization = {}
        # self._last_memory_init = {}
        # self._memory_decoherence_time = {}

        # Request tracking
        self.expected_seq = 0

        # Store information about expire messages sent to remote peer
        self.waitExpireAcks = {}
        self.comm_delay = 0

        # Communication handlers
        self.commandHandlers = {
            self.CMD_REQ_E: self.cmd_REQ_E,
            self.CMD_ACK_E: self.cmd_ACK_E,
            self.CMD_EXPIRE: self.cmd_EXPIRE,
            self.CMD_EXPIRE_ACK: self.cmd_EXPIRE_ACK
        }

        # Create local share of distributed queue
        self.dqp = WFQDistributedQueue(node=self.node, throw_local_queue_events=throw_local_queue_events,
                                       accept_all=accept_all_requests, numQueues=num_priorities)
        self.dqp.add_callback = self._add_to_queue_callback

        # Create the request scheduler
        self.scheduler = WFQRequestScheduler(distQueue=self.dqp, qmm=self.qmm, weights=scheduler_weights)
        self.scheduler.set_timeout_callback(self.request_timeout_handler)

        # Pydynaa events
        self._EVT_EXPIRE_ACK_TIMEOUT = EventType("EXPIRE COMM TIMEOUT", "Communication timeout for expire message")
        self._EVT_CREATE = EventType("CREATE", "Call to create has completed")
        self._EVT_BIT_COMPLETED = EventType("BIT COMPLETE", "Successfully generated a bit from entangled pair")
        self._EVT_ENT_COMPLETED = EventType("ENT COMPLETE", "Successfully generated an entangled pair of qubits")
        self._EVT_REQ_COMPLETED = EventType("REQ COMPLETE", "Successfully completed a request")

        # Setup program handlers
        self.node.qmem.set_program_done_callback(self._handle_program_done, once=False)
        self.node.qmem.set_program_fail_callback(self._handle_program_failure, once=False)
        self._current_prgm_name = self.OP_NONE
        self._current_prgm = None

        # Measure directly storage and handler
        self.measurement_info = []
        self.move_info = None
        self.init_info = None
        self.midpoint_outcome = None
        self.emission_handling_in_progress = self.EMIT_HANDLER_NONE
        self.mhp_reply = None
        self.measurement_results = defaultdict(list)
        self.corrected_measurements = defaultdict(list)

        # Queue mismatch information
        self._previous_mismatch = None
        self._nr_of_mismatch = None

        # Count MHP cycles used by a gen
        self._used_MHP_cycles = {}
        self._current_create_id = None

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
        cycle_period = self.mhp_service.get_cycle_time(self.node)
        remote_node = self.mhp_service.get_node(nodeID=self.mhp.get_otherID())
        scheduling_offsets = self.mhp_service.get_timing_offsets([self.node, remote_node])
        local_trigger = scheduling_offsets[self.node.nodeID]
        remote_trigger = scheduling_offsets[self.mhp.get_otherID()]
        self.scheduler.configure_mhp_timings(cycle_period=cycle_period,
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

        # Store information about communication delays
        self.comm_delay = self.conn.channel_from_A.delay_mean + egp_conn.channel_from_B.delay_mean
        other_egp.comm_delay = self.comm_delay

        # Store the peer's delay information
        # TODO: assuming that the ID of the communication qubit is 0
        this_move_delays = self.qmm.get_move_delays(0)
        max_this_move_delay = max([time for time in this_move_delays.values() if time != float('inf')])
        peer_move_delays = other_egp.qmm.get_move_delays(0)
        max_peer_move_delay = max([time for time in peer_move_delays.values() if time != float('inf')])
        max_move_delay = max(max_this_move_delay, max_peer_move_delay)
        # TODO: assuming that the ID of the storage qubit is 1
        this_memory_init_delay = self.qmm.get_memory_init_delay(1)
        peer_memory_init_delay = other_egp.qmm.get_memory_init_delay(1)
        self.max_memory_init_delay = max(this_memory_init_delay, peer_memory_init_delay)
        other_egp.max_memory_init_delay = self.max_memory_init_delay

        local_measurement_delay = self.qmm.get_measurement_delay(0)
        remote_measurement_delay = other_egp.qmm.get_measurement_delay(0)
        self.max_measurement_delay = max(local_measurement_delay, remote_measurement_delay)
        other_egp.max_measurement_delay = self.max_measurement_delay
        self.max_move_delay = max_move_delay
        other_egp.max_move_delay = max_move_delay

        mhp_cycle_period = self.scheduler.mhp_cycle_period
        # TODO Only using qubit ID 1
        for qubit_id in [1]:
            _, this_T2 = self.qmm.get_qubit_T1_T2(qubit_id)
            _, peer_T2 = other_egp.qmm.get_qubit_T1_T2(qubit_id)
            if this_T2 == 0:
                if peer_T2 == 0:
                    memory_decoherence_time = 0
                else:
                    memory_decoherence_time = peer_T2
            else:
                if peer_T2 == 0:
                    memory_decoherence_time = this_T2
                else:
                    memory_decoherence_time = min(this_T2, peer_T2)
            if memory_decoherence_time == 0:
                self._cycles_per_initialization[qubit_id] = None
            else:
                self._cycles_per_initialization[qubit_id] = floor(memory_decoherence_time / mhp_cycle_period)
            other_egp._cycles_per_initialization = self._cycles_per_initialization

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

    def send_msg(self, cmd, data):
        """
        Sends a message to the remote node on our connection
        :param cmd: int
            The command we want the peer to execute
        :param data: obj any
            The associated data with the command
        """
        self.conn.put_from(self.node.nodeID, [(cmd, data)])

    def request_other_free_memory(self):
        """
        Requests our peer for their QMM's free memory
        """
        logger.debug("Requesting other node's free memory advertisement")
        my_free_mem = self.qmm.get_free_mem_ad()
        self.send_msg(self.CMD_REQ_E, my_free_mem)

    def send_expire_notification(self, aid, createID, originID, old_seq, new_seq):
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
        # Check if we are waiting for an acknowledgement for this expiry
        if aid not in self.waitExpireAcks:
            logger.error("Sending EXPIRE notification to peer")
            self._send_exp_msg(aid, createID, originID, old_seq, new_seq)

            # Schedule communication timeout to make sure we alert peer of expired request
            evt = self._schedule_after(2 * self.comm_delay, self._EVT_EXPIRE_ACK_TIMEOUT)
            timeout_handler = EventHandler(partial(self.expire_ack_timeout, aid=aid))
            self._wait_once(timeout_handler, event=evt)

            # Store information about expired request
            self.waitExpireAcks[aid] = (createID, originID, old_seq, new_seq)

    def _send_exp_msg(self, aid, createID, originID, old_seq, new_seq):
        """
        Sends an expire message to our peer
        :param aid: tuple of (int, int)
            The absolute queue id of the request to be expired
        :param createID: int
            The create id of the request being expired
        :param originID: int
            The nodeID of the creator of the request
        :param new_seq: int
            The local sequence number we have
        """
        self.send_msg(self.CMD_EXPIRE, (aid, createID, originID, old_seq, new_seq))

    def expire_ack_timeout(self, evt, aid):
        """
        Timeout handler that checks if we received an ACK for the expire message we sent
        :param evt: obj `~netsquid.pydynaa.Event`
            The event triggering this handler
        :param aid: tuple of (int, int)
            The absolute queue id of the request we tried to tell remote peer about
        """
        # Check if we are still waiting for the ack
        if aid in self.waitExpireAcks:
            logger.warning("Did not receive expire ACK in time, retransmitting")

            # Remove local information of the expire message
            createID, originID, seq_start, seq_end = self.waitExpireAcks.pop(aid)

            # Reattempt expiration and include up to date sequence info
            self.send_expire_notification(aid, createID, originID, seq_start, seq_end)

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
        aid, createID, originID, old_seq, new_seq = data

        # Alert higher layer protocols
        self.issue_err(err=self.ERR_EXPIRE, create_id=createID, origin_id=originID, old_exp_mhp_seq=old_seq, new_exp_mhp_seq=new_seq - 1)

        # If our peer is ahead of us we should update
        if new_seq > self.expected_seq:
            logger.debug("Updated expected sequence to {}".format(new_seq))
            self.expected_seq = new_seq

        # May receive expire multiple times if ack lost, clear only if we have the request locally
        request = self.scheduler.get_request(aid)
        if request is not None:
            # If we are still processing the expired request clear it
            self.scheduler.clear_request(aid)
            if request.measure_directly:
                self._remove_measurement_data(aid)

        # Let our peer know we expired
        self._send_exp_ack(aid)

    def _send_exp_ack(self, aid):
        """
        Sends an expire acknowledgement to the remote peer
        :param aid: tuple of (int, int)
            The expired absolute queue id we are acknowledging
        """
        self.send_msg(self.CMD_EXPIRE_ACK, (aid, self.expected_seq))

    def cmd_EXPIRE_ACK(self, data):
        """
        Process acknowledgement of expiration.  Update our local expected mhp sequence number if our peer was ahead
        of us
        :param data: tuple of (aid, int)
            The absolute queue id and expected mhp sequence number of our peer
        :return:
        """
        logger.error("Got EXPIRE ACK command from peer")

        # Check if our peer was ahead and we need to update
        aid, other_expected_seq = data
        if other_expected_seq > self.expected_seq:
            logger.debug("Updated expected sequence to {}".format(other_expected_seq))
            self.expected_seq = other_expected_seq

        # Remove so that we don't retransmit the expire message
        if aid in self.waitExpireAcks:
            self.waitExpireAcks.pop(aid)

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
        self.send_msg(self.CMD_ACK_E, free_memory_size)

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
            logger.error("Requested fidelity {} is too high to be satisfied, maximum achievable is {}"
                         .format(creq.min_fidelity, self.feu.get_max_fidelity()))
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
            if status == WFQDistributedQueue.DQ_OK:
                logger.debug("Completed adding item to Distributed Queue, got result: {}".format(result))

            # Otherwise bubble up the DQP error
            else:
                logger.error("Error occurred adding request to distributed queue!")
                self.issue_err(err=status, create_id=creq.create_id)

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

            # Request memory update when out of resources
            if not self.scheduler.other_has_resources():
                self.request_other_free_memory()

            self.scheduler.inc_cycle()

            # Get scheduler's next gen task
            gen = self.scheduler.next()

            if gen.flag:
                # Keep track of used MHP cycles per request (data collection)
                qid, qseq = self.scheduler.curr_aid
                request = self.scheduler.distQueue.local_peek(qid, qseq).request
                create_id = request.create_id
                self._current_create_id = create_id
                if create_id not in self._used_MHP_cycles:
                    self._used_MHP_cycles[create_id] = 1
                else:
                    self._used_MHP_cycles[create_id] += 1

                if gen.storage_q != gen.comm_q:
                    # Check that storage qubit is already initialized
                    if self._memory_needs_initialization(gen.storage_q):
                        self.initialize_storage(gen.storage_q)
                        return False

                # If we are storing the qubit prevent additional attempts until we have a reply or have timed out
                if not self.scheduler.is_handling_measure_directly():
                    suspend_time = self.scheduler.mhp_full_cycle
                    logger.debug("Next generation attempt after {}".format(suspend_time))
                    self.scheduler.suspend_generation(suspend_time)

                # Store the gen for pickup by mhp
                self.mhp_service.put_ready_data(self.node.nodeID, gen)

            else:
                # Keep track of used MHP cycles per request (data collection)
                if self._current_create_id is not None:
                    if self.scheduler.suspended() or self.scheduler.qmm.is_busy():
                        self._used_MHP_cycles[self._current_create_id] += 1

            return gen.flag

        except Exception:
            logger.exception("Error occurred when triggering MHP!")
            self.issue_err(err=self.ERR_OTHER)
            return False

    def initialize_storage(self, qubit_id):
        """
        Initializes the qubit if possible, if so, suspends the scheduler
        :param qubit_id:
        :return:
        """
        logger.debug("Node {} : Memory qubit {} needs initalization".format(self.node.name, qubit_id))
        if self.scheduler.suspended():
            logger.debug("Node {} : Scheduler is suspended".format(self.node.name))
            return
        elif self.qmm.is_busy():
            logger.debug("Node {} : QMM is busy".format(self.node.name))
            return
        elif self.qmm.reserved_qubits[qubit_id]:
            logger.debug("Node {} : Qubit ID {} is reserved".format(self.node.name, qubit_id))
            return
        else:
            logger.debug("Node {} : Initializing qubit {} in cycle {}".format(self.node.name, qubit_id, self.scheduler.mhp_cycle_number))
            if self._cycles_per_initialization[qubit_id] is not None:
                this_cycle = self.scheduler.mhp_cycle_number
                init_delay_cycles = ceil(self.max_memory_init_delay / self.scheduler.mhp_cycle_period)
                dec_cycles = self._cycles_per_initialization[qubit_id]
                self._next_init_cycle[qubit_id] = (this_cycle + init_delay_cycles + dec_cycles) % self.scheduler.max_mhp_cycle_number
            else:
                self._next_init_cycle[qubit_id] = None
            # self.init_info = qubit_id
            # self._last_memory_init[qubit_id] = None
            prgm = QuantumProgram()
            q = prgm.get_qubit_indices(1)[0]
            prgm.apply(INSTR_INIT, q)
            self.scheduler.suspend_generation(self.max_memory_init_delay)
            # self.reset_program_callback()

            self._current_prgm = prgm
            self._current_prgm_name = self.OP_INIT
            self.node.qmem.execute_program(prgm, qubit_mapping=[qubit_id])

    def _handle_program_failure(self):
        """
        Just prints the error of a program failed
        :return:
        """
        logger.error("Node {} : QuantumProgram failed because {}".format(self.node.name, self.node.qmem.failure_exception))

    def _handle_program_done(self):
        """
        Handles the finish of a measure, init or swap program
        :param operation: str
            "meas", "move" or "init"
        :return:
        """
        if self._current_prgm_name == self.OP_MEAS:
            logger.debug("Node {} : Handling meas program done".format(self.node.name))
            self._handle_measurement_outcome()
        elif self._current_prgm_name == self.OP_MOVE:
            logger.debug("Node {} : Handling move program done".format(self.node.name))
            self._handle_move_completion()
        elif self._current_prgm_name == self.OP_INIT:
            logger.debug("Node {} : Handling init program done".format(self.node.name))
            self._handle_init_completion()
        else:
            raise ValueError("Unknown operation")

    def _handle_init_completion(self):
        """
        Handles the completion of an initialization
        :return:
        """
        pass

    def _memory_needs_initialization(self, qubit_id):
        """
        Checks if qubit should be initialized to be ready to be used.
        :param qubit_id:
        :return:
        """
        # Has the qubit been initialized and not used
        if qubit_id not in self._next_init_cycle:
            return True
        else:
            # Does this qubit have infinite decoherence time?
            if self._cycles_per_initialization[qubit_id] is None:
                return False
            else:
                curr_cycle = self.scheduler.mhp_cycle_number
                if self.scheduler._compare_mhp_cycle(self._next_init_cycle[qubit_id], curr_cycle) <= 0:
                    return True
                else:
                    return False

    # Callback handler to be given to MHP so that EGP updates when request is satisfied
    def handle_reply_mhp(self, result):
        """
        Handler for processing the reply from the MHP service
        :param result: tuple
            Contains the processing result information from attempting entanglement generation
        """
        try:
            logger.debug("Node {} : Handling MHP Reply: {} in cycle {}".format(self.node.name, result, self.scheduler.mhp_cycle_number))

            # Otherwise we are ready to process the reply now
            midpoint_outcome, mhp_seq, aid, proto_err = self._extract_mhp_reply(result=result)
            self._remove_old_measurement_results(aid)

            # Check if an error occurred while processing a request
            if proto_err:
                logger.error("Protocol error occured in MHP: {}".format(proto_err))
                self._handle_mhp_err(result)

            # Check if this aid may have been expired or timed out while awaiting reply
            elif not self.scheduler.has_request(aid=aid):
                self.clear_if_handling_emission(aid)
                # Update the MHP Sequence number as necessary
                if midpoint_outcome in [1, 2]:
                    logger.debug("Updating MHP Seq")
                    self._process_mhp_seq(mhp_seq, aid)
                if self.scheduler.previous_request(aid=aid):
                    logger.debug("Got MHP Reply containing aid {} a previous request!".format(aid))
                else:
                    # If we have never seen this aid before we should throw a warning
                    logger.warning("Got MHP reply containing aid {} for an unknown request".format(aid))

            # Check if the reply came in before our emission handling completed
            elif self.emission_handling_in_progress == self.EMIT_HANDLER_CK:
                raise RuntimeError("Shouldn't be handling CK emit")
                # if midpoint_outcome == 0:
                #     self.clear_if_handling_emission(aid)
                # else:
                #     print("suspended cycles = {}".format(self.scheduler.num_suspended_cycles))
                #     self.mhp_reply = result

            # Check if we have results for this aid
            elif self.emission_handling_in_progress == self.EMIT_HANDLER_MD and len(self.measurement_results[aid]) == 0:
                self.mhp_reply = result

            # Otherwise this response is associated with a generation attempt where emission handling is finished
            # and we are ready to process
            else:
                # No entanglement generated
                if midpoint_outcome == 0:
                    logger.debug("Failed to produce entanglement with other node")
                    creq = self.scheduler.get_request(aid)

                    if creq is None:
                        logger.error("Request not found!")
                        self.issue_err(err=self.ERR_OTHER)

                    else:
                        # Resume generation
                        self.scheduler.resume_generation()

                        # Free the resources for the next attempt
                        self.scheduler.free_gen_resources(aid)

                        # If handling a measure directly request we need to throw away the measurement result
                        if creq.measure_directly and self.scheduler.has_request(aid):
                            try:
                                ecycle, m, basis = self.measurement_results[aid].pop(0)
                                logger.debug("Removing measurement outcome {} in basis {} for aid {} (failed attempt)"
                                             .format(m, basis, aid))
                            except IndexError:
                                pass

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
            self.issue_err(err=self.ERR_OTHER)

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

    def _handle_mhp_err(self, result):
        """
        Handles errors from the MHP
        :param result: tuple
            Result data returned by MHP
        """
        # Unpack the results
        midpoint_outcome, mhp_seq, aid, proto_err = result

        # If the error occurred while program was running stop the program and free the resources
        self.clear_if_handling_emission(aid)

        # Process the error
        if proto_err == self.mhp.conn.ERR_QUEUE_MISMATCH or proto_err == self.mhp.conn.ERR_NO_CLASSICAL_OTHER:
            # Get our absolute queue id based on error
            if proto_err == self.mhp.conn.ERR_QUEUE_MISMATCH:
                aidA, aidB = aid
                local_aid, remote_aid = (aidA, aidB) if self.node.nodeID == self.mhp.conn.nodeA.nodeID else (aidB, aidA)

                # Increment mismatch counter if we received this before
                if aid == self._previous_mismatch:
                    self._nr_of_mismatch += 1
                else:
                    self._previous_mismatch = aid
                    self._nr_of_mismatch = 1

                max_nr_mismatch = int(1.5 * self.scheduler.mhp_full_cycle / self.scheduler.mhp_cycle_period)
                if self._nr_of_mismatch > max_nr_mismatch:
                    for a in aid:
                        qid, qseq = a
                        if self.dqp.contains_item(qid, qseq):
                            req = self.dqp.remove_item(qid, qseq).request
                            if self.dqp.master ^ req.master_request:
                                originID = self.get_otherID()
                            else:
                                originID = self.node.nodeID
                            self.send_expire_notification(aid=a, createID=req.create_id, originID=originID, old_seq=self.expected_seq, new_seq=self.expected_seq)
                            self.issue_err(err=self.ERR_EXPIRE, create_id=req.create_id, origin_id=originID, old_exp_mhp_seq=self.expected_seq, new_exp_mhp_seq=self.expected_seq - 1)
                        else:
                            self.send_expire_notification(aid=a, createID=None, originID=None, old_seq=self.expected_seq, new_seq=self.expected_seq)
            else:
                local_aid = aid

            # If we still have the request issue and error
            if self.scheduler.has_request(local_aid):
                self.issue_err(err=proto_err)

            # Check if we may have lost a message
            if mhp_seq >= self.expected_seq:
                # Issue an expire for the request
                request = self.scheduler.get_request(local_aid)
                new_mhp_seq = mhp_seq + 1
                if request is not None:
                    if self.dqp.master ^ request.master_request:
                        originID = self.get_otherID()
                    else:
                        originID = self.node.nodeID

                    createID = request.create_id
                    self.send_expire_notification(aid=local_aid, createID=createID, originID=originID,
                                                  old_seq=self.expected_seq, new_seq=new_mhp_seq)

                    # Clear the request
                    self.scheduler.clear_request(aid=local_aid)
                    if request.measure_directly:
                        # Pop the earliest measurement results if it exists
                        try:
                            self.measurement_results[local_aid].pop(0)
                        except IndexError:
                            pass
                        # self._remove_measurement_data(aid)

                    # Alert higher layer protocols
                    self.issue_err(err=self.ERR_EXPIRE, create_id=createID, origin_id=originID, old_exp_mhp_seq=self.expected_seq, new_exp_mhp_seq=mhp_seq)

                # Update our expected seq, because error came back we should expect the subsequent seq
                self.expected_seq = new_mhp_seq

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
            return

        # Get comm and storage qubit
        comm_q = self.scheduler.curr_gen.comm_q
        storage_q = self.scheduler.curr_gen.storage_q

        # Check if the corresponding request is measure directly
        if creq.measure_directly:
            # Grab the result and correct
            try:
                ecycle, m, basis = self.measurement_results[aid].pop(0)
                logger.debug("Removing measurement outcome {} in basis {} for aid {} (successful attempt)"
                             .format(m, basis, aid))
            except IndexError:
                logger.warning("Trying to grab a measurement result but there are no there")
                return

            # Flip this outcome in the case we need to apply a correction
            creator = not (self.dqp.master ^ creq.master_request)
            if creator:  # True if we're master and request was from master etc.
                # Measurements in computational basis are always anti-correlated for the entangled state
                if basis == 0:
                    m ^= 1

                # Measurements in hadamard and Y basis are only anti-correlated when r == 2
                elif (basis == 1 or basis == 2) and r == 2:
                    m ^= 1

            # Pass up the meaurement info to higher layers
            self.corrected_measurements[aid].append((m, basis))
            self._return_ok(mhp_seq, aid)
        # We need to move state to memory qubit
        elif comm_q != storage_q:
            self.midpoint_outcome = r
            self.move_info = mhp_seq, aid, storage_q
            self._move_comm_to_storage(comm_q, storage_q)
        # Otherwise we're leaving the state in the communication qubit and just return the ok
        else:
            self.midpoint_outcome = r
            self._return_ok(mhp_seq=mhp_seq, aid=aid)

    def _handle_photon_emission(self, evt):
        """
        Catches the event produced when MHP has emitted a photon to the midpoint.  The EGP then checks if the current
        request requires measurement of the qubit immediately and acts as such.  If the current request is for a
        create and keep request and the specified storage id is different from the communicaiton qubit initiate
        a move.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered this handler
        """
        # Get request resources
        comm_q = self.scheduler.curr_gen.comm_q
        storage_q = self.scheduler.curr_gen.storage_q

        logger.debug("Handling photon emission")

        if self.scheduler.is_handling_measure_directly():
            logger.debug("Beginning measurement of qubit for measure directly")
            # Set a flag to make sure we catch replies that occur during the measurement
            self.emission_handling_in_progress = self.EMIT_HANDLER_MD

            # Constuct a quantum program
            prgm = QuantumProgram()
            q = prgm.get_qubit_indices(1)[0]

            # Select the basis based on the mhp cycle number
            possible_bases = [0, 1, 2]
            basis = possible_bases[self.scheduler.mhp_cycle_number % len(possible_bases)]

            if basis == 0:
                logger.debug("Measuring comm_q {} in Standard basis".format(comm_q))
            elif basis == 1:
                logger.debug("Measuring comm_q {} in Hadamard basis".format(comm_q))
                prgm.apply(INSTR_H, q)
            else:
                logger.debug("Measuring comm_q {} in Y basis".format(comm_q))
                prgm.apply(INSTR_ROT_X, q, angle=np.pi / 2)

            # Store the aid and basis for retrieval post measurement
            self.measurement_info.append((self.scheduler.curr_aid, basis, comm_q))

            prgm.apply(INSTR_MEASURE, q, output_key="m")
            # self.node.qmem.set_program_done_callback(self._handle_measurement_outcome, prgm=prgm)

            self._current_prgm = prgm
            self._current_prgm_name = self.OP_MEAS
            self.node.qmem.execute_program(prgm, qubit_mapping=[comm_q])

        # elif comm_q != storage_q:
        #     self._move_comm_to_storage(comm_q, storage_q)

        else:
            logger.debug("Entangled qubit will remain in comm_q until midpoint reply")

    def _move_comm_to_storage(self, comm_q, storage_q):
        """
        Moves the state in the communication qubit to a specified storage qubit.
        Suspends scheduler during this time
        :param comm_q:
        :param storage_q:
        :return:
        """
        logger.debug("Node {} : Moving comm_q {} to storage_q {}".format(self.node.name, comm_q, storage_q))
        if self.node.qmem._memory_positions[storage_q]._qubit is None:
            raise RuntimeError("No qubit before trying to swap")

        # Reset init info of this storage qubit
        self._next_init_cycle.pop(storage_q)

        # Construct a quantum program to correct and move
        prgm = QuantumProgram()
        qs = prgm.get_qubit_indices(2)
        qprgms.move_using_CXDirections(prgm, qs[0], qs[1])

        # Set the callback of the program
        self.scheduler.suspend_generation(self.max_move_delay)
        # self.node.qmem.set_program_done_callback(self._handle_move_completion, prgm=prgm)

        self._current_prgm = prgm
        self._current_prgm_name = self.OP_MOVE
        self.node.qmem.execute_program(prgm, qubit_mapping=[comm_q, storage_q])

    def handling_emission(self, aid):
        """
        Checks if we are handling photon emission for the specified aid
        :param aid: tuple (int, int)
            The absolute queue id to check if we are handling
        :return:
        """
        if self.emission_handling_in_progress == self.EMIT_HANDLER_NONE:
            return False

        # Handle create and keep program
        elif self.emission_handling_in_progress == self.EMIT_HANDLER_CK:
            raise RuntimeError("Shouldn't be handling CK after emission now")

        # Handle measure directly program
        else:
            emit_aid, _, _ = self.measurement_info[0]

        return emit_aid == aid

    def clear_if_handling_emission(self, aid):
        """
        Stops the program and clears internal information if we are currently handling photon emission for the
        specified absolute queue id
        :param aid: tuple (int, int)
            The absolute queue id to check for
        :return:
        """
        # Check if we are handling the emission for this aid
        if self.handling_emission(aid):
            # Halt handling
            # logger.info("Stopping swap program")
            # self.node.qmemory.stop_program()

            # Allow scheduler to resume
            self.scheduler.resume_generation()

            # Remove emission handling metadata
            if self.emission_handling_in_progress == self.EMIT_HANDLER_CK:
                self.move_info = None

            # else:
            #     self.measurement_results.pop(0)

    def _remove_old_measurement_results(self, aid):
        cycle = self.scheduler.mhp_cycle_number
        rtt_cycles = floor(self.mhp_service.get_midpoint_rtt_delay(self.node) / self.scheduler.mhp_cycle_period)
        while self.measurement_results[aid]:
            emission_cycle, basis, outcome = self.measurement_results[aid][0]
            if emission_cycle >= cycle - rtt_cycles:
                break
            elif emission_cycle < cycle - rtt_cycles:
                logger.warning("Failed to get expected reply, removing measurement result ")
                self.measurement_results[aid].pop(0)

    def _handle_measurement_outcome(self):
        """
        Handles the measurement outcome from measureing the communication qubit
        directly after the photon was emitted.
        Calls back to complete MHP reply handling.
        :return: None
        """
        prgm = self._current_prgm
        self._current_prgm_name = self.OP_NONE
        self._current_prgm = None

        outcome = prgm.output["m"][0]

        # Saves measurement outcome
        self.emission_handling_in_progress = self.EMIT_HANDLER_NONE
        logger.debug("Measured {} on qubit".format(outcome))

        # If the request did not time out during the measurement then store the result
        try:
            aid, basis, comm_q = self.measurement_info.pop(0)
        except IndexError:
            logger.error("No measurement info when handling measurement")
            return

        if self.scheduler.has_request(aid):
            # Free the communication qubit
            # comm_q = self.scheduler.curr_gen.comm_q
            self.qmm.vacate_qubit(comm_q)

            # Store the measurement result
            ecycle = self.scheduler.mhp_cycle_number
            self.measurement_results[aid].append((ecycle, outcome, basis))
            logger.debug("Adding measurement outcome {} in basis {} for aid {}".format(outcome, basis, aid))

            # If we received a reply for this attempt during measurement we can handle it immediately
            if self.mhp_reply:
                self.handle_reply_mhp(self.mhp_reply)
                self.mhp_reply = None

        elif self.scheduler.previous_request(aid):
            logger.debug("Handling measurement outcome from request that is already completed with aid {}".format(aid))
        else:
            logger.warning("Handling measurement outcome for request that is not held by"
                           "the scheduler, with aid {}".format(aid))

    def _remove_measurement_data(self, aid):
        """
        Clears measurement data associated with provided aid for measure directly requests.
        :param aid: tuple (int, int)
            The absolute queue id of the request to clear measurement data for
        """
        self.measurement_results[aid] = []
        self.corrected_measurements[aid] = []

    def _handle_move_completion(self):
        """
        Handles completion of the move for create and keep requests that specify store.  If the MHP reply came in
        during the move then we proceed to handle it at this point
        :param prgm:
        :return:
        """
        self._current_prgm_name = self.OP_NONE
        self._current_prgm = None

        self.emission_handling_in_progress = self.EMIT_HANDLER_NONE
        if self.move_info is None:
            raise RuntimeError("No move info")
        mhp_seq, aid, storage_q = self.move_info
        self.move_info = None
        logger.debug("Node {} : Completed moving comm_q to storage_q".format(self.node.name))
        if self.scheduler.curr_gen and self.scheduler.curr_aid == aid:
            # Return ok
            self._return_ok(mhp_seq, aid)

        else:
            logger.warning("Scheduler no longer processing aid {}!  Freeing storage qubit")
            self.qmm.vacate_qubit(storage_q)

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
            if request is not None:
                if self.dqp.master ^ request.master_request:
                    creatorID = self.get_otherID()
                else:
                    creatorID = self.node.nodeID
                self.send_expire_notification(aid=aid, createID=request.create_id, originID=creatorID,
                                              old_seq=self.expected_seq, new_seq=new_mhp_seq)

                # Alert higher layer protocols
                self.issue_err(err=self.ERR_EXPIRE, old_exp_mhp_seq=self.expected_seq, new_exp_mhp_seq=new_mhp_seq - 1)

                # Clear the request
                self.scheduler.clear_request(aid=aid)
                if request.measure_directly:
                    self._remove_measurement_data(aid)

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

    def get_measurement_outcome(self, aid):
        """
        Returns the oldest measurement outcome along with its basis.
        :param creq: obj ~qlinklayer.SchedulerRequest
            The request to construct the okay for
        :return: tuple of int, int
            The measurement outcome and basis in which it was measured
        """
        # Retrieve the measurement outcome and basis
        m, basis = self.corrected_measurements[aid].pop(0)
        return m, basis

    def _create_ok(self, creq, aid, mhp_seq):
        """
        Crafts an OK to issue to higher layers depending on the current generation.  If measure_directly we construct
        and ent_id that excludes the logical_id.  The ok for a measure_directly request contains the measurement
        outcome and basis.
        :param creq: obj ~qlinklayer.SchedulerRequest
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
        if creq.measure_directly:
            ent_id = (creatorID, otherID, mhp_seq)
            m, basis = self.get_measurement_outcome(aid)
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
            length = ENT_INFO_MEAS_DIRECT_LENGTH
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

        # Pass back the okay and clean up
        logger.debug("Issuing okay to caller")
        ok_data = self._create_ok(creq, aid, mhp_seq)
        self.issue_ok(ok_data)

        # Schedule different events depending on the type of gen we completed
        if creq.measure_directly:
            self._schedule_now(self._EVT_BIT_COMPLETED)

        else:
            # Schedule event for entanglement completion
            self._schedule_now(self._EVT_ENT_COMPLETED)

        logger.debug("Marking generation completed")
        self.scheduler.mark_gen_completed(aid=aid)

        # Schedule event if request completed
        if not self.scheduler.has_request(aid):
            logger.debug("Finished request, clearing stored results")
            if creq.measure_directly:
                self._remove_measurement_data(aid)
            self._schedule_now(self._EVT_REQ_COMPLETED)

    def request_timeout_handler(self, aid, request):
        """
        Handler for requests that were not serviced within their alotted time.  Passes an error along with the
        request up to higher layers.
        :param evt: obj `~qlinklayer.scheduler.SchedulerRequest`
            The request (used to get the Create ID)
        """
        self.issue_err(err=self.ERR_TIMEOUT, create_id=request.create_id)
        if request.measure_directly:
            self._remove_measurement_data(aid)
