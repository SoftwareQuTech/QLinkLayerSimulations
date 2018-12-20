import abc
import random
from collections import OrderedDict
from easysquid.toolbox import logger
from easysquid.easyprotocol import TimedProtocol
from netsquid.pydynaa import EventType
from netsquid.simutil import sim_time
from netsquid import get_qstate_formalism, DM_FORMALISM, KET_FORMALISM, STAB_FORMALISM
from SimulaQron.cqc.backend.cqcHeader import CQCHeader, CQCCmdHeader, CQCEPRRequestHeader, CQC_TP_COMMAND, \
    CQC_CMD_EPR, CQC_VERSION, CQC_CMD_HDR_LENGTH, CQC_EPR_REQ_LENGTH, CQC_HDR_LENGTH, CQCXtraQubitHeader, \
    CQC_XTRA_QUBIT_HDR_LENGTH
from SimulaQron.cqc.backend.entInfoHeader import EntInfoCreateKeepHeader, EntInfoMeasDirectHeader,\
    ENT_INFO_MEAS_DIRECT_LENGTH, ENT_INFO_CREATE_KEEP_LENGTH


class EGPSimulationScenario(TimedProtocol):
    def __init__(self, egp, request_cycle, request_prob=1, min_pairs=1, max_pairs=1, min_fidelity=0.2, tmax_pair=0,
                 num_requests=0, purpose_id=1, priority=10, store=False, atomic=False, measure_directly=False, t0=0):
        """
        EGP simulation scenario that schedules create calls onto the EGP and acts as a higher layer protocol that can
        collect the ok messages and errors returned by the EGP operation.
        A request is scheduled every 'request_cycle' with probability 'request_prob'.
        The number of pairs per request is a random integer between 'min_pairs' and 'max_pairs'.
        If 'num_requests > 0', then only 'num_requests' are created.
        :param egp: obj `~qlinklayer.egp.EGP`
            The EGP we want to call to for entanglement generation
        :param request_cycle: float
            Every 'request_cycle' there is an request with probability 'request_cycle'.
        :param request_prob: float
            Every 'request_cycle' there is an request with probability 'request_cycle'.
        :param min_pairs: int
            Minimum number of pairs per request.
        :param max_pairs: int
            Maximum number of pairs per request.
        :param min_fidelity: float
            Minimum fidelity for request.
        :param tmax_pair: float
            Maximum waiting time per pair for request. Maximum waiting time for request is then 'num_pairs*tmax_pair'.
        :param num_requests: int
            Maximum number of requests (0 is treated as infinite)
        :param purpose_id: int
            The purpose ID of this request.
        :param priority: int
            The priority of this request.
        :param store: bool
            Whether to move the qubit to a memory qubit after entanglement is generated.
        :param measure_directly:
            Whether the communication qubit should be measured directly after a photon is emitted.
        :param t0: float
            When this protocol should start.

        """
        super(EGPSimulationScenario, self).__init__(timeStep=request_cycle, t0=t0, node=egp.node)
        # Check input
        if min_pairs > max_pairs:
            raise ValueError("'min_pairs' cannot be larger than 'max_pairs'.")

        # Our EGP
        self.egp = egp

        # Request probability
        self.request_prob = request_prob

        # Request data
        self.otherID = self._get_other_node_ID()
        self.min_pairs = min_pairs
        self.max_pairs = max_pairs
        self.min_fidelity = min_fidelity
        self.tmax_pair = tmax_pair
        if num_requests > 0:
            self.num_requests = num_requests
        else:
            self.num_requests = float('inf')
        self.purpose_id = purpose_id
        self.priority = priority
        self.store = store
        self.atomic = atomic
        self.measure_directly = measure_directly

        # Store the current number of created requests
        self.created_requests = 0

        # Hook up a handler to the ok events
        self.egp.ok_callback = self.ok_callback
        self._EVT_OK = EventType("EGP OK", "Triggers when egp has issued an ok message")

        # Hook up a handler to the error events
        self.egp.err_callback = self.err_callback
        self._EVT_ERR = EventType("EGP ERR", "Triggers when egp has issued an err message")

        # Hook up a handler to the create events
        self.create_storage = []
        self._EVT_CREATE = EventType("EGP CREATE", "Triggers when create was called")

        # Data collection
        self.ok_storage = []
        self.entangled_qstates = {}
        self.node_measurement_storage = {}
        self.err_storage = []

    def run_protocol(self):
        """
        Calls the EGP to make a request for entanglement
        :return: None
        """
        if self.created_requests >= self.num_requests:
            return
        # Note that we always schedule the first event, to see something interesting in the simulations
        if (random.random() <= self.request_prob) or (self.created_requests == 0 and self.request_prob > 0):
            # Number of pairs
            num_pairs = random.randint(self.min_pairs, self.max_pairs)

            # Max time for request
            max_time = num_pairs * self.tmax_pair

            # Create a request
            cqc_request_raw = self.construct_cqc_epr_request(otherID=self.otherID, num_pairs=num_pairs,
                                                             min_fidelity=self.min_fidelity, max_time=max_time,
                                                             purpose_id=self.purpose_id, priority=self.priority,
                                                             store=self.store, atomic=self.atomic,
                                                             measure_directly=self.measure_directly)

            # Give the request to the egp
            self._create(cqc_request_raw)

            self.created_requests += 1

    def _get_other_node_ID(self):
        """
        Returns the node ID of the other node.
        :return: int
        """
        idA = self.egp.conn.idA
        idB = self.egp.conn.idB

        if self.node.nodeID == idA:
            return idB
        else:
            return idA

    @staticmethod
    def construct_cqc_epr_request(otherID, num_pairs=1, min_fidelity=0.5, max_time=0, purpose_id=0, priority=0,
                                  store=True, atomic=False, measure_directly=False):
        """
        Construct a CQC message for creating an EPR pair, to be passed to the create method of EGP.
        :return: bytes
        """
        cqc_header = CQCHeader()
        cqc_header.setVals(version=CQC_VERSION, tp=CQC_TP_COMMAND, app_id=purpose_id,
                           length=CQC_CMD_HDR_LENGTH + CQC_EPR_REQ_LENGTH)

        cqc_cmd_header = CQCCmdHeader()
        cqc_cmd_header.setVals(qubit_id=0, instr=CQC_CMD_EPR, notify=True, block=True, action=False)

        cqc_epr_request_header = CQCEPRRequestHeader()
        cqc_epr_request_header.setVals(remote_ip=otherID, remote_port=0, num_pairs=num_pairs, min_fidelity=min_fidelity,
                                       max_time=max_time, priority=priority, store=store, atomic=atomic,
                                       measure_directly=measure_directly)

        cqc_message = cqc_header.pack() + cqc_cmd_header.pack() + cqc_epr_request_header.pack()
        return cqc_message

    @staticmethod
    def unpack_cqc_ok(results):
        """
        Unpacks the CQC message containing the OK from EGP.
        If measure direct, cqc_xtra_qubit_header will be None.
        :param result: bytes
        :return: tuple (cqc_header, cqc_xtra_qubit_header, cqc_ent_info_header)
        """
        # Read header
        cqc_header = CQCHeader(results[:CQC_HDR_LENGTH])
        results = results[CQC_HDR_LENGTH:]
        if cqc_header.length == CQC_XTRA_QUBIT_HDR_LENGTH + ENT_INFO_CREATE_KEEP_LENGTH:
            cqc_xtra_qubit_header = CQCXtraQubitHeader(results[:CQC_XTRA_QUBIT_HDR_LENGTH])
            results = results[CQC_XTRA_QUBIT_HDR_LENGTH:]
            cqc_ent_info_header = EntInfoCreateKeepHeader(results[:ENT_INFO_CREATE_KEEP_LENGTH])
        elif cqc_header.length == ENT_INFO_MEAS_DIRECT_LENGTH:
            cqc_xtra_qubit_header = None
            cqc_ent_info_header = EntInfoMeasDirectHeader(results[:ENT_INFO_MEAS_DIRECT_LENGTH])
        else:
            raise ValueError("Could not parse cqc message, unknown length")

        return cqc_header, cqc_xtra_qubit_header, cqc_ent_info_header

    def _create(self, cqc_request_raw):
        """
        Internal method for calling the EGP's create method and storing the creation id and timestamp info for
        data collection
        :param cqc_request_raw: bytes
            The raw CQC request consisting of a CQCHeader + CQCCmdHeader and CQCEPRRequestHeader
        """
        # Only extract result information if the create was successfully submitted
        create_id = self.egp.create(cqc_request_raw=cqc_request_raw)
        create_time = sim_time()
        if create_id is not None:
            self.create_storage.append((self.egp.node.nodeID, cqc_request_raw, create_id, create_time))
            logger.debug("Scheduling create event now.")
            self._schedule_now(self._EVT_CREATE)

    def get_create_info(self, remove=True):
        """
        For use by data collectors to track the creation information
        :param remove: bool
            Whether to remove this data from the storage
        :return: tuple of (int, float)
            Creation ID and simulation timestamp of create call
        """
        create_info = self.create_storage.pop(0) if remove else self.create_storage[0]
        return create_info

    def ok_callback(self, result):
        """
        Handler for oks issued by the EGP containing generation result information.  Schedules an event for data
        collection
        :param result: tuple
            The result of our create request
        """
        self._ok_callback(result)
        logger.debug("Scheduling OK event now.")
        self._schedule_now(self._EVT_OK)

    @abc.abstractmethod
    def _ok_callback(self, result):
        """
        Internal handler for ok messages
        :param result: tuple
            Result from the EGP
        """
        pass

    def err_callback(self, result):
        """
        Handler for errors thrown by the EGP during the simulation.  Schedules an event for data collection
        :param result: tuple
            Result information containing the error
        """
        self._err_callback(result)
        logger.debug("Scheduling error event now.")
        self._schedule_now(self._EVT_ERR)

    def get_ok(self, remove=True):
        """
        Returns the oldest ok message that we received during the simulation
        :param remove: bool
            Whether to remove the ok from the scenario's storage
        :return: tuple
            Ok information
        """
        ok = self.ok_storage.pop(0) if remove else self.ok_storage[0]
        return ok

    def get_error(self, remove=True):
        """
        Returns the oldest error that we received during the simulation
        :param remove: bool
            Whether to remove the error data from the scenario's storage
        """
        err = self.err_storage.pop(0) if remove else self.err_storage[0]
        return err

    def _err_callback(self, result):
        """
        Collects the errors from the EGP and stores them for data collection
        :param result: tuple
            Contains the error information from the EGP
        """
        now = sim_time()
        logger.error("{} got error {} at time {}".format(self.node.nodeID, result, now))
        self.err_storage.append(result)

    def get_measurement(self, ent_id=None, remove=True):
        """
        Returns the measurement result corresponding to the given entanglement id.
        If ent_id is None, the first item (ent_id, meas_data) is returned
        :param remove: bool
            Whether to remove the measurement result from the scenario's storage
        :param ent_id: tuple or None
            The entanglement ID
        :return: tuple
            Returns the entanglement ID and the measurement data as (ent_id, meas_data)
        """
        if ent_id is None:
            try:
                # Get the key of the first item
                ent_id = next(iter(self.node_measurement_storage))
                if remove:
                    meas_data = self.node_measurement_storage.pop(ent_id)
                else:
                    meas_data = self.node_measurement_storage[ent_id]
                return ent_id, meas_data
            except StopIteration:
                return None, None
        else:
            try:
                if remove:
                    meas_data = self.node_measurement_storage.pop(ent_id)
                else:
                    meas_data = self.node_measurement_storage[ent_id]
                return ent_id, meas_data
            except KeyError:
                return None, None

    def store_qstate(self, qubit_id, source_id, other_id, mhp_seq):
        """
        Extracts the qubit state based on the used formalism and stores it locally for collection
        :param qubit_id: int
            The qubit ID in memory that we want the state of
        :param source_id: int
        :param other_id: int
        :param mhp_seq: int
        """
        qstate = self.node.qmem.peek(qubit_id)[0].qstate
        formalism = get_qstate_formalism()
        key = (source_id, other_id, mhp_seq)

        # if formalism == DM_FORMALISM and qstate.dm.shape == (4, 4):
        if formalism == DM_FORMALISM:
            self.entangled_qstates[key] = qstate.dm

        # elif formalism == KET_FORMALISM and qstate.ket.shape == (4, 1):
        elif formalism == KET_FORMALISM:
            self.entangled_qstates[key] = qstate.ket

        elif formalism == STAB_FORMALISM:
            self.entangled_qstates[key] = qstate.stab

        else:
            raise RuntimeError("Unknown state formalism")


class MeasureAfterSuccessScenario(EGPSimulationScenario):
    def __init__(self, egp, request_cycle, request_prob=1, min_pairs=1, max_pairs=1, min_fidelity=0.2, tmax_pair=0,
                 num_requests=0, purpose_id=1, priority=10, store=False, atomic=False, t0=0):
        """
        A simulation scenario that will immediately measure any entangled qubits generated by the EGP.
        EGP simulation scenario that schedules create calls onto the EGP and acts as a higher layer protocol that can
        collect the ok messages and errors returned by the EGP operation.
        A request is scheduled every 'request_cycle' with probability 'request_prob'.
        The number of pairs per request is a random integer between 'min_pairs' and 'max_pairs'.
        If 'num_requests > 0', then only 'num_requests' are created.
        :param egp: obj `~qlinklayer.egp.EGP`
            The EGP we want to call to for entanglement generation
        :param request_cycle: float
            Every 'request_cycle' there is an request with probability 'request_cycle'.
        :param request_prob: float
            Every 'request_cycle' there is an request with probability 'request_cycle'.
        :param min_pairs: int
            Minimum number of pairs per request.
        :param max_pairs: int
            Maximum number of pairs per request.
        :param min_fidelity: float
            Minimum fidelity for request.
        :param tmax_pair: float
            Maximum waiting time per pair for request. Maximum waiting time for request is then 'num_pairs*tmax_pair'.
        :param num_requests: int
            Maximum number of requests (0 is treated as infinite)
        :param purpose_id: int
            The purpose ID of this request.
        :param priority: int
            The priority of this request.
        :param store: bool
            Whether to move the qubit to a memory qubit after entanglement is generated.
        :param t0: float
            When this protocol should start.

        """
        super(MeasureAfterSuccessScenario, self).__init__(egp=egp, request_cycle=request_cycle,
                                                          request_prob=request_prob, min_pairs=min_pairs,
                                                          max_pairs=max_pairs, min_fidelity=min_fidelity,
                                                          tmax_pair=tmax_pair, num_requests=num_requests,
                                                          purpose_id=purpose_id, priority=priority, store=store,
                                                          atomic=atomic, t0=t0)

        # EGP internal objects
        self.qmm = egp.qmm

        # Data storage from collected info
        self.ok_storage = []
        self.entangled_qstates = {}
        self.node_measurement_storage = []
        self.err_storage = []

    def _ok_callback(self, result):
        """
        Internal handler for collecting oks issued by the EGP.  Will extract the result information from the EGP,
        measure the qubit in the memory and release it making it available to the EGP again.
        :param result: tuple
            OK information returned by the EGP
        """
        # Store data for retrieval
        self.ok_storage.append(result)

        # Extract fields from result
        create_id, ent_id, logical_id, _, _, _ = self.unpack_cqc_ok(result)
        _, peer_id, mhp_seq = ent_id

        # Store the qubit state for collection
        self.store_qstate(logical_id, create_id, peer_id, mhp_seq)

        # Measure the logical qubit in the result
        [outcome], _ = self.node.qmem.measure([logical_id])

        now = sim_time()
        logger.info("{} measured {} for ent_id {} at time {}".format(self.node.nodeID, outcome, ent_id, now))

        # Store the measurement result for data collection
        self.node_measurement_storage.append((mhp_seq, outcome))

        # Free the qubit for the EGP
        self.qmm.free_qubit(logical_id)

    @staticmethod
    def unpack_cqc_ok(result):
        """
        Unpacks the CQC message containing the OK from EGP of type create-and-keep
        :param result: bytes
        :return: tuple (create_id, ent_id, logical_id, f_goodness, t_create, t_goodness)
        """
        result = result[CQC_HDR_LENGTH:]
        cqc_xtra_qubit_header = CQCXtraQubitHeader(result[:CQC_XTRA_QUBIT_HDR_LENGTH])
        result = result[CQC_XTRA_QUBIT_HDR_LENGTH:]
        cqc_ent_info_header = EntInfoCreateKeepHeader(result)

        create_id = cqc_ent_info_header.create_id
        creator_id = cqc_ent_info_header.ip_A
        peer_id = cqc_ent_info_header.ip_B
        mhp_seq = cqc_ent_info_header.mhp_seq
        ent_id = (creator_id, peer_id, mhp_seq)
        logical_id = cqc_xtra_qubit_header.qubit_id
        f_goodness = cqc_ent_info_header.goodness
        t_create = cqc_ent_info_header.t_create
        t_goodness = cqc_ent_info_header.t_goodness

        return create_id, ent_id, logical_id, f_goodness, t_create, t_goodness


class MeasureBeforeSuccessScenario(EGPSimulationScenario):
    def __init__(self, egp, request_cycle, request_prob=1, min_pairs=1, max_pairs=1, min_fidelity=0.2, tmax_pair=0,
                 num_requests=0, purpose_id=1, priority=10, store=False, atomic=False, t0=0):
        """
        Scenario for when spin is measured directly after photon is emitted, i.e. before messages is returned
        from midpoint. The classical information from the choice of measurement basis and measurement
        outcome can be used to compute QubErr and/or produce key.
      i  EGP simulation scenario that schedules create calls onto the EGP and acts as a higher layer protocol that can
        collect the ok messages and errors returned by the EGP operation.
        A request is scheduled every 'request_cycle' with probability 'request_prob'.
        The number of pairs per request is a random integer between 'min_pairs' and 'max_pairs'.
        If 'num_requests > 0', then only 'num_requests' are created.
        :param egp: obj `~qlinklayer.egp.EGP`
            The EGP we want to call to for entanglement generation
        :param request_cycle: float
            Every 'request_cycle' there is an request with probability 'request_cycle'.
        :param request_prob: float
            Every 'request_cycle' there is an request with probability 'request_cycle'.
        :param min_pairs: int
            Minimum number of pairs per request.
        :param max_pairs: int
            Maximum number of pairs per request.
        :param min_fidelity: float
            Minimum fidelity for request.
        :param tmax_pair: float
            Maximum waiting time per pair for request. Maximum waiting time for request is then 'num_pairs*tmax_pair'.
        :param num_requests: int
            Maximum number of requests (0 is treated as infinite)
        :param purpose_id: int
            The purpose ID of this request.
        :param priority: int
            The priority of this request.
        :param store: bool
            Whether to move the qubit to a memory qubit after entanglement is generated.
        :param t0: float
            When this protocol should start.

        """

        super(MeasureBeforeSuccessScenario, self).__init__(egp=egp, request_cycle=request_cycle,
                                                           request_prob=request_prob, min_pairs=min_pairs,
                                                           max_pairs=max_pairs, min_fidelity=min_fidelity,
                                                           tmax_pair=tmax_pair, num_requests=num_requests,
                                                           purpose_id=purpose_id, priority=priority, store=store,
                                                           atomic=atomic, measure_directly=True, t0=t0)

        # EGP internal objects
        self.qmm = egp.qmm

        # Data storage from collected info
        self.ok_storage = []
        self.node_measurement_storage = OrderedDict()
        self.err_storage = []

    def _ok_callback(self, result):
        """
        Internal handler for collecting oks issued by the EGP. Will extract the result information from EGP.
        :param result: tuple
            OK information returned by the EGP
        :return: None
        """
        # Store data for retrieval
        self.ok_storage.append(result)

        # Extract fields from result
        _, ent_id, outcome, basis, _, _ = self.unpack_cqc_ok(result)

        # Store the basis/bit choice and the midpoint outcomes for QubErr or key generation
        meas_data = (basis, outcome)

        self.node_measurement_storage[ent_id] = meas_data

    @staticmethod
    def unpack_cqc_ok(result):
        """
        Unpacks the CQC message containing the OK from EGP of type measure-directly
        :param result: bytes
        :return: tuple (create_id, ent_id, meas_out, basis, f_goodness, t_create)
        """
        result = result[CQC_HDR_LENGTH:]
        cqc_ent_info_header = EntInfoMeasDirectHeader(result)

        create_id = cqc_ent_info_header.create_id
        creator_id = cqc_ent_info_header.ip_A
        peer_id = cqc_ent_info_header.ip_B
        mhp_seq = cqc_ent_info_header.mhp_seq
        ent_id = (creator_id, peer_id, mhp_seq)
        meas_out = cqc_ent_info_header.meas_out
        basis = cqc_ent_info_header.basis
        f_goodness = cqc_ent_info_header.goodness
        t_create = cqc_ent_info_header.t_create

        return create_id, ent_id, meas_out, basis, f_goodness, t_create
