import abc
import numpy as np
from easysquid.puppetMaster import PM_SQLDataSequence
from easysquid.toolbox import logger
from netsquid.pydynaa import Entity, EventHandler
from netsquid.simutil import warn_deprecated
from qlinklayer.egp import NodeCentricEGP, EGPRequest
from qlinklayer.scenario import MeasureBeforeSuccessScenario, MeasureAfterSuccessScenario
from SimulaQron.cqc.backend.entInfoHeader import EntInfoMeasDirectHeader, EntInfoCreateKeepHeader


class EGPDataSequence(PM_SQLDataSequence, metaclass=abc.ABCMeta):
    def __init__(self, name, dbFile, column_names=None, maxSteps=1000):
        super(EGPDataSequence, self).__init__(name=name, dbFile=dbFile, column_names=column_names, maxSteps=maxSteps)

    @abc.abstractmethod
    def get_column_names(self):
        pass

    def sumData(self, val, succ):
        pass


class EGPDataPoint(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, data=None):
        """
        Abstract class to be used to decode raw data from data sequences.
        :param data: Should be this class or a list or tuple of data arguments
        """
        if data:
            if isinstance(data, EGPDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.argument = None

    @abc.abstractmethod
    def from_raw_data(self, data):
        try:
            self.argument = data[0]
        except IndexError:
            raise ValueError("Cannot parse data")

    @abc.abstractmethod
    def from_data_point(self, data):
        if isinstance(data, EGPDataPoint):
            self.argument = data.argument
        else:
            raise ValueError("'data' is not an instance of this class")

    @abc.abstractmethod
    def printable_data(self):
        to_print = "EGP Data-point:\n"
        to_print += "   Argument: {}\n".format(self.argument)
        return to_print

    def __repr__(self):
        return self.printable_data()


class EGPErrorSequence(EGPDataSequence):
    """
    Collects error events and their error codes thrown by an EGP
    """

    def get_column_names(self):
        return ["Timestamp", "Node ID", "Error Code", "Success"]

    def getData(self, time, source=None):
        error_source = source[0]
        error_code, error_info = error_source.get_error()
        return [(error_source.egp.node.nodeID, error_code), True]


class EGPErrorDataPoint(EGPDataPoint):
    def __init__(self, data=None):
        if data:
            if isinstance(data, EGPErrorDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.timestamp = None
            self.node_id = None
            self.error_code = None
            self.success = None

    def from_raw_data(self, data):
        try:
            self.timestamp = data[0]
            self.node_id = data[1]
            self.error_code = data[2]
            self.success = data[3]
        except IndexError:
            raise ValueError("Cannot parse data")

    def from_data_point(self, data):
        if isinstance(data, EGPErrorDataPoint):
            self.timestamp = data.timestamp
            self.node_id = data.node_id
            self.error_code = data.error_code
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP Error Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    Node ID: {}\n".format(self.node_id)
        to_print += "    Error Code: {}\n".format(self.error_code)
        to_print += "    Success: {}\n".format(self.success)


class EGPCreateSequence(EGPDataSequence):
    """
    Collects CREATE events from an EGP including the CREATE events
    """

    def get_column_names(self):
        # TODO ITEMS AFTER "Timestamp and "Node ID" currently have to be sorted....
        return ["Timestamp", "Node ID", "CQC Request", "Create ID", "Create Time", "Success"]

    def getData(self, time, source=None):
        nodeID, cqc_request, create_id, create_time = source[0].get_create_info()
        return [(nodeID, cqc_request, create_id, create_time), True]


class EGPCreateDataPoint(EGPDataPoint):
    def __init__(self, data=None):
        if data:
            if isinstance(data, EGPCreateDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.timestamp = None
            self.node_id = None
            self.create_id = None
            self.create_time = None
            self.max_time = None
            self.measure_directly = None
            self.min_fidelity = None
            self.num_pairs = None
            self.other_id = None
            self.priority = None
            self.store = None
            self.success = None

    def from_raw_data(self, data):
        try:
            self.timestamp, self.node_id, cqc_request, create_id, create_time, self.success = data
            request = EGPRequest(cqc_request=cqc_request)
            request.assign_create_id(create_id, create_time)
            self.create_id = request.create_id
            self.create_time = request.create_time
            self.max_time = request.max_time
            self.measure_directly = request.measure_directly
            self.min_fidelity = request.min_fidelity
            self.num_pairs = request.num_pairs
            self.other_id = request.otherID
            self.priority = request.priority
            self.store = request.store
        except Exception as err:
            raise ValueError("Cannot parse data since {}".format(err))

    def from_data_point(self, data):
        if isinstance(data, EGPCreateDataPoint):
            self.timestamp = data.timestamp
            self.node_id = data.node_id
            self.create_id = data.create_id
            self.create_time = data.create_time
            self.max_time = data.max_time
            self.measure_directly = data.measure_directly
            self.min_fidelity = data.min_fidelity
            self.num_pairs = data.num_pairs
            self.other_id = data.other_id
            self.priority = data.priority
            self.store = data.store
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP Create Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    Node ID: {}\n".format(self.node_id)
        to_print += "    Create ID: {}\n".format(self.create_id)
        to_print += "    Create Time: {}\n".format(self.create_time)
        to_print += "    Max Time: {}\n".format(self.max_time)
        to_print += "    Measure Directly: {}\n".format(self.measure_directly)
        to_print += "    Min Fidelity: {}\n".format(self.min_fidelity)
        to_print += "    Num Pairs: {}\n".format(self.num_pairs)
        to_print += "    Other ID: {}\n".format(self.other_id)
        to_print += "    Priority: {}\n".format(self.priority)
        to_print += "    Store: {}\n".format(self.store)
        to_print += "    Success: {}\n".format(self.success)
        return to_print


class EGPOKSequence(EGPDataSequence):
    def __init__(self, name, dbFile, attempt_collectors=None, column_names=None, maxSteps=1000):
        """
        Collects OK events from an EGP including the CREATE events and number of attempts.
        :param attempt_collector: dct of :obj:`qlinklayer.datacollection.AttemptCollector`
            The attempt collectors for the nodes, keys are the node IDs
        """
        super(EGPOKSequence, self).__init__(name=name, dbFile=dbFile, column_names=column_names, maxSteps=maxSteps)

        self._attempt_collectors = attempt_collectors

    def get_column_names(self):
        return ["Timestamp", "Node ID", "OK_TYPE", "CQC_OK", "Attempts", "Success"]

    def getData(self, time, source=None):
        scenario = source[0]
        ok = scenario.get_ok()
        try:
            create_id, ent_id, _, _, _, _ = MeasureAfterSuccessScenario.unpack_cqc_ok(ok)
            ok_type = EntInfoCreateKeepHeader.type
        except ValueError:
            try:
                create_id, ent_id, _, _, _, _ = MeasureBeforeSuccessScenario.unpack_cqc_ok(ok)
                ok_type = EntInfoMeasDirectHeader.type
            except ValueError:
                raise ValueError("Unknown OK type")
        other_id = ent_id[1]

        nodeID = scenario.node.nodeID

        # Get number of attempts
        if self._attempt_collectors:
            try:
                attempt_collector = self._attempt_collectors[nodeID]
            except KeyError:
                nr_attempts = -1
            else:
                nr_attempts = attempt_collector.get_attempts(create_id, other_id)
        else:
            nr_attempts = -1

        data = [nodeID, ok_type, ok, nr_attempts]
        return [data, True]


class EGPOKDataPoint(EGPDataPoint):
    def __init__(self, data=None):
        if data:
            if isinstance(data, EGPOKDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.timestamp = None
            self.ok_type = None
            self.node_id = None
            self.create_id = None
            self.origin_id = None
            self.other_id = None
            self.mhp_seq = None
            self.logical_id = None
            self.measurement_outcome = None
            self.measurement_basis = None
            self.goodness = None
            self.goodness_time = None
            self.create_time = None
            self.attempts = None
            self.success = None

    def from_raw_data(self, data):
        self.timestamp = data[0]
        self.node_id = data[1]
        self.ok_type = data[2]
        ok = data[3]
        self.attempts = data[4]
        self.success = data[5]

        if self.ok_type == EntInfoMeasDirectHeader.type:
            self.create_id, ent_id, self.meas_out, self.basis, self.goodness,\
                self.t_create = MeasureBeforeSuccessScenario.unpack_cqc_ok(ok)
            self.origin_id, self.other_id, self.mhp_seq = ent_id
        elif self.ok_type == EntInfoCreateKeepHeader.type:
            self.create_id, ent_id, self.logical_id, self.goodness, self.t_create,\
                self.t_goodness = MeasureAfterSuccessScenario.unpack_cqc_ok(ok)
            self.origin_id, self.other_id, self.mhp_seq = ent_id
        else:
            raise ValueError("Cannot parse data")

    def from_data_point(self, data):
        if isinstance(data, EGPOKDataPoint):
            self.timestamp = data.timestamp
            self.ok_type = data.ok_type
            self.node_id = data.node_id
            self.create_id = data.create_id
            self.origin_id = data.origin_id
            self.other_id = data.other_id
            self.mhp_seq = data.mhp_seq
            self.logical_id = data.logical_id
            self.measurement_outcome = data.measurement_outcome
            self.measurement_basis = data.measurement_basis
            self.goodness = data.goodness
            self.goodness_time = data.goodness_time
            self.create_time = data.create_time
            self.attempts = data.attempts
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP OK Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    OK Type: {}\n".format("CK" if self.ok_type == NodeCentricEGP.CK_OK else "MD")
        to_print += "    Node ID: {}\n".format(self.node_id)
        to_print += "    Create ID: {}\n".format(self.create_id)
        to_print += "    Origin ID: {}\n".format(self.origin_id)
        to_print += "    Other ID: {}\n".format(self.other_id)
        to_print += "    MHP Seq: {}\n".format(self.mhp_seq)
        if self.ok_type == NodeCentricEGP.CK_OK:
            to_print += "    Logical ID: {}\n".format(self.logical_id)
            to_print += "    Goodness: {}\n".format(self.goodness)
            to_print += "    Goodness Time: {}\n".format(self.goodness_time)
        else:
            to_print += "    Measurement Outcome: {}\n".format(self.measurement_outcome)
            to_print += "    Measurement Basis: {}\n".format(self.measurement_basis)
        to_print += "    Create Time: {}\n".format(self.create_time)
        to_print += "    Attempts: {}\n".format(self.attempts)
        to_print += "    Success: {}\n".format(self.success)
        return to_print


class EGPStateSequence(EGPDataSequence):
    """
    Collects qubit states of generated entangled pairs
    """

    def __init__(self, *args, **kwargs):
        super(EGPStateSequence, self).__init__(*args, **kwargs)

        # Keep track of what states have been collected
        self._collected_states = []

    def get_column_names(self):
        matrix_columns = ["{}, {}, {}".format(i, j, k) for i in range(4) for j in range(4) for k in ['real', 'imag']]
        return ["Timestamp", "Node ID"] + matrix_columns + ["Success"]

    def getData(self, time, source=None):
        scenario = source[0]
        key, qstate = scenario.entangled_qstates.popitem()

        # Check if we already collected the state
        # If so return None to tell the pupperMaster to not record this data point
        if key in self._collected_states:
            return [None, True]
        else:
            self._collected_states.append(key)
            nodeID = scenario.egp.node.nodeID
            val = [nodeID] + [n for sl in [[z.real, z.imag] for z in qstate.flat] for n in sl]
            return [val, True]


class EGPStateDataPoint(EGPDataPoint):
    def __init__(self, data=None):
        if data:
            if isinstance(data, EGPStateDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.timestamp = None
            self.node_id = None
            self.density_matrix = None
            self.success = None

    def from_raw_data(self, data):
        try:
            self.timestamp = data[0]
            self.node_id = data[1]

            # Construct the matrix
            m_data = data[2:34]
            density_matrix = np.matrix(
                [[m_data[i] + 1j * m_data[i + 1] for i in range(k, k + 8, 2)] for k in range(0, len(m_data), 8)])
            self.density_matrix = density_matrix
            self.success = data[34]
        except IndexError:
            raise ValueError("Cannot parse data")

    def from_data_point(self, data):
        if isinstance(data, EGPStateDataPoint):
            self.timestamp = data.timestamp
            self.node_id = data.node_id
            self.density_matrix = data.density_matrix
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP State Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    Node ID: {}\n".format(self.node_id)
        to_print += "    Density Matrix: {}\n".format(self.density_matrix)
        to_print += "    Success: {}\n".format(self.success)
        return to_print


class EGPQubErrSequence(EGPDataSequence):
    """
    Collects Qub-errors of from measurement outcomes at the nodes and the midpoint
    We use the following entries:
        -1: No data
        0 : No qub-error
        1 : Qub-error
    """

    def get_column_names(self):
        return ["Timestamp", "Z_err", "X_err", "Success"]

    def getData(self, time, source=None):
        # Get scenarios
        scenarioA = source[0]
        scenarioB = source[1]

        # Get latest measurement data from A
        ent_id, meas_dataA = scenarioA.get_measurement(remove=False)
        if ent_id is None:
            # No data yet
            return [[-1, -1], False]

        # Check if B also got the measurement data yet
        _, meas_dataB = scenarioB.get_measurement(ent_id=ent_id, remove=True)

        if meas_dataB is None:
            # B hasn't received the corresponding OK yet, try next time
            return [[-1, -1], False]

        # Got measurement data from both A and B, delete entry from A
        scenarioA.get_measurement(ent_id=ent_id, remove=True)

        # Check qub-err

        # Get basis and bit choices
        basis_choiceA = meas_dataA[0]
        basis_choiceB = meas_dataB[0]
        bit_choiceA = meas_dataA[1]
        bit_choiceB = meas_dataB[1]

        # Check if equal basis choices
        if basis_choiceA != basis_choiceB:
            return [-1, -1], False

        error = 1 if bit_choiceA != bit_choiceB else 0
        if basis_choiceA == 0:  # Standard basis
            return [error, -1], True
        else:  # Hadamard basis
            return [-1, error], True


class EGPQubErrDataPoint(EGPDataPoint):
    def __init__(self, data=None):
        if data:
            if isinstance(data, EGPQubErrDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.timestamp = None
            self.z_err = None
            self.x_err = None
            self.success = None

    def from_raw_data(self, data):
        try:
            self.timestamp = data[0]
            self.z_err = data[1]
            self.x_err = data[2]
            self.success = data[3]
        except IndexError:
            raise ValueError("Cannot parse data")

    def from_data_point(self, data):
        if isinstance(data, EGPQubErrDataPoint):
            self.timestamp = self.timestamp
            self.z_err = self.z_err
            self.x_err = self.x_err
            self.success = self.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP QubErr Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    Z Error: {}\n".format(self.z_err)
        to_print += "    X Error: {}\n".format(self.x_err)
        to_print += "    Success: {}\n".format(self.success)
        return to_print


class EGPLocalQueueSequence(EGPDataSequence):
    """
    Collects additions and removals of items from a local queue.
    """

    def get_column_names(self):
        return ["Timestamp", "Change", "Seq", "Success"]

    def getData(self, time, source=None, trigger=None):
        local_queue = source[0]
        if trigger == local_queue._EVT_ITEM_ADDED:
            data = 1, local_queue._seqs_added.pop(0)
        elif trigger == local_queue._EVT_ITEM_REMOVED:
            data = -1, local_queue._seqs_removed.pop(0)
        else:
            raise ValueError("Unknown event triggered collection of queue length")

        return data, True


class EGPLocalQueueDataPoint(EGPDataPoint):
    def __init__(self, data=None):
        if data:
            if isinstance(data, EGPLocalQueueDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.timestamp = None
            self.change = None
            self.seq = None
            self.success = None

    def from_raw_data(self, data):
        try:
            self.timestamp = data[0]
            self.change = data[1]
            self.seq = data[2]
            self.success = data[3]
        except IndexError:
            raise ValueError("Cannot parse data")

    def from_data_point(self, data):
        if isinstance(data, EGPLocalQueueDataPoint):
            self.timestamp = data.timestamp
            self.change = data.change
            self.seq = data.seq
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP Local Queue Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.argument)
        to_print += "    Change: {}\n".format(self.change)
        to_print += "    Queue Seq: {}\n".format(self.seq)
        to_print += "    Success: {}\n".format(self.success)
        return to_print


class MHPNodeEntanglementAttemptSequence(EGPDataSequence):
    """
    Collects entanglement attempts that occur at the end nodes
    """

    def __init__(self, *args, **kwargs):
        super(MHPNodeEntanglementAttemptSequence, self).__init__(*args, **kwargs)

        warn_deprecated("EGPOkDataSequence now collects the number of attempts, use this instead")

    def get_column_names(self):
        return ["Timestamp", "Nr Attempts", "Queue ID", "Queue Seq", "Success"]

    def getData(self, time, source=None):
        # Get the scenario and MHP
        scenario = source[0]
        mhp = scenario.egp.mhp

        (aid, nr_of_attempts) = mhp._succesful_gen_attempts.popitem()

        data = (nr_of_attempts,) + aid

        return [data, True]


class MHPMidpointEntanglementAttemptSequence(EGPDataSequence):
    """
    Collects entanglement attempts that occur at the midpoint
    """

    def __init__(self, *args, **kwargs):
        super(MHPMidpointEntanglementAttemptSequence, self).__init__(*args, **kwargs)

        warn_deprecated(
            "This is not a good way to count the number of attempts. EGPOKDateSequence now collects node attempts"
            " by counting them instead of registering each one as a data point. We should do the same if we wish to"
            " also collect the attempts at the midpoint.")

    def get_column_names(self):
        return ["Timestamp", "Nr Attempts", "Queue ID", "Queue Seq", "Success"]

    def getData(self, time, source=None):
        # Get the scenario and MHP
        scenario = source[0]
        mhp_conn = scenario.egp.mhp.conn

        (aid, nr_of_attempts) = mhp_conn._succesful_gen_attempts.popitem()

        data = (nr_of_attempts,) + aid

        return [data, True]


class AttemptCollector(Entity):
    def __init__(self, egp):
        """
        Counts the entanglement generation attempts done at a node
        These can then be retrieved using the create ID.

        :param egp: :obj:`qlinklayer.egp.NodeCentricEGP`
            The EGP at the node
        """
        super(AttemptCollector, self).__init__()

        self._egp = egp
        self._mhp = self._egp.mhp

        # Listen to the entanglement attempts at the nodes and midpoint
        self.evt_handler = EventHandler(self._attempt_handler)
        self._wait(self.evt_handler, entity=self._mhp, event_type=self._mhp._EVT_ENTANGLE_ATTEMPT)

        # Data storage
        self._attempts = {}

    def _attempt_handler(self, event):
        """
        Handles the entanglement attempts at the nodes and midpoint and collects the data
        :param event:
        :return:
        """
        # The node transmitted a photon
        # Get the absolute queue ID
        aid = self._mhp.aid

        # Get the current request, the create ID and the ID of other
        request = self._egp.scheduler.get_request(aid=aid)
        if request:
            create_id = request.create_id
            other_id = request.otherID

            self._register_attempt(create_id, other_id)
        else:
            logger.warning("Entanglement attempt occurred without request")

    def _register_attempt(self, create_id, other_id):
        """
        Register the attempt in the storage
        """
        key = (create_id, other_id)
        if key in self._attempts:
            self._attempts[key] += 1
        else:
            self._attempts[key] = 1

    def get_attempts(self, create_id, other_id, remove=True):
        """
        Returns the number current number of attempts for this create_id and other_id
        :param create_id: int
        :param other_id: int
        :param remove: bool
        :return: int
        """
        key = (create_id, other_id)
        if remove:
            try:
                return self._attempts.pop(key)
            except KeyError:
                logger.warning("No attempt info for create ID {} and other ID {}".format(create_id, other_id))
                return None
        else:
            try:
                return self._attempts[key]
            except KeyError:
                logger.warning("No attempt info for create ID {} and other ID {}".format(create_id, other_id))
                return None

    def get_all_remaining_attempts(self):
        """
        Returns the total number of attempts not already collected.
        Useful for the end of the simulation to collect attempts for unsuccesful generations
        :return: int
        """
        total_attempts = 0
        for (key, attempts) in self._attempts.items():
            total_attempts += attempts
        return total_attempts
