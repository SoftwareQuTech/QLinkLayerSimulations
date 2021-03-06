import abc
import numpy as np
from easysquid.puppetMaster import PM_SQLDataSequence
from easysquid.toolbox import logger
from netsquid.pydynaa import Entity, EventHandler
from netsquid.simutil import warn_deprecated
from netsquid.qubits import qubitapi as qapi
from qlinklayer.egp import EGP
from qlinklayer.scenario import MeasureBeforeSuccessScenario, MeasureAfterSuccessScenario
from cqc.backend.entInfoHeader import EntInfoMeasDirectHeader, EntInfoCreateKeepHeader


current_version = 1


class EGPData:
    version = current_version


class EGPDataSequence(PM_SQLDataSequence, EGPData, metaclass=abc.ABCMeta):
    def __init__(self, name, dbFile, column_names=None, maxSteps=1000):
        super(EGPDataSequence, self).__init__(name=name, dbFile=dbFile, column_names=column_names, maxSteps=maxSteps)

    @abc.abstractmethod
    def get_column_names(self):
        pass

    def sumData(self, val, succ):
        pass


class EGPDataPoint(EGPData, metaclass=abc.ABCMeta):
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
        return ["Timestamp", "Node ID", "Error Code", "Create ID", "Origin ID", "Old Exp MHP Seq", "New Exp MHP Seq",
                "Success"]

    def getData(self, time, source=None):
        error_source = source[0]
        error_code, error_info = error_source.get_error()
        data = [error_source.egp.node.nodeID, error_code] + list(error_info)
        return [data, True]


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
            self.create_id = None
            self.origin_id = None
            self.old_exp_mhp_seq = None
            self.new_exp_mhp_seq = None
            self.success = None

    def from_raw_data(self, data):
        try:
            self.timestamp = data[0]
            self.node_id = data[1]
            self.error_code = data[2]
            self.create_id = data[3]
            self.origin_id = data[4]
            self.old_exp_mhp_seq = data[5]
            self.new_exp_mhp_seq = data[6]
            self.success = data[7]
        except IndexError:
            raise ValueError("Cannot parse data")

    def from_data_point(self, data):
        if isinstance(data, EGPErrorDataPoint):
            self.timestamp = data.timestamp
            self.node_id = data.node_id
            self.error_code = data.error_code
            self.create_id = data.create_id
            self.origin_id = data.origin_id
            self.old_exp_mhp_seq = data.old_exp_mhp_seq
            self.new_exp_mhp_seq = data.new_exp_mhp_seq
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP Error Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    Node ID: {}\n".format(self.node_id)
        to_print += "    Error Code: {}\n".format(self.error_code)
        to_print += "    Create ID: {}\n".format(self.create_id)
        to_print += "    Origin ID: {}\n".format(self.origin_id)
        to_print += "    Old Exp MHP Seq: {}\n".format(self.old_exp_mhp_seq)
        to_print += "    New Exp MHP Seq: {}\n".format(self.new_exp_mhp_seq)
        to_print += "    Success: {}\n".format(self.success)


class EGPCreateSequence(EGPDataSequence):
    """
    Collects CREATE events from an EGP including the CREATE events
    """

    def get_column_names(self):
        # TODO ITEMS AFTER "Timestamp and "Node ID" currently have to be sorted....
        return ["Timestamp", "Node ID", "CQC Request Raw", "Create ID", "Create Time", "Success"]

    def getData(self, time, source=None):
        nodeID, cqc_request_raw, create_id, create_time = source[0].get_create_info()
        return [(nodeID, cqc_request_raw, create_id, create_time), True]


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
            self.atomic = None

    def from_raw_data(self, data):
        try:
            self.timestamp, self.node_id, cqc_request_raw, create_id, create_time, self.success = data
            egp_request = EGP._get_egp_request(cqc_request_raw=cqc_request_raw)
            self.create_id = create_id
            self.create_time = create_time
            self.max_time = egp_request.max_time
            self.measure_directly = egp_request.measure_directly
            self.min_fidelity = egp_request.min_fidelity
            self.num_pairs = egp_request.num_pairs
            self.other_id = egp_request.other_id
            self.priority = egp_request.priority
            self.store = egp_request.store
            self.atomic = egp_request.atomic
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
            self.atomic = data.atomic
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
        to_print += "    Atomic: {}\n".format(self.atomic)
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
        return ["Timestamp", "Node ID", "OK_TYPE", "CQC_OK", "Attempts", "Used Cycles", "Success"]

    def getData(self, time, source=None):
        scenario = source[0]
        ok = scenario.get_ok()
        try:
            create_id, ent_id, _, _, _, _ = MeasureAfterSuccessScenario.unpack_cqc_ok(ok)
            ok_type = EntInfoCreateKeepHeader.type
        # TODO the bitstring.ReadError should occur but this needs to be fixed in SimulaQron
        except ValueError:
            try:
                create_id, ent_id, _, _, _, _ = MeasureBeforeSuccessScenario.unpack_cqc_ok(ok)
                ok_type = EntInfoMeasDirectHeader.type
            except ValueError:
                raise ValueError("Unknown OK type")
        origin_id = ent_id[0]

        node_id = scenario.node.nodeID

        if scenario.egp.dqp.master:
            if origin_id == node_id:
                master_request = True
            else:
                master_request = False
        else:
            if origin_id == node_id:
                master_request = False
            else:
                master_request = True

        # Get number of attempts
        if self._attempt_collectors:
            try:
                attempt_collector = self._attempt_collectors[node_id]
            except KeyError:
                nr_attempts = -1
            else:
                nr_attempts = attempt_collector.get_attempts(create_id, master_request)
        else:
            nr_attempts = -1

        # Get number of used MHP cycles
        used_cycles = scenario.egp._used_MHP_cycles.pop(create_id)
        scenario.egp._current_create_id = None

        data = [node_id, ok_type, ok, nr_attempts, used_cycles]
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
            self.used_cycles = None
            self.success = None

    def from_raw_data(self, data):
        self.timestamp = data[0]
        self.node_id = data[1]
        self.ok_type = data[2]
        ok = data[3]
        self.attempts = data[4]
        self.used_cycles = data[5]
        self.success = data[6]

        if self.ok_type == EntInfoMeasDirectHeader.type:
            (self.create_id, ent_id, self.measurement_outcome, self.measurement_basis, self.goodness,
             self.create_time) = MeasureBeforeSuccessScenario.unpack_cqc_ok(ok)
            self.origin_id, self.other_id, self.mhp_seq = ent_id
        elif self.ok_type == EntInfoCreateKeepHeader.type:
            (self.create_id, ent_id, self.logical_id, self.goodness, self.create_time,
             self.goodness_time) = MeasureAfterSuccessScenario.unpack_cqc_ok(ok)
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
            self.used_cycles = data.used_cycles
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP OK Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    OK Type: {}\n".format("CK" if self.ok_type == EntInfoCreateKeepHeader.type else "MD")
        to_print += "    Node ID: {}\n".format(self.node_id)
        to_print += "    Create ID: {}\n".format(self.create_id)
        to_print += "    Origin ID: {}\n".format(self.origin_id)
        to_print += "    Other ID: {}\n".format(self.other_id)
        to_print += "    MHP Seq: {}\n".format(self.mhp_seq)
        if self.ok_type == EntInfoCreateKeepHeader.type:
            to_print += "    Logical ID: {}\n".format(self.logical_id)
            to_print += "    Goodness: {}\n".format(self.goodness)
            to_print += "    Goodness Time: {}\n".format(self.goodness_time)
        else:
            to_print += "    Measurement Outcome: {}\n".format(self.measurement_outcome)
            to_print += "    Measurement Basis: {}\n".format(self.measurement_basis)
        to_print += "    Create Time: {}\n".format(self.create_time)
        to_print += "    Attempts: {}\n".format(self.attempts)
        to_print += "    Used Cycles: {}\n".format(self.used_cycles)
        to_print += "    Success: {}\n".format(self.success)
        return to_print


class EGPStateSequence(EGPDataSequence):
    """
    Collects qubit states of generated entangled pairs
    """

    def __init__(self, *args, **kwargs):
        super(EGPStateSequence, self).__init__(*args, **kwargs)

        # Keep track of what states have been collected
        self._collected_ent_ids = []

    def get_column_names(self):
        matrix_columns = ["{}, {}, {}".format(i, j, k) for i in range(4) for j in range(4) for k in ['real', 'imag']]

        if self.version <= 0:
            return ["Timestamp", "Outcome 1", "Outcome 2"] + matrix_columns + ["Success"]
        else:
            return (["Timestamp", "originID", "peerID", "MHP Seq", "Outcome 1", "Outcome 2"] +
                    matrix_columns + ["Success"])

    def getData(self, time, source=None):
        scenario = source[0]
        ent_id = next(iter(scenario.entangled_qubits))

        # Wait for both nodes to commit their state
        if ent_id in self._collected_ent_ids:
            self._collected_ent_ids.remove(ent_id)
            r1, q1 = self.evt_source_list[0].entangled_qubits.pop(ent_id)
            r2, q2 = self.evt_source_list[1].entangled_qubits.pop(ent_id)
            qstate = qapi.reduced_dm([q1, q2])

            qapi.discard(q1)
            qapi.discard(q2)

            if self.version <= 0:
                val = [r1, r2] + [n for sl in [[z.real, z.imag] for z in qstate.flat] for n in sl]
            else:
                val = [r1, r2] + list(ent_id) + [n for sl in [[z.real, z.imag] for z in qstate.flat] for n in sl]
            return [val, r1 == r2]
        else:
            self._collected_ent_ids.append(ent_id)
            return [None, True]


class EGPStateDataPoint(EGPDataPoint):
    def __init__(self, data=None):
        if data:
            if isinstance(data, EGPStateDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.timestamp = None
            self.ent_id = None
            self.outcome1 = None
            self.outcome2 = None
            self.density_matrix = None
            self.success = None

    def from_raw_data(self, data):
        try:
            self.timestamp = data[0]
            self.outcome1 = data[1]
            self.outcome2 = data[2]
            if self.version >= 1:
                self.ent_id = tuple(data[3:6])

            # Construct the matrix
            if self.version <= 0:
                m_data = data[3:35]
            else:
                m_data = data[6:38]
            density_matrix = np.matrix(
                [[m_data[i] + 1j * m_data[i + 1] for i in range(k, k + 8, 2)] for k in range(0, len(m_data), 8)])
            self.density_matrix = density_matrix
            if self.version <= 0:
                self.success = data[35]
            else:
                self.success = data[38]
        except IndexError:
            raise ValueError("Cannot parse data")

    def from_data_point(self, data):
        if isinstance(data, EGPStateDataPoint):
            self.timestamp = data.timestamp
            self.ent_id = data.ent_id
            self.outcome1 = data.outcome1
            self.outcome2 = data.outcome2
            self.density_matrix = data.density_matrix
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP State Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    Entanglement Identifier: {}\n".format(self.ent_id)
        to_print += "    Outcome 1: {}\n".format(self.outcome1)
        to_print += "    Outcome 2: {}\n".format(self.outcome2)
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
        if self.version <= 0:
            return ["Timestamp", "Z_err", "X_err", "Y_err", "Success"]
        else:
            return ["Timestamp", "originID", "peerID", "MHP Seq", "Z_err", "X_err", "Y_err", "Success"]

    def getData(self, time, source=None):
        # Get scenarios
        scenarioA = source[0]
        scenarioB = source[1]

        # Get latest measurement data from A
        ent_id, meas_dataA = scenarioA.get_measurement(remove=False)
        if ent_id is None:
            # No data yet
            if self.version <= 0:
                return [[-1, -1, -1], False]
            else:
                return [[-1, -1, -1, -1, -1, -1], False]

        # Check if B also got the measurement data yet
        _, meas_dataB = scenarioB.get_measurement(ent_id=ent_id, remove=True)

        if meas_dataB is None:
            # B hasn't received the corresponding OK yet, try next time
            if self.version <= 0:
                return [[-1, -1, -1], False]
            else:
                return [[-1, -1, -1, -1, -1, -1], False]

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
            if self.version <= 0:
                return [-1, -1, -1], False
            else:
                return list(ent_id) + [-1, -1, -1], False

        error = 1 if bit_choiceA != bit_choiceB else 0
        if basis_choiceA == 0:  # Standard basis
            if self.version <= 0:
                return [error, -1, -1], True
            else:
                return list(ent_id) + [error, -1, -1], True
        elif basis_choiceA == 1:  # Hadamard basis
            if self.version <= 0:
                return [-1, error, -1], True
            else:
                return list(ent_id) + [-1, error, -1], True
        else:
            if self.version <= 0:
                return [-1, -1, error], True
            else:
                return list(ent_id) + [-1, -1, error], True


class EGPQubErrDataPoint(EGPDataPoint):
    def __init__(self, data=None):
        if data:
            if isinstance(data, EGPQubErrDataPoint):
                self.from_data_point(data)
            else:
                self.from_raw_data(data)
        else:
            self.timestamp = None
            self.ent_id = None
            self.z_err = None
            self.x_err = None
            self.y_err = None
            self.success = None

    def from_raw_data(self, data):
        try:
            self.timestamp = data[0]
            if self.version <= 0:
                self.z_err = data[1]
                self.x_err = data[2]
                self.y_err = data[3]
                self.success = data[4]
            else:
                self.ent_id = tuple(data[1:4])
                self.z_err = data[4]
                self.x_err = data[5]
                self.y_err = data[6]
                self.success = data[7]
        except IndexError:
            raise ValueError("Cannot parse data")

    def from_data_point(self, data):
        if isinstance(data, EGPQubErrDataPoint):
            self.timestamp = data.timestamp
            self.ent_id = data.ent_id
            self.z_err = data.z_err
            self.x_err = data.x_err
            self.y_err = data.y_err
            self.success = data.success
        else:
            raise ValueError("'data' is not an instance of this class")

    def printable_data(self):
        to_print = "EGP QubErr Data-point:\n"
        to_print += "    Timestamp: {}\n".format(self.timestamp)
        to_print += "    Entanglement Identifier: {}\n".format(self.ent_id)
        to_print += "    Z Error: {}\n".format(self.z_err)
        to_print += "    X Error: {}\n".format(self.x_err)
        to_print += "    Y Error: {}\n".format(self.y_err)
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
        aid = self._mhp._previous_aid

        # Get the current request, the create ID and the ID of other
        try:
            request = self._egp.scheduler.get_request(aid=aid)
        except Exception as err:
            print("Attempt collector got exception when trying to get request with aid={}".format(aid))
            qids = self._egp.dqp.queueList
            print("Current qids are {}".format(qids))
            for q in qids:
                print(q._queue)
            raise err
        if request:
            create_id = request.create_id
            master_request = request.master_request

            self._register_attempt(create_id, master_request)
        else:
            logger.warning("Entanglement attempt occurred without request")

    def _register_attempt(self, create_id, master_request):
        """
        Register the attempt in the storage
        """
        key = (create_id, master_request)
        if key in self._attempts:
            self._attempts[key] += 1
        else:
            self._attempts[key] = 1

    def get_attempts(self, create_id, master_request, remove=True):
        """
        Returns the number current number of attempts for this create_id and other_id
        :param create_id: int
        :param master_request: bool
        :param remove: bool
        :return: int
        """
        key = (create_id, master_request)
        if remove:
            try:
                return self._attempts.pop(key)
            except KeyError:
                logger.warning(
                    "No attempt info for create ID {} and master request {}".format(create_id, master_request))
                print(self._attempts)
                return None
        else:
            try:
                return self._attempts[key]
            except KeyError:
                logger.warning(
                    "No attempt info for create ID {} and master request {}".format(create_id, master_request))
                print(self._attempts)
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
