import abc
from easysquid.puppetMaster import PM_SQLDataSequence
from easysquid.toolbox import EasySquidException


class EGPDataSequence(PM_SQLDataSequence, metaclass=abc.ABCMeta):
    def __init__(self, name, dbFile, column_names=None, maxSteps=1000):
        super(EGPDataSequence, self).__init__(name=name, dbFile=dbFile, column_names=column_names, maxSteps=maxSteps)

    @abc.abstractmethod
    def get_column_names(self):
        pass

    def sumData(self, val, succ):
        pass


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


class EGPCreateSequence(EGPDataSequence):
    """
    Collects CREATE events from an EGP including the CREATE events
    """

    def get_column_names(self):
        # TODO ITEMS AFTER "Timestamp and "Node ID" currently have to be sorted....
        return ["Timestamp", "Node ID", "Create ID", "Create_Time", "Max Time", "Measure Directly", "Min Fidelity",
                "Num Pairs", "Other ID", "Priority", "Purpose ID", "Store", "Success"]

    def getData(self, time, source=None):
        nodeID, request = source[0].get_create_info()
        create_info = request.get_create_info()
        request_data = [vars(request)[k] for k in sorted(vars(request))]
        val = [nodeID] + request_data
        return [val, create_info != (None, None)]


class EGPOKSequence(EGPDataSequence):
    """
    Collects OK events from an EGP including the CREATE events
    """

    def get_column_names(self):
        return ["Timestamp", "Create ID", "Origin ID", "Other ID", "MHP Seq", "Logical ID", "Goodness", "Goodness Time",
                "Create Time", "Success"]

    def getData(self, time, source=None):
        ok = source[0].get_ok()
        val = [ok[0]] + list(ok[1]) + list(ok[2:])
        return [val, True]


class EGPStateSequence(EGPDataSequence):
    """
    Collects qubit states of generated entangled pairs
    """

    def get_column_names(self):
        matrix_columns = ["{}, {}, {}".format(i, j, k) for i in range(4) for j in range(4) for k in ['real', 'imag']]
        return ["Timestamp", "Node ID"] + matrix_columns + ["Success"]

    def event_handler(self, event=None):
        source = event.source
        if source.get_qstate(remove=False) is not None:
            self.gather(event)
        else:
            source.get_qstate()

    def getData(self, time, source=None):
        state = source[0].get_qstate()
        nodeID = source[0].egp.node.nodeID
        val = [nodeID] + [n for sl in [[z.real, z.imag] for z in state.flat] for n in sl]
        return [val, True]


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

        # Get meas outcomes from midpoint
        (m1, m2) = meas_dataA[2]

        # Check consistency
        if not (m1, m2) == meas_dataB[2]:
            raise EasySquidException("Inconsistent measurement outcomes as nodes. Classical error?")

        # Check if equal basis choices
        if basis_choiceA != basis_choiceB:
            return [-1, -1], False

        # Possible Bell meas outcomes in ideal meas
        ideal_outcomes_standard = {"equal_bits": [(0, 0), (1, 0)],
                                   "unequal_bits": [(0, 1), (1, 1)]}
        ideal_outcomes_hadamard = {"equal_bits": [(0, 0), (0, 1)],
                                   "unequal_bits": [(1, 0), (1, 1)]}

        if basis_choiceA == 0:  # Standard basis
            if bit_choiceA == bit_choiceB:  # Equal bits
                ideal_outcomes = ideal_outcomes_standard["equal_bits"]
            else:
                ideal_outcomes = ideal_outcomes_standard["unequal_bits"]
            if (m1, m2) in ideal_outcomes:  # (no QubErr)
                return [0, -1], True
            else:  # (QubErr)
                return [1, -1], True
        else:  # Hadamard basis
            if bit_choiceA == bit_choiceB:  # Equal bits
                ideal_outcomes = ideal_outcomes_hadamard["equal_bits"]
            else:
                ideal_outcomes = ideal_outcomes_hadamard["unequal_bits"]
            if (m1, m2) in ideal_outcomes:  # (no QubErr)
                return [-1, 0], True
            else:  # (QubErr)
                return [-1, 1], True


class EGPLocalQueueSequence(EGPDataSequence):
    """
    Collects additions and removals of items from a local queue.
    """

    def get_column_names(self):
        return ["Timestamp", "Add or Rem", "Seq", "Success"]

    def getData(self, time, source=None):
        local_queue = source[0]
        if local_queue._last_seq_added is not None:
            data = 1, local_queue._last_seq_added

            # Check for consistency
            if local_queue._last_seq_removed is not None:
                raise RuntimeError("Got both addition and removal in queue collection.")
        else:
            if local_queue._last_seq_removed is not None:
                data = -1, local_queue._last_seq_removed
            else:
                raise RuntimeError("Got no addition or removal in queue collection.")
        local_queue._reset_data()

        return data, True


class MHPNodeEntanglementAttemptSequence(EGPDataSequence):
    """
    Collects entanglement attempts that occur at the end nodes
    """

    def get_column_names(self):
        return ["Timestamp", "Node ID", "Success"]

    def getData(self, time, source=None):
        [event_source] = source
        nodeID = event_source.node.nodeID
        return [nodeID, True]


class MHPMidpointEntanglementAttemptSequence(EGPDataSequence):
    """
    Collects entanglement attempts that occur at the end nodes
    """

    def get_column_names(self):
        return ["Timestamp", "Outcome", "Success"]

    def getData(self, time, source=None):
        [event_source] = source
        outcome = event_source.last_outcome
        success = False
        if outcome in [1, 2]:
            success = True

        return [outcome, success]
