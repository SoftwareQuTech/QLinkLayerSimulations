import abc
from easysquid.puppetMaster import PM_SQLDataSequence


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
        return ["Timestamp", "Node ID", "Create ID", "Create_Time", "Max Time", "Min Fidelity", "Num Pairs",
                "Other ID", "Priority", "Purpose ID", "Success"]

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
