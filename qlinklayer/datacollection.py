from easysquid.puppetMaster import PM_DataSequence, PM_MultiDataSequence
from qlinklayer.mhp import MHPHeraldedConnection, MHPServiceProtocol
from netsquid.simutil import sim_time


class EGPErrorSequence(PM_MultiDataSequence):
    """
    Collects error events and their error codes thrown by an EGP
    """
    def getData(self, time, sourceList):
        error_source = sourceList[0]
        error_code, error_info = error_source.get_error()
        return [(error_source.egp.node.nodeID, error_code), True]

    def sumData(self, val, succ):
        pass


class EGPOKSequence(PM_MultiDataSequence):
    """
    Collects OK events from an EGP including the CREATE events
    """
    def gather(self, event):
        now = sim_time()
        if event.type == event.source._EVT_CREATE:
            [val, succ] = self.getCreateData(time=now, source=event.source)

        elif event.type == event.source._EVT_OK:
            [val, succ] = self.getOKData(time=now, source=event.source)

        else:
            raise Exception

        self._commonGather(now, val, succ)

    def getCreateData(self, time, source=None):
        nodeID, request = source.get_create_info()
        create_info = request.get_create_info()
        return [(nodeID, vars(request)), create_info != (None, None)]

    def getOKData(self, time, source=None):
        ok = source.get_ok()
        return [ok, True]

    def sumData(self, val, succ):
        pass


class MHPEntanglementAttemptSequence(PM_DataSequence):
    """
    Collects entanglement attempts that occur at the midpoint
    """
    def getData(self, time, source=None):
        [event_source] = source
        if isinstance(event_source, MHPHeraldedConnection):
            outcome = event_source.last_outcome
            success = False
            if outcome in [1, 2]:
                success = True

            return [outcome, success]

        elif isinstance(event_source, MHPServiceProtocol):
            nodeID = event_source.node.nodeID
            return [nodeID, True]
