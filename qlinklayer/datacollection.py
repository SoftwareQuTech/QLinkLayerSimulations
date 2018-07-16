from easysquid.puppetMaster import PM_DataSequence, PM_MultiDataSequence
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
        create_info = source.get_create_info()
        return [create_info, True]

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
        [midpoint] = source
        outcome = midpoint.last_outcome
        success = False
        if outcome in [1, 2]:
            success = True

        return [outcome, success]
