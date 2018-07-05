from easysquid.puppetMaster import PM_MultiDataSequence
from netsquid.pydynaa import DynAASim


class EGPErrorSequence(PM_MultiDataSequence):
    def getData(self, time, sourceList):
        error_source = sourceList[0]
        error_code, error_info = error_source.get_error()
        return [(error_source.egp.node.nodeID, error_code), True]

    def sumData(self, val, succ):
        pass


class EGPOKSequence(PM_MultiDataSequence):
    def gather(self, event):
        now = DynAASim().current_time
        if event.type == event.source._EVT_CREATE:
            [val, succ] = self.getCreateData(time=now, source=event.source)

        elif event.type == event.source._EVT_OK:
            [val, succ] = self.getOKData(time=now, source=event.source)

        else:
            print("???????")
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
