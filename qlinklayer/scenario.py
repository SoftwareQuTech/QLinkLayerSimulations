import abc
from functools import partial
from easysquid.toolbox import create_logger, SimulationScheduler
from netsquid.pydynaa import DynAASim, Entity, EventType


logger = create_logger("logger")


class SimulationScenario(Entity, metaclass=abc.ABCMeta):
    def __init__(self):
        self.sim_scheduler = SimulationScheduler()

    def _schedule_action(self, func, sim_time):
        self.sim_scheduler.schedule_function(func=func, t=sim_time)


class EGPSimulationScenario(SimulationScenario):
    def __init__(self, egp):
        super(EGPSimulationScenario, self).__init__()
        self.egp = egp
        self.egp.ok_callback = self.ok_callback
        self._EVT_OK = EventType("EGP OK", "Triggers when egp has issued an ok message")
        self.egp.err_callback = self.err_callback
        self._EVT_ERR = EventType("EGP ERR", "Triggers when egp has issued an err message")

    def schedule_create(self, request, t):
        func_create = partial(self.egp.create, creq=request)
        self._schedule_action(func_create, sim_time=t)

    def ok_callback(self, result):
        self._ok_callback(result)
        self._schedule_now(self._EVT_OK)

    @abc.abstractmethod
    def _ok_callback(self, result):
        pass

    def err_callback(self, result):
        self._err_callback(result)
        self._schedule_now(self._EVT_ERR)

    @abc.abstractmethod
    def _err_callback(self, result):
        pass


class MeasureImmediatelyScenario(EGPSimulationScenario):
    def __init__(self, egp):
        super(MeasureImmediatelyScenario, self).__init__(egp=egp)
        self.egp = egp
        self.node = egp.node
        self.qmm = egp.qmm

        self.ok_storage = []
        self.measurement_results = []
        self.err_storage = []

    def _ok_callback(self, result):
        self.ok_storage.append(result)
        create_id, ent_id, f_goodness, t_create, t_goodness = result
        creator_id, peer_id, mhp_seq, logical_id = ent_id
        [outcome] = self.node.qmem.measure_subset([logical_id])
        now = DynAASim().current_time
        logger.info("{} measured {} for ent_id {} at time {}".format(self.node.nodeID, outcome, ent_id, now))
        self.measurement_results.append((mhp_seq, outcome))
        self.qmm.free_qubit(logical_id)

    def get_ok(self, remove=True):
        ok = self.ok_storage.pop(0) if remove else self.ok_storage[0]
        return ok

    def get_measurement(self, remove=True):
        measurement = self.measurement_results.pop(0) if remove else self.measurement_results[0]
        return measurement

    def _err_callback(self, result):
        now = DynAASim().current_time
        logger.error("{} got error {} at time {}".format(self.node.nodeID, result, now))
        self.err_storage.append(result)

    def get_error(self, remove=True):
        err = self.err_storage.pop(0) if remove else self.err_storage[0]
        return err
