import random

from netsquid.pydynaa import EventType
from qlinklayer.scenario import EGPSimulationScenario, MeasureAfterSuccessScenario, MeasureBeforeSuccessScenario


class MixedScenario(EGPSimulationScenario):
    def __init__(self, egp, request_cycle, t0=0, request_params=None):
        super(EGPSimulationScenario, self).__init__(timeStep=request_cycle, t0=t0, node=egp.node)

        # Request params
        self.request_params = request_params

        # Our EGP
        self.egp = egp

        # Who's our peer
        self.otherID = self._get_other_node_ID()

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

    def run_protocol(self):
        # Note that we always schedule the first event, to see something interesting in the simulations
        rand_var = random.random()
        for

    def _ok_callback(self, result):
        pass

    @staticmethod
    def unpack_cqc_ok(results):
        pass

    def _err_callback(self, result):
        pass