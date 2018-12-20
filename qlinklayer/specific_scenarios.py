import random

from netsquid.pydynaa import EventType
from netsquid.simutil import sim_time
from easysquid.toolbox import logger
from qlinklayer.scenario import EGPSimulationScenario
from SimulaQron.cqc.backend.entInfoHeader import EntInfoCreateKeepHeader, EntInfoMeasDirectHeader


class MixedScenario(EGPSimulationScenario):
    def __init__(self, egp, request_cycle, request_params, t0=0):

        assert(request_cycle > 0)

        super(EGPSimulationScenario, self).__init__(timeStep=request_cycle, t0=t0, node=egp.node)

        # Request params
        self.request_params = request_params
        self.scenario_names = list(self.request_params.keys())
        self.scenario_probs = [self.request_params[name]["prob"] for name in self.scenario_names]
        assert(sum(self.scenario_probs) <= 1)
        self.scenario_params = {name: self.request_params[name]["params"] for name in self.scenario_names}
        self.scenario_num_requests = {}
        for name in self.scenario_names:
            try:
                self.scenario_num_requests[name] = self.request_params[name]["num_requests"]
            except KeyError:
                self.scenario_num_requests[name] = float('inf')

        # Our EGP
        self.egp = egp

        # EGP internal objects
        self.qmm = egp.qmm

        # Who's our peer
        self.otherID = self._get_other_node_ID()

        # Store the current number of created requests
        self.created_requests = {name: 0 for name in self.scenario_names}

        # Hook up a handler to the ok events
        self.egp.ok_callback = self.ok_callback
        self._EVT_OK = EventType("EGP OK", "Triggers when egp has issued an ok message")

        # Hook up a handler to the error events
        self.egp.err_callback = self.err_callback
        self._EVT_ERR = EventType("EGP ERR", "Triggers when egp has issued an err message")

        # Hook up a handler to the create events
        self.create_storage = []
        self._EVT_CREATE = EventType("EGP CREATE", "Triggers when create was called")

        # For data collection
        self.ok_storage = []
        self.entangled_qstates = {}
        self.node_measurement_storage = {}
        self.err_storage = []

    def run_protocol(self):
        rand_var = random.random()
        for i in range(len(self.scenario_probs)):
            offset = sum(self.scenario_probs[:i])
            if offset <= rand_var < (offset + self.scenario_probs[i]):
                scenario = self.scenario_names[i]
                break
        else:
            scenario = None

        # Note that we always schedule the first event, to see something interesting in the simulations
        if sum(self.created_requests.values()) == 0:
            if scenario is None:
                rand_var = random.random()
                prob_scenario = sum(self.scenario_probs)
                for i in range(len(self.scenario_probs) - 1):
                    offset = sum(self.scenario_probs[:i]) * prob_scenario
                    if offset <= rand_var < (offset + self.scenario_probs[i] * prob_scenario):
                        scenario = self.scenario_names[i]
                        break
                else:
                    scenario = self.scenario_names[-1]

        if scenario is not None:
            if self.created_requests[scenario] >= self.scenario_num_requests[scenario]:
                return

            # Number of pairs
            params = self.scenario_params[scenario]
            if isinstance(params["num_pairs"], list):
                min_pairs, max_pairs = params["num_pairs"][0], params["num_pairs"][1]
                num_pairs = random.randint(min_pairs, max_pairs)
            else:
                num_pairs = params["num_pairs"]

            # Max time for request
            max_time = num_pairs * params["tmax_pair"]

            min_fidelity = params["min_fidelity"]
            purpose_id = params["purpose_id"]
            priority = params["priority"]
            store = params["store"]
            atomic = params["atomic"]
            measure_directly = params["measure_directly"]

            cqc_request_raw = self.construct_cqc_epr_request(otherID=self.otherID, num_pairs=num_pairs,
                                                             max_time=max_time, min_fidelity=min_fidelity,
                                                             purpose_id=purpose_id, priority=priority, store=store,
                                                             atomic=atomic, measure_directly=measure_directly)

            # Give the request to the egp
            self._create(cqc_request_raw=cqc_request_raw)

            self.created_requests[scenario] += 1

    def _ok_callback(self, result):
        """
        Internal handler for collecting oks issued by the EGP.  Will extract the result information from the EGP,
        If create and keep measure the qubit in the memory and release it making it available to the EGP again.
        :param result: bytes
            CQC OK information returned by the EGP
        """
        # Store data for retrieval
        self.ok_storage.append(result)

        # Unpack result
        cqc_header, cqc_xtra_qubit_header, cqc_ent_info_header = self.unpack_cqc_ok(result)
        create_id = cqc_ent_info_header.create_id
        creator_id = cqc_ent_info_header.ip_A
        peer_id = cqc_ent_info_header.ip_B
        mhp_seq = cqc_ent_info_header.mhp_seq
        ent_id = (creator_id, peer_id, mhp_seq)

        if isinstance(cqc_ent_info_header, EntInfoCreateKeepHeader):
            logical_id = cqc_xtra_qubit_header.qubit_id

            # Store the qubit state for collection
            self.store_qstate(logical_id, create_id, peer_id, mhp_seq)

            # Measure the logical qubit in the result
            [outcome], _ = self.node.qmem.measure([logical_id])
            # outcome = self.node.qmem.measure([logical_id])
            # print(outcome)

            meas_data = (0, outcome)

            now = sim_time()
            logger.info("{} measured {} for ent_id {} at time {}".format(self.node.nodeID, outcome, ent_id, now))

            # Free the qubit for the EGP
            self.qmm.free_qubit(logical_id)

        elif isinstance(cqc_ent_info_header, EntInfoMeasDirectHeader):
            basis = cqc_ent_info_header.basis
            outcome = cqc_ent_info_header.meas_out

            # Store the basis/bit choice and the midpoint outcomes for QubErr or key generation
            meas_data = (basis, outcome)

        else:
            raise ValueError("Unknown ent info header class")

        # Store the measurement result for data collection
        self.node_measurement_storage[ent_id] = meas_data
