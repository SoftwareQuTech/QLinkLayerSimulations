import abc
from numpy import kron, zeros
from netsquid.qubits import dm_fidelity
from easysquid.toolbox import EasySquidException
from qlinklayer.toolbox import LinkLayerException
from netsquid.qubits.ketstates import s00, s01, s10, s11, b01, b11

# Density matrices
dm00 = kron(s00.H, s00)  # |00><00|
dm01 = kron(s01.H, s01)  # |01><01|
dm10 = kron(s10.H, s10)  # |10><10|
dm11 = kron(s11.H, s11)  # |11><11|
dm_psi_plus = kron(b01.H, b01)
dm_psi_minus = kron(b11.H, b11)


class FidelityEstimationUnit(metaclass=abc.ABCMeta):
    def __init__(self):
        """
        Fidelity estimation unit code stub, estimates the fidelity of generated entangled pairs
        using parameters from the generation protocol.
        """

    @abc.abstractmethod
    def _estimate_fidelity(self):
        """
        Abstract method that implements the fidelity estimation.  To be overridden by subclasses.
        """
        pass


class SingleClickFidelityEstimationUnit(FidelityEstimationUnit):
    def __init__(self, node, mhp_service):
        """
        Fidelity Estimation Unit that estimates fidelity of generated states given parameters used in the Single Click
        heralding protocol.
        :param node: obj `~easysquid.qnode.QuantumNode`
            The node containing the quantumprocessingdevice that describes photon emission parameters
        :param mhp_conn: obj `~MHPHeraldedConnection`
            The connection containing fibre lengths and noise models of photon transmission as well as
            midpoint parameters
        :param mhp_service: `~qlinklayer.mhp.MHPService`
            MHP Service that tracks the node protocols and MHP parameters used at the endnodes like the bright
            state population
        """
        self.node = node
        self.mhp_service = mhp_service
        self.estimated_fidelity = self._estimate_fidelity()
        self.achievable_fidelities = self._calculate_achievable_fidelities()

    def update_components(self, node=None, mhp_service=None):
        """
        Updates the components used for the fidelity estimated
        :param node: obj `~easysquid.qnode.QuantumNode`
            The node containing the quantumprocessingdevice that describes photon emission parameters
        :param mhp_service: `~qlinklayer.mhp.MHPService`
            MHP Service that tracks the node protocols and MHP parameters used at the endnodes like the bright
            state population
        """
        if node:
            self.node = node
        if mhp_service:
            self.mhp_service = mhp_service

    def _calculate_achievable_fidelities(self):
        """
        Computes the estimated fidelity for all bright state populations in underlying MHPs
        :return: list of tuples
            A list of (alpha, estimated_fidelity)
        """
        # Return None if we have no resources
        if not self.node or not self.mhp_service:
            return []

        # Get bright state populations from nodes
        bright_statesA, bright_statesB = self.mhp_service.get_allowed_bright_state_populations(self.node)
        bright_statesA = sorted(bright_statesA)
        bright_statesB = sorted(bright_statesB)

        # Currently we do not handle the case where different configurations used
        if bright_statesA != bright_statesB:
            raise LinkLayerException("A and B have different bright state sets!  Fidelity calculation unsupported")

        achievable = []
        params = self._extract_params()
        for alphaA, alphaB in zip(bright_statesA, bright_statesB):
            params = (params[0], params[1], alphaA, alphaB, params[4], params[5])
            ideal_state = kron(b01.H, b01)  # (|01> + |10>)(<01| + <10|)
            estimated_state = self._calculate_estimated_state(*params)
            fidelity = float(dm_fidelity(estimated_state, ideal_state, squared=True))
            achievable.append((alphaA, fidelity))

        return achievable

    def get_max_fidelity(self):
        """
        Returns the maximum estimated fidelity based on all MHP configurations
        :return: float
            The best possible estimated fidelity
        """
        if not self.achievable_fidelities:
            self.achievable_fidelities = self._calculate_achievable_fidelities()

        return max([f[1] for f in self.achievable_fidelities])

    def select_bright_state(self, min_fidelity):
        """
        Given a minimum desired fidelity returns the largest bright state population that achieves this fidelity
        :param min_fidelity: float
            The minimum fidelity to be achieved
        :return: float
            The bright state population that achieves this
        """
        for bright_state, fidelity in reversed(self.achievable_fidelities):
            if fidelity >= min_fidelity:
                return bright_state

        return None

    def recalculate_estimate(self):
        """
        Recalculates and updates the stored fidelity estimate
        :return: float
            The recalculated fidelity estimate
        """
        self.estimated_fidelity = self._estimate_fidelity()
        return self.estimated_fidelity

    def _estimate_fidelity(self):
        """
        Calculates the fidelity by extracting parameters from the mhp components, calculating an estimated state of
        the entangled qubit, and computing the fidelity with the ideal state.
        :return: float
            Estimated fidelity based on the component parameters
        """
        params = self._extract_params()
        ideal_state = kron(b01.H, b01)  # (|01> + |10>)(<01| + <10|)
        estimated_state = self._calculate_estimated_state(*params)
        return float(dm_fidelity(estimated_state, ideal_state, squared=True))

    def _compute_total_detection_probabilities(self):
        """
        Computes the total detection probabilities of the two nodes, i.e. given that the the electron is excited,
        what is the probability that a detector clicks at the midpoint.
        :return: tuple (p_det_A, p_det_B)
        """
        mhp_conn = self.mhp_service.conn
        total_det_effA = self._compute_total_detection_probability_of_node(mhp_conn.nodeA)
        total_det_effB = self._compute_total_detection_probability_of_node(mhp_conn.nodeB)
        return total_det_effA, total_det_effB

    def _compute_total_detection_probability_of_node(self, node):
        """
        Computes the total detection probabilities of one node, i.e. given that the the electron is excited,
        what is the probability that a detector clicks at the midpoint. (assuming no dark counts)
        :return: float
        """
        p_zero_phonon = node.qmem.photon_emission_noise.p_zero_phonon
        collection_eff = node.qmem.photon_emission_noise.collection_eff
        if node == self.mhp_service.conn.nodeA:
            p_loss = self.mhp_service.get_fibre_transmissivities(node)[0]
        else:
            p_loss = self.mhp_service.get_fibre_transmissivities(node)[1]
        detection_eff = self.mhp_service.conn.midpoint.detection_eff

        total_detection_eff = p_zero_phonon * collection_eff * (1 - p_loss) * detection_eff

        return total_detection_eff

    def _compute_conditional_detection_probabilities(self, alpha):
        """
        Computes the probabilites p_uu, p_ud, pdu, pdd as given in eq (10) in https://arxiv.org/src/1712.07567v2/anc/SupplementaryInformation.pdf
        :return:
        """

        p_det_A, p_det_B = self._compute_total_detection_probabilities()
        p_dc = self.mhp_service.conn.midpoint.pdark

        # TODO I don't think this is correct
        p_uu = alpha**2 * ((1 - p_dc)**2 * (p_det_A + p_det_B) / 2 + 2 * (1 - p_dc) * p_dc * (1-p_det_A) * (1-p_det_B))
        p_ud = alpha * (1-alpha)((1-p_dc)**2 * p_det_A)

        # TODO NOT FINISHED!!!

    def _extract_params(self):
        """
        Extracts parameters for fidelity estimation from the provided hardware such as midpoint detectors, fibre
        properties, node state preparation, etc.
        :return: Parameters to use for fidelity estimation
        """
        # Get the transmissivity info from the service
        etaA, etaB = self.mhp_service.get_fibre_transmissivities(self.node)

        # Get bright state info from the service (states are prepared in sqrt(1-alpha)|0> + sqrt(alpha)|1> so the
        # probability of emission is simply alpha for both end nodes
        alphaA, alphaB = self.mhp_service.get_bright_state_populations(self.node)

        # Get the probability of a dark count at the midpoint
        pdark = self.mhp_service.calculate_dark_count_probability(self.node)

        # Get the dephasing photon parameter from the quantum memory device at our node
        dp_photon = (1 - getattr(self.node.qmem.photon_emission_noise, '_dp_photon', 0))

        return etaA, etaB, alphaA, alphaB, pdark, dp_photon

    @staticmethod
    def _calculate_estimated_state(etaA, etaB, alphaA, alphaB, pdark, dp_photon):
        """
        Calculates an estimated state based on the provided parameters.  Adapted from "Near-term repeater experiments
        with NV centers: overcoming the limitations of direct transmission"
        :param etaA: float
            Transmitivity of fibre from A to midpoint
        :param etaB: float
            Transmitivity of fibre from B to midpoint
        :param alphaA: float
            Bright state population of generated state at node A
        :param alphaB: float
            Bright state population of generated state at node B
        :param pdark: float
            Probability of a dark count occurrence at the midpoint
        :param dp_photon: float
            Dephasing parameter of photon
        :return: obj `~numpy.matrix`
            A density matrix of the estimated entangled state
        """
        if pdark == 1:
            raise EasySquidException("Probability of dark count 1 does not allow for generating entanglement!")

        # Calculate the probability that the left or right detector click depending on the probability of emission
        # Probability of click for photon from A
        p0 = alphaA * (1 - alphaB) * etaA

        # Probability of click for photon from B
        p0 += (1 - alphaA) * alphaB * etaB

        # Probility of click for photons from both
        p0 += alphaA * alphaB * (1 - (1 - etaA) * (1 - etaB))
        p0 *= 0.5

        # Due to symmetries the probability is the same for the other detector
        p1 = p0

        # Calculate the probability that neither detector clicks
        p2 = (1 - etaA * alphaA) * (1 - etaB * alphaB)

        if p2 == 1:
            raise EasySquidException("Probability of photons reaching detectors is zero!")

        # Calculate the probability of dark counts occuring within the detection window
        # Time window in nanoseconds, dark rate in Hz

        # Calculate the probability of entanglement (only one photon causes the detector to click)
        pent = alphaA * (1 - alphaB) * etaA +\
            (1 - alphaA) * alphaB * etaB

        # Calculate the post-measurement state in the case that a detector clicks due to a photon from A or B
        a = dp_photon
        b = 1 - dp_photon
        rho0 = (pent / (p0 + p1)) * (a * dm_psi_plus + b * dm_psi_minus) + (1 - pent / (p0 + p1)) * dm11

        # Calculate the post-measurement state in the case that a dark count causes the detector to click
        if p2 != 0:
            # Probability that neither endpoint emits a photon
            rho2 = (1 - alphaA) * (1 - alphaB) * dm00

            # Probability that left endpoint emits a photon and it gets lost
            rho2 += (alphaA * (1 - alphaB) * (1 - etaA)) * dm10

            # Probability that right endpoint emits a photon and it gets lost
            rho2 += ((1 - alphaA) * alphaB * (1 - etaB)) * dm01

            # Probability that both endpoints emit a photon and both are lost
            rho2 += (1 - etaA) * (1 - etaB) * alphaA * alphaB * dm11
            rho2 *= (1 / p2)

        # Probability of no clicks is zero then there is no influence
        else:
            rho2 = zeros((4, 4))

        # Calculate the probability of registering a click in only one detector
        Y = (p0 + p1) * (1 - pdark) + 2 * p2 * pdark * (1 - pdark)

        # Calculate the effective accepted state if a click in one detector occurs
        estimated_rho = (1 / Y) * ((p0 + p1) * (1 - pdark) * rho0 + 2 * p2 * pdark * (1 - pdark) * rho2)

        return estimated_rho

    def optimize_bright_state_population(self, p_success, min_fidelity):
        """
        Code stub for optimizing the emission angles at the endnodes given a minimum success probability and fidelity
        :param p_success: float
            Value from 0 to 1 specifying the desired minimum probability of success
        :param min_fidelity: float
            Value from 0 to 1 specifying the desired minimum fidelity
        :return: dict
            Mapping of nodeID -> bright state population to use for state preparation
        """
        # Verify the input parameters are valid
        if p_success < 0 or p_success > 1:
            raise Exception("Desired probability of success {} must be between 0 and 1".format(p_success))

        elif min_fidelity < 0 or min_fidelity > 1:
            raise Exception("Desired minimum fidelity {} must be between 0 and 1".format(min_fidelity))

        # TODO: Implement optimization, for now return default
        alphaA = 0.5
        alphaB = 0.5

        conn = self.mhp_service.get_node_proto(self.node).conn
        # Since emission angle depends on node parameters we should store it for retrieval by the correct node
        bright_state_populations = {
            conn.nodeA.nodeID: alphaA,
            conn.nodeB.nodeID: alphaB
        }

        return bright_state_populations

    def estimate_time_to_process(self, scheduler_request, units="seconds"):
        """
        Computes the estimated time to process a request (assuming that this is the only request in the queues
        and that its ready)

        The 'units' argument can be used to choose whether what is returned is in seconds or nr of mhp cycles
        :param scheduler_request: :obj:`~qlinklayer.scheduler.SchedulerRequest`
        :param units: str (either "seconds" or "mhp_cycles")
        :return: float or int
        """
        if not units in ["seconds", "mhp_cycles"]:
            raise ValueError("'units' need to be 'seconds' or 'mhp_cycles'")
        # TODO implement this
        return 1
