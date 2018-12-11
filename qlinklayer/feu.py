import abc
from numpy import kron, sqrt
from netsquid.qubits import dm_fidelity
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
        for alphaA, alphaB in zip(bright_statesA, bright_statesB):
            ideal_state = kron(b01.H, b01)  # (|01> + |10>)(<01| + <10|)
            estimated_state = self._calculate_estimated_state(alphaA, alphaB)
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

    def select_bright_state(self, min_fidelity, return_fidelity=False):
        """
        Given a minimum desired fidelity returns the largest bright state population that achieves this fidelity
        :param min_fidelity: float
            The minimum fidelity to be achieved
        :return: float
            The bright state population that achieves this
        """
        for bright_state, fidelity in reversed(self.achievable_fidelities):
            if fidelity >= min_fidelity:
                if return_fidelity:
                    return bright_state, fidelity
                else:
                    return bright_state

        return None

    def estimate_fidelity_of_request(self, request):
        """
        Estimates the fidelity of a given request
        The request object must have an attribute 'min_fidelity'.
        :param request:
        :return: float
        """
        _, fidelity = self.select_bright_state(request.min_fidelity, return_fidelity=True)
        return fidelity

    def _estimate_fidelity(self, alphaA=None, alphaB=None):
        """
        Calculates the fidelity by extracting parameters from the mhp components, calculating an estimated state of
        the entangled qubit, and computing the fidelity with the ideal state.
        :return: float
            Estimated fidelity based on the component parameters
        """
        ideal_state = kron(b01.H, b01)  # (|01> + |10>)(<01| + <10|)
        estimated_state = self._calculate_estimated_state(alphaA, alphaB)
        return float(dm_fidelity(estimated_state, ideal_state, squared=True))

    def _compute_total_detection_probabilities(self):
        """
        Computes the total detection probabilities of the two nodes, i.e. given that the the electron is excited,
        what is the probability that a detector clicks at the midpoint.
        :return: tuple (p_det_A, p_det_B)
        """
        mhp_conn = self.mhp_service.get_mhp_conn(self.node)
        total_det_effA = self._compute_total_detection_probability_of_node(mhp_conn.nodeA)
        total_det_effB = self._compute_total_detection_probability_of_node(mhp_conn.nodeB)
        return total_det_effA, total_det_effB

    def _compute_total_detection_probability_of_node(self, node):
        """
        Computes the total detection probabilities of one node, i.e. given that the the electron is excited,
        what is the probability that a detector clicks at the midpoint. (assuming no dark counts)
        :return: float
        """
        photon_emission_noise = node.qmem.photon_emission_noise
        if photon_emission_noise is None:
            p_zero_phonon = 1
            collection_eff = 1
        else:
            p_zero_phonon = node.qmem.photon_emission_noise.p_zero_phonon
            collection_eff = node.qmem.photon_emission_noise.collection_eff
        mhp_conn = self.mhp_service.get_mhp_conn(node)
        if node == mhp_conn.nodeA:
            p_no_loss = self.mhp_service.get_fibre_transmissivities(node)[0]
        else:
            p_no_loss = self.mhp_service.get_fibre_transmissivities(node)[1]
        detection_eff = mhp_conn.midPoint.detection_eff

        total_detection_eff = p_zero_phonon * collection_eff * p_no_loss * detection_eff

        return total_detection_eff

    def _compute_conditional_detection_probabilities(self, alphaA=None, alphaB=None):
        """
        Computes the probabilites p_uu, p_ud, pdu, pdd as given in eq (10) in
        https://arxiv.org/src/1712.07567v2/anc/SupplementaryInformation.pdf
        Note that this assumed low detection efficiencies
        If either alphaA or alphaB is None, then alpha is assumed to be the same for the two nodes.

        :return: tuple of floats
            (p_uu, p_ud, pdu, pdd)
        """
        if alphaA is None and alphaB is None:
            raise LinkLayerException("alphaA or alphaB needs to be specified")
        if alphaA is None:
            alphaA = alphaB
        if alphaB is None:
            alphaB = alphaA

        p_det_A, p_det_B = self._compute_total_detection_probabilities()
        mhp_conn = self.mhp_service.get_mhp_conn(self.node)
        p_dc = mhp_conn.midPoint.pdark
        # Dark count probabilities for both detectors
        p_no_dc = (1 - p_dc) ** 2
        p_at_least_one_dc = 1 - p_no_dc

        prob_click_given_two_photons = (
            p_no_dc * (p_det_A * (1 - p_det_B) + p_det_B * (1 - p_det_A) + p_det_A * p_det_B)
            + p_at_least_one_dc * (1 - p_det_A) * (1 - p_det_B))
        p_uu = alphaA * alphaB * prob_click_given_two_photons

        prob_click_given_photon_A = p_no_dc * p_det_A + p_at_least_one_dc * (1 - p_det_A)
        prob_click_given_photon_B = p_no_dc * p_det_B + p_at_least_one_dc * (1 - p_det_B)
        p_ud = alphaA * (1 - alphaB) * prob_click_given_photon_A
        p_du = alphaB * (1 - alphaA) * prob_click_given_photon_B

        prob_click_given_no_photons = p_at_least_one_dc
        p_dd = (1 - alphaA) * (1 - alphaB) * prob_click_given_no_photons

        return p_uu, p_ud, p_du, p_dd

    def _estimate_success_probability(self, alphaA=None, alphaB=None):
        """
        Computes the success probability as the sum of p_uu, p_ud, p_du, p_dd as defined in eq (10) in
        https://arxiv.org/src/1712.07567v2/anc/SupplementaryInformation.pdf
        If either alphaA or alphaB is None, then alpha is assumed to be the same for the two nodes.
        :return: float
            The success probability
        """
        return sum(self._compute_conditional_detection_probabilities(alphaA, alphaB))

    def _calculate_estimated_state(self, alphaA=None, alphaB=None):
        """
        Calculates an estimated state based on the provided parameters.  See eq (8) in
        https://arxiv.org/src/1712.07567v2/anc/SupplementaryInformation.pdf

        :return: obj `~numpy.matrix`
            A density matrix of the estimated entangled state
        """
        p_uu, p_ud, p_du, p_dd = self._compute_conditional_detection_probabilities(alphaA, alphaB)
        if (p_uu + p_ud + p_du + p_dd) == 0:
            raise LinkLayerException("Cannot estimate state when success probability is zero")

        mhp_conn = self.mhp_service.get_mhp_conn(self.node)
        visibility = mhp_conn.midPoint.visibility

        ent_state = (p_ud * dm10 +  # |10><10| part
                     p_du * dm01 +  # |01><01| part
                     sqrt(visibility * p_ud * p_du) * kron(s10.H, s01) +  # |10><01| part
                     sqrt(visibility * p_ud * p_du) * kron(s01.H, s10))  # |01><10| part

        unnormalised_state = ent_state + p_uu * dm11 + p_dd * dm00

        # Normalise the state
        estimated_rho = unnormalised_state / (p_uu + p_ud + p_du + p_dd)

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

    def estimate_nr_of_attempts(self, scheduler_request):
        """
        Computes the estimated number of entanglement generation attempts needed generate ONE pair in the given request
        (assuming that this is the only request in the queues and that its ready)

        :param scheduler_request: :obj:`~qlinklayer.scheduler.SchedulerRequest`
        :return: float
        """
        alpha = self.select_bright_state(scheduler_request.min_fidelity)
        # Get success probability given alpha
        p_succ = self._estimate_success_probability(alpha)

        # Estimate number of attempts needed
        est_nr_attempts = 1 / p_succ

        return est_nr_attempts
