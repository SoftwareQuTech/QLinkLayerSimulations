import abc
from numpy import kron, zeros
from netsquid.qubits import dm_fidelity
from easysquid.toolbox import EasySquidException
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
        return dm_fidelity(estimated_state, ideal_state, squared=True)

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
        # Calculate the probability that the left or right detector click depending on the probability of emission
        # Probability of click for photon from A
        # Probability of click for photon from B
        # Probility of click for photons from both
        if pdark == 1:
            raise EasySquidException("Probability of dark count 1 does not allow for generating entanglement!")

        p0 = 0.5 * (alphaA * (1 - alphaB) * etaA +
                    (1 - alphaA) * alphaB * etaB +
                    alphaA * alphaB * (1 - (1 - etaA) * (1 - etaB)))

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
            rho2 = (1 / p2) * (
                # Probability that neither endpoint emits a photon
                (1 - alphaA) * (1 - alphaB) * dm00 +
                # Probability that left endpoint emits a photon and it gets lost
                (alphaA * (1 - alphaB) * (1 - etaA)) * dm10 +
                # Probability that right endpoint emits a photon and it gets lost
                ((1 - alphaA) * alphaB * (1 - etaB)) * dm01 +
                # Probability that both endpoints emit a photon and both are lost
                (1 - etaA) * (1 - etaB) * alphaA * alphaB * dm11)

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
