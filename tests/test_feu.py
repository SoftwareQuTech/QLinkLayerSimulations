import unittest
from qlinklayer.feu import SingleClickFidelityEstimationUnit
from qlinklayer.mhp import SimulatedNodeCentricMHPService
from qlinklayer.toolbox import LinkLayerException
from easysquid.easynetwork import setup_physical_network
from util.config_paths import ConfigPathStorage
from netsquid.qubits import dm_fidelity
from netsquid.qubits.ketstates import b00, b01, b10, b11
from numpy import kron, isclose


class TestSingleClickFidelityEstimationUnit(unittest.TestCase):
    def setUp(self):
        # Setup MHP network and MHP service
        network = setup_physical_network(ConfigPathStorage.NETWORK_NV_LAB_NOCAV_NOCONV)
        alice = network.get_node_by_id(0)
        bob = network.get_node_by_id(1)
        self.mhp_conn = network.get_connection(alice, bob, "mhp_conn")
        mhp_service = SimulatedNodeCentricMHPService("mhp_service", alice, bob, conn=self.mhp_conn)
        self.feuA = SingleClickFidelityEstimationUnit(alice, mhp_service)
        self.feuB = SingleClickFidelityEstimationUnit(alice, mhp_service)

    def test_dm_fidelity(self):
        # Basic test cases using bell states
        dm00 = kron(b00.H, b00)
        dm01 = kron(b01.H, b01)
        dm10 = kron(b10.H, b10)
        dm11 = kron(b11.H, b11)

        self.assertEqual(round(dm_fidelity(dm00, dm00, squared=True), ndigits=8), 1)
        self.assertEqual(round(dm_fidelity(dm00, dm01, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm00, dm10, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm00, dm11, squared=True), ndigits=8), 0)

        self.assertEqual(round(dm_fidelity(dm01, dm00, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm01, dm01, squared=True), ndigits=8), 1)
        self.assertEqual(round(dm_fidelity(dm01, dm10, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm01, dm11, squared=True), ndigits=8), 0)

        self.assertEqual(round(dm_fidelity(dm10, dm00, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm10, dm01, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm10, dm10, squared=True), ndigits=8), 1)
        self.assertEqual(round(dm_fidelity(dm10, dm11, squared=True), ndigits=8), 0)

        self.assertEqual(round(dm_fidelity(dm11, dm00, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm11, dm01, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm11, dm10, squared=True), ndigits=8), 0)
        self.assertEqual(round(dm_fidelity(dm11, dm11, squared=True), ndigits=8), 1)

    def test_success_prob_est(self):

        with self.assertRaises(LinkLayerException):
            self.feuA._estimate_success_probability()

        # Should evaluate to total detection probability
        p_succ = self.feuA._estimate_success_probability(1, 0)
        self.assertAlmostEqual(p_succ, 4e-4, places=4)

        # Test zero alpha
        p_succ = self.feuA._estimate_success_probability(0)
        self.assertAlmostEqual(p_succ, 0, places=4)

        # Test linearity in alpha
        for i in range(11):
            alpha = i / 20
            p_succ = self.feuA._estimate_success_probability(alpha)
            self.assertAlmostEqual(p_succ, 2 * alpha * (4e-4), places=4)

    def test_fidelity_estimation(self):
        # Both electrons excited
        self.assertAlmostEqual(self.feuA._estimate_fidelity(1), 0, places=4)

        # No excitation (all dark counts)
        self.assertAlmostEqual(self.feuA._estimate_fidelity(0), 0, places=4)

        # Check linearity in alpha (for high enough alpha due to dark counts)
        for i in range(40, 100):
            alpha = i / 200
            F = self.feuA._estimate_fidelity(alpha)
            self.assertAlmostEqual(F, 1 - alpha, places=1)

        # Perfect entanglement when no dark counts
        self.mhp_conn.midPoint.pdark = 0
        self.assertAlmostEqual(self.feuA._estimate_fidelity(0.00001), 1, places=1)

        with self.assertRaises(LinkLayerException):
            self.feuA._calculate_estimated_state(0)

    def test_minimum_fidelities(self):

        self.assertEqual(self.feuA.achievable_fidelities, self.feuB.achievable_fidelities)
        self.assertEqual(self.feuA.achievable_fidelities[0][0], 0.1)
        self.assertAlmostEqual(self.feuA.achievable_fidelities[0][1], 0.8671423723534686, places=2)
        self.assertEqual(self.feuA.achievable_fidelities[1][0], 0.3)
        self.assertAlmostEqual(self.feuA.achievable_fidelities[1][1], 0.6800331800062903, places=2)

        self.assertAlmostEqual(self.feuA.get_max_fidelity(), 0.8671423723534686, places=2)

        protoA = self.feuA.mhp_service.get_node_proto(self.feuA.node)
        protoA.set_allowed_bright_state_populations([0.1, 0.2])

        with self.assertRaises(LinkLayerException):
            self.feuA._calculate_achievable_fidelities()

    def test_get_bright_state(self):
        self.assertEqual(self.feuA.select_bright_state(0.9), None)
        self.assertEqual(self.feuA.select_bright_state(0.8), 0.1)
        self.assertEqual(self.feuA.select_bright_state(0.7), 0.1)
        self.assertEqual(self.feuA.select_bright_state(0.6), 0.3)


if __name__ == '__main__':
    unittest.main()
