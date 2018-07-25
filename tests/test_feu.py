import unittest
from easysquid.toolbox import EasySquidException
from qlinklayer.feu import SingleClickFidelityEstimationUnit
from netsquid.qubits import dm_fidelity
from netsquid.qubits.ketstates import b00, b01, b10, b11
from numpy import kron, isclose


class TestSingleClickFidelityEstimationUnit(unittest.TestCase):
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

    def test_fidelity_estimation(self):
        # Check various parameters against the estimation
        etaA = 1       # Transmitivity of fibre from A to midpoint
        etaB = 1       # Transmitivity of fibre from B to midpoint
        alphaA = 0.5   # Bright state population at A
        alphaB = 0.5   # Bright state population at B
        pdark = 0      # Probability of a dark count at the midpoint
        dp_photon = 1  # Dephasing parameter of the entangled qubit
        ideal_state = kron(b01.H, b01)  # (|01> + |10>)(<01| + <10|)

        # Check a lossless scenario with bright state populations of 1/2
        estimated_state = SingleClickFidelityEstimationUnit._calculate_estimated_state(etaA, etaB, alphaA, alphaB,
                                                                                       pdark, dp_photon)

        estimated_fidelity = dm_fidelity(estimated_state, ideal_state, squared=True)
        self.assertTrue(isclose(estimated_fidelity, 2 / 3))

        # Check a lossless scenario with a dark count probability of 1/2
        pdark = 0.5
        estimated_state = SingleClickFidelityEstimationUnit._calculate_estimated_state(etaA, etaB, alphaA, alphaB,
                                                                                       pdark, dp_photon)

        estimated_fidelity = dm_fidelity(ideal_state, estimated_state, squared=True)
        self.assertEqual(round(estimated_fidelity, ndigits=5), 0.5)

        # Check that depolarizing the electron completely would estimate 0 fidelity with the desired state
        dp_photon = 0
        estimated_state = SingleClickFidelityEstimationUnit._calculate_estimated_state(etaA, etaB, alphaA, alphaB,
                                                                                       pdark, dp_photon)

        estimated_fidelity = dm_fidelity(ideal_state, estimated_state, squared=True)
        self.assertEqual(estimated_fidelity, 0)

        # Check that effectively setting chance of no clicks to zero yields a state with 0 fidelity to the ideal state
        alphaA = 1
        alphaB = 1
        estimated_state = SingleClickFidelityEstimationUnit._calculate_estimated_state(etaA, etaB, alphaA, alphaB,
                                                                                       pdark, dp_photon)

        estimated_fidelity = dm_fidelity(ideal_state, estimated_state, squared=True)
        self.assertEqual(estimated_fidelity, 0)

        # Test a few extreme cases
        # Transmitivity of the fibres from a and b are 0 (imagine someone cuts the fibre)
        with self.assertRaises(EasySquidException):
            SingleClickFidelityEstimationUnit._calculate_estimated_state(etaA=0, etaB=0, alphaA=0, alphaB=0, pdark=0,
                                                                         dp_photon=0)

        with self.assertRaises(EasySquidException):
            SingleClickFidelityEstimationUnit._calculate_estimated_state(etaA=0, etaB=0, alphaA=1, alphaB=1, pdark=0,
                                                                         dp_photon=0)

        # Probability of a dark count is 1, effectively never get single clicks
        with self.assertRaises(EasySquidException):
            SingleClickFidelityEstimationUnit._calculate_estimated_state(etaA=1, etaB=1, alphaA=1, alphaB=1, pdark=1,
                                                                         dp_photon=0)


if __name__ == '__main__':
    unittest.main()
