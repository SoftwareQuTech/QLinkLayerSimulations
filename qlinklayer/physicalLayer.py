#
# Physical Layer generation
#

from easysquid.easyprotocol import TimedProtocol
from easysquid.entanglementGenerator import NV_PairPreparation

from easysquid.toolbox import EasySquidException


class PhysicalLayerGeneration(TimedProtocol):
    """
    This is the physical layer entanglement generation protocol.

    Parameters
    ----------
    timeStep : float
        Frequency of execution: will run every timeSteps in terms of simulation timesteps, once started
    t0 : float
        Offset when first to schedule
    node : :obj:`easysquid.qnode.QuantumNode`
        Quantum node at which this protocol is running.
    conn : :obj:`easysquid.connection.Connection`
        Connection for which this protocol is a handler.
    egp : :obj:`qlinklayer.linkLayer.EntanglementGenerationProtocol`
        Link layer protocol to connect to.
    """

    def __init__(self, timeStep, t0, node, conn, egp, alpha):
        super(TimedProtocol, self).__init__(timeStep, t0, node, conn)

        # Link to higher layer - the link layer
        self.egp = egp

        # Preparation mechanism to use
        self.prep = NV_PairPreparation(self.node.name, alpha=alpha)

        # Qubit ID's of the other side, indexed by ours
        self.otherIDs = {}

        # Outcomes ie state produces
        self.recOutcomes = {}

    def run_protocol(self):
        """
        Protocol executed at a specific time step.
        """

        # Check whether we should make pairs
        if not (self.egp.request_ready()):
            # No pairs needed right now
            return

        # Find a free memory slot
        freeID = -1
        for j in range(self.node.qmem.num_positions):
            if not self.node.qmem.in_use(j):
                freeID = j
                break

        if freeID < 0:
            # No memory available, do nothing
            return

            # This will make a new pair
        [spin, photon] = self.prep.generate()

        # Store the spin state
        self.node.qmem.add_qubit(spin, freeID)

        # Send the photon to the mid point, including the local qubit ID
        self.conn.put_from(self.node.nodeID, [freeID], photon)

    def process_data(self):
        """
        Handle incoming data.
        """
        [msg, deltaT] = self.conn.get_as(self.node.nodeID)

        # Receiving ID information on qubits
        [myID, hisID] = msg[0]

        # Get outcome
        outcome = msg[1]

        # If successful, record the qubit, otherwise remove it
        if outcome == 1 or outcome == 2:
            self.egp.pair_made(myID, hisID, outcome)

        elif outcome == 0:
            # Generation failed, remove the qubit
            self.node.qmem.release_qubit(myID)
        else:
            raise EasySquidException("Unknown outcome of heralded generation.")

        return False

    def post_process_data(self):
        # We are done, release all memory and qubits
        self.otherIDs = {}
        self.recOutcomes = {}
        for j in range(self.node.qmem.num_positions):
            self.node.qmem.release_qubit(j)

    def start(self):
        super(TimedProtocol, self).start()
