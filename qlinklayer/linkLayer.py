#
# Entanglement Generation Protocol
#

from easysquid.easyprotocol import EasyProtocol

from qlinklayer.scheduler import RequestScheduler
from qlinklayer.qmm import QuantumMemoryManagement
from qlinklayer.distQueue import DistributedQueue
from qlinklayer.physicalLayer import PhysicalLayerGeneration


class EntanglementGenerationProtocol(EasyProtocol):
    """
    Entanglement generation protocol.

    This is the overall link layer protocol that receive create commands and subsequently produces pairs
    or else fails.

    Parameter
    ---------
    em : :obj:`qlinklayer.higherLayer.EntanglementManagement`
        Entanglement management unit at a higher layer to which we deliver any responses.
    node : :obj:`easysquid.qnode.QuantumNode`
        Quantum network node we run on.
    conn : :obj:`easysquid.connection.Connection`
        Connection to use for producing pairs.
    """

    def __init__(self, em, node, conn):

        # Entanglement management unit
        self.em = em

        # Node to run on
        self.node = node 

        # Connection to use
        self.conn = conn

        # Set up a distributed queue
        self.distQueue = DistributedQueue(node, conn)

        # Set up scheduler
        self.scheduler = RequestScheduler(self.distQueue)

        # Set of memory management unit
        self.qmm = QuantumMemoryManagement()

        # Physical layer entanglement generation
        self.physical = PhysicalLayerGeneration(t0=conn.t0, timeStep=conn.tmax, node=node, conn=conn, egp=self)

    # API to the Entanglement Generation Protocol

    def create(self, numPairs, appID=0, Fmin=0.5, tmax=100000):
        """
        Create request.

        Parameters
        ----------
        numPairs : int
            Number of pairs to produce
        appID : int
            Purpose of this creation request
        Fmin : float
            Minimum acceptable fidelity (Default 0.5)
        tmax : float
            Maximum acceptable waiting time in s (Default 100000s, i.e. we don't care)
        """

        # Assemble request
        request = _EGP_Create(numPairs, appID, Fmin, tmax)

        # Decide which queue to add it to
        qid = self.scheduler.get_queue(request)

        # Add to request queue (TODO for now we use only 1 queue, qid=0)
        self.distQueue.add(request, qid)

    # Communication with lower layer (physical layer protocol)

    def request_ready(self):
        """
        Check whether the physical layer should produce more pairs. 
        """
        return self.scheduler.request_ready()

    def pair_made(self, myID, hisID, outcome):
        """
        Pair has been produced.
        """
        pass

        
class _EGP_Create:
    def __init__(self, numPairs, appID, Fmin, tmax):
        self.numPairs = numPairs
        self.appID = appID
        self.Fmin = Fmin
        self.tmax = tmax
