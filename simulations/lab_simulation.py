import netsquid as ns
from os.path import abspath, dirname
from easysquid.easynetwork import Connections, setup_physical_network
from netsquid.pydynaa import DynAASim
from qlinklayer.egp import EGPRequest, NodeCentricEGP
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection

Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION = "node_centric_heralded_fibre_connection"
Connections._CONN_BY_NAME[Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION] = NodeCentricMHPHeraldedConnection

dir_path = dirname(abspath(__file__))
config = "{}/configs/lab_config.json".format(dir_path)
DynAASim().reset()

SECOND = 1e9
ns.set_qstate_formalism(ns.DM_FORMALISM)

network = setup_physical_network(config)

alice = network.get_node(0)
bob = network.get_node(1)

egpA = NodeCentricEGP(alice)
egpB = NodeCentricEGP(bob)

egp_conn = network.get_connection(alice.nodeID, bob.nodeID, "egp_conn")
mhp_conn = network.get_connection(alice.nodeID, bob.nodeID, "mhp_conn")
dqp_conn = network.get_connection(alice.nodeID, bob.nodeID, "dqp_conn")

egpA.connect_to_peer_protocol(other_egp=egpB, egp_conn=egp_conn, mhp_conn=mhp_conn, dqp_conn=dqp_conn)

request = EGPRequest(otherID=bob.nodeID, num_pairs=3, min_fidelity=0.5, max_time=2 * SECOND, purpose_id=1, priority=10)

egpA.create(request)

network.add_node_protocol(alice.nodeID, egpA.dqp)
network.add_connection_protocol(dqp_conn, egpA.dqp)
network.add_node_protocol(alice.nodeID, egpA.mhp)
network.add_connection_protocol(mhp_conn, egpA.mhp)
network.add_node_protocol(alice.nodeID, egpA)
network.add_connection_protocol(egp_conn, egpA)

network.add_node_protocol(bob.nodeID, egpB.dqp)
network.add_connection_protocol(dqp_conn, egpB.dqp)
network.add_node_protocol(bob.nodeID, egpB.mhp)
network.add_connection_protocol(mhp_conn, egpB.mhp)
network.add_node_protocol(bob.nodeID, egpB)
network.add_connection_protocol(egp_conn, egpB)

egpA.mhp_service.start()

network.start()

DynAASim().run(request.max_time + 1)
