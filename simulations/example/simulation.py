import netsquid as ns
from os.path import abspath, dirname
from easysquid.easynetwork import Connections, setup_physical_network
from easysquid.toolbox import create_logger
from netsquid.pydynaa import DynAASim
from qlinklayer.egp import EGPRequest, NodeCentricEGP
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection
from qlinklayer.scenario import MeasureImmediatelyScenario


logger = create_logger("logger")
Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION = "node_centric_heralded_fibre_connection"
Connections._CONN_BY_NAME[Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION] = NodeCentricMHPHeraldedConnection


def run_simulation():
    dir_path = dirname(abspath(__file__))
    config = "{}/configs/qlink_configs/network_with_cav_with_conv.json".format(dir_path)

    SECOND = 1e9
    ns.set_qstate_formalism(ns.DM_FORMALISM)
    DynAASim().reset()

    network = setup_physical_network(config)

    alice = network.get_node(0)
    bob = network.get_node(1)

    egpA = NodeCentricEGP(alice)
    egpB = NodeCentricEGP(bob)

    egp_conn = network.get_connection(alice.nodeID, bob.nodeID, "egp_conn")
    mhp_conn = network.get_connection(alice.nodeID, bob.nodeID, "mhp_conn")
    dqp_conn = network.get_connection(alice.nodeID, bob.nodeID, "dqp_conn")

    egpA.connect_to_peer_protocol(other_egp=egpB, egp_conn=egp_conn, mhp_conn=mhp_conn, dqp_conn=dqp_conn)

    num_seconds = 10
    alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=3, min_fidelity=0.5, max_time=num_seconds * SECOND,
                               purpose_id=1, priority=10)

    bob_request = EGPRequest(otherID=alice.nodeID, num_pairs=3, min_fidelity=0.5, max_time=num_seconds * SECOND,
                             purpose_id=1, priority=10)

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

    alice_scenario = MeasureImmediatelyScenario(egp=egpA)
    bob_scenario = MeasureImmediatelyScenario(egp=egpB)
    alice_scenario.schedule_create(request=alice_request, t=0)
    bob_scenario.schedule_create(request=bob_request, t=0)

    logger.info("Beginning simulation")
    sim_time = num_seconds * SECOND + 1
    DynAASim().run(sim_time)
    logger.info("Finished simulation")


if __name__ == '__main__':
    run_simulation()
