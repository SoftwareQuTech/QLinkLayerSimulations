import netsquid as ns
from time import time
from os import makedirs
from os.path import abspath, dirname, exists
from easysquid.easynetwork import Connections, setup_physical_network
from easysquid.puppetMaster import PM_Controller
from easysquid.toolbox import create_logger
from netsquid.pydynaa import DynAASim
from qlinklayer.datacollection import EGPErrorSequence, EGPOKSequence
from qlinklayer.egp import EGPRequest, NodeCentricEGP
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection
from qlinklayer.scenario import MeasureImmediatelyScenario


logger = create_logger("logger")

# Here we add an entry into the Connection structre in Easysquid Easynetwork to give us access to load up configs
# for the simulation using connections defined here in the QLinkLayer
Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION = "node_centric_heralded_fibre_connection"
Connections._CONN_BY_NAME[Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION] = NodeCentricMHPHeraldedConnection


def setup_data_directory(dir_path):
    """
    Creates a directory for storing symulation data
    :param dir_path: str
        path to create
    """
    if exists(dir_path):
        raise Exception("Simulation data directory {} already exists!".format(dir_path))
    else:
        makedirs(dir_path)


# This simulation should be run from the root QLinkLayer directory so that we can load the config
def run_simulation():
    # Grab the config we want to set the netowrk up with
    dir_path = dirname(abspath(__file__))
    config = "{}/../configs/qlink_configs/network_with_cav_with_conv.json".format(dir_path)

    # Create simulation data directory (simple timestamp) containing data collected by the datasequences
    timestamp = time()
    data_dir = "{}/{}".format(dir_path, timestamp)
    setup_data_directory(data_dir)
    err_log = "{}/error.log".format(data_dir)
    req_log = "{}/request.log".format(data_dir)

    SECOND = 1e9    # Since we are using real world parameters we need to simulate on larger timescale than ns
    ns.set_qstate_formalism(ns.DM_FORMALISM)
    DynAASim().reset()

    # Create the network
    network = setup_physical_network(config)

    # Grab the nodes and connections
    alice = network.get_node(0)
    bob = network.get_node(1)
    egp_conn = network.get_connection(alice.nodeID, bob.nodeID, "egp_conn")
    mhp_conn = network.get_connection(alice.nodeID, bob.nodeID, "mhp_conn")
    dqp_conn = network.get_connection(alice.nodeID, bob.nodeID, "dqp_conn")

    # Create our EGP instances and connect them
    egpA = NodeCentricEGP(alice)
    egpB = NodeCentricEGP(bob)
    egpA.connect_to_peer_protocol(other_egp=egpB, egp_conn=egp_conn, mhp_conn=mhp_conn, dqp_conn=dqp_conn)

    # Create requests to simulate
    num_seconds = 10
    alice_request = EGPRequest(otherID=bob.nodeID, num_pairs=3, min_fidelity=0.5, max_time=num_seconds * SECOND,
                               purpose_id=1, priority=10)

    bob_request = EGPRequest(otherID=alice.nodeID, num_pairs=3, min_fidelity=0.5, max_time=num_seconds * SECOND,
                             purpose_id=1, priority=10)

    # Configure the nodes and connections to contain all of the protocols we have set up (this is kind of painful and
    # should be refactored into something nicer)
    network.add_node_protocol(alice.nodeID, egpA.dqp)
    network.add_connection_protocol(dqp_conn, egpA.dqp)
    network.add_node_protocol(alice.nodeID, egpA.mhp)
    network.add_connection_protocol(mhp_conn, egpA.mhp)
    network.add_node_protocol(alice.nodeID, egpA)
    network.add_connection_protocol(egp_conn, egpA)

    # Do it for Bob as well
    network.add_node_protocol(bob.nodeID, egpB.dqp)
    network.add_connection_protocol(dqp_conn, egpB.dqp)
    network.add_node_protocol(bob.nodeID, egpB.mhp)
    network.add_connection_protocol(mhp_conn, egpB.mhp)
    network.add_node_protocol(bob.nodeID, egpB)
    network.add_connection_protocol(egp_conn, egpB)

    # Start up the mhp and node/connection protocols
    egpA.mhp_service.start()
    network.start()

    # Set up the Measure Immediately scenarios at nodes alice and bob
    alice_scenario = MeasureImmediatelyScenario(egp=egpA)
    bob_scenario = MeasureImmediatelyScenario(egp=egpB)

    # Schedule the calls to the EGP "create"
    alice_scenario.schedule_create(request=alice_request, t=0)
    bob_scenario.schedule_create(request=bob_request, t=0)

    # Prepare our data collection (this should be made a lot nicer especially the addEvent calls)
    sim_time = num_seconds * SECOND + 1
    pm = PM_Controller()

    # DataSequence for error collection
    err_ds = EGPErrorSequence(name="EGP Errors", recFile=err_log, maxSteps=sim_time)

    # DataSequence for ok/create collection
    ok_ds = EGPOKSequence(name="EGP OKs", recFile=req_log, maxSteps=sim_time)

    # Hook up the datasequences to the events in the EGP
    pm.addEvent(source=alice_scenario, evtType=alice_scenario._EVT_CREATE, ds=ok_ds)
    pm.addEvent(source=alice_scenario, evtType=alice_scenario._EVT_OK, ds=ok_ds)
    pm.addEvent(source=bob_scenario, evtType=bob_scenario._EVT_CREATE, ds=ok_ds)
    pm.addEvent(source=bob_scenario, evtType=bob_scenario._EVT_OK, ds=ok_ds)
    pm.addEvent(source=alice_scenario, evtType=alice_scenario._EVT_ERR, ds=err_ds)
    pm.addEvent(source=bob_scenario, evtType=bob_scenario._EVT_ERR, ds=err_ds)

    # Start the simulation
    logger.info("Beginning simulation")
    DynAASim().run(sim_time)
    logger.info("Finished simulation")


if __name__ == '__main__':
    run_simulation()
