import netsquid as ns
from time import time
from os import makedirs
from os.path import abspath, dirname, exists
from easysquid.easynetwork import Connections, setup_physical_network
from easysquid.puppetMaster import PM_Controller
from easysquid.toolbox import create_logger
from netsquid.simutil import SECOND, sim_reset, sim_run
from qlinklayer.datacollection import EGPErrorSequence, EGPOKSequence
from qlinklayer.egp import EGPRequest, NodeCentricEGP
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection
from qlinklayer.scenario import MeasureImmediatelyScenario


logger = create_logger("logger")

# Here we add an entry into the Connection structre in Easysquid Easynetwork to give us access to load up configs
# for the simulation using connections defined here in the QLinkLayer
Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION = "node_centric_heralded_fibre_connection"
Connections._CONN_BY_NAME[Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION] = NodeCentricMHPHeraldedConnection


def setup_simulation():
    ns.set_qstate_formalism(ns.DM_FORMALISM)
    sim_reset()


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


def setup_data_collection(scenarioA, scenarioB, collection_duration, dir_path):
    # Create simulation data directory (simple timestamp) containing data collected by the datasequences
    timestamp = time()
    data_dir = "{}/{}".format(dir_path, timestamp)
    setup_data_directory(data_dir)

    # Prepare our data collection (this should be made a lot nicer especially the addEvent calls)
    pm = PM_Controller()
    err_log = "{}/error.log".format(data_dir)
    req_log = "{}/request.log".format(data_dir)

    # DataSequence for error collection
    err_ds = EGPErrorSequence(name="EGP Errors", recFile=err_log, maxSteps=collection_duration)

    # DataSequence for ok/create collection
    ok_ds = EGPOKSequence(name="EGP OKs", recFile=req_log, maxSteps=collection_duration)

    # Hook up the datasequences to the events in that occur
    pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_CREATE, ds=ok_ds)
    pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_OK, ds=ok_ds)
    pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_ERR, ds=err_ds)

    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_CREATE, ds=ok_ds)
    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_OK, ds=ok_ds)
    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_ERR, ds=err_ds)


def schedule_scenario_actions(scenarioA, scenarioB):
    idA = scenarioA.egp.node.nodeID
    idB = scenarioB.egp.node.nodeID
    max_time = 60 * SECOND

    requestA = EGPRequest(otherID=idB, num_pairs=3, min_fidelity=0.5, max_time=max_time, purpose_id=1, priority=10)
    requestB = EGPRequest(otherID=idA, num_pairs=3, min_fidelity=0.5, max_time=max_time, purpose_id=1, priority=10)

    scenarioA.schedule_create(request=requestA, t=0)
    scenarioB.schedule_create(request=requestB, t=0)


def setup_network_protocols(network):
    # Grab the nodes and connections
    nodeA = network.get_node_by_id(0)
    nodeB = network.get_node_by_id(1)
    egp_conn = network.get_connection(nodeA, nodeB, "egp_conn")
    mhp_conn = network.get_connection(nodeA, nodeB, "mhp_conn")
    dqp_conn = network.get_connection(nodeA, nodeB, "dqp_conn")

    # Create our EGP instances and connect them
    egpA = NodeCentricEGP(nodeA)
    egpB = NodeCentricEGP(nodeB)
    egpA.connect_to_peer_protocol(other_egp=egpB, egp_conn=egp_conn, mhp_conn=mhp_conn, dqp_conn=dqp_conn)

    # Attach the protocols to the nodes and connections
    network.add_network_protocol(egpA.dqp, nodeA, dqp_conn)
    network.add_network_protocol(egpA.mhp, nodeA, mhp_conn)
    network.add_network_protocol(egpA, nodeA, egp_conn)
    network.add_network_protocol(egpB.dqp, nodeB, dqp_conn)
    network.add_network_protocol(egpB.mhp, nodeB, mhp_conn)
    network.add_network_protocol(egpB, nodeB, egp_conn)

    return egpA, egpB


# This simulation should be run from the root QLinkLayer directory so that we can load the config
def run_simulation():
    # Grab the config we want to set the netowrk up with
    dir_path = dirname(abspath(__file__))
    config = "{}/../configs/qlink_configs/network_with_cav_with_conv.json".format(dir_path)

    # Set up the simulation
    setup_simulation()

    # Create the network
    network = setup_physical_network(config)
    egpA, egpB = setup_network_protocols(network)

    # Set up the Measure Immediately scenarios at nodes alice and bob
    alice_scenario = MeasureImmediatelyScenario(egp=egpA)
    bob_scenario = MeasureImmediatelyScenario(egp=egpB)
    schedule_scenario_actions(alice_scenario, bob_scenario)

    # Hook up data collectors to the scenarios
    sim_duration = 60 * SECOND + 1
    setup_data_collection(alice_scenario, bob_scenario, sim_duration, dir_path)

    # Start the simulation
    network.start()

    logger.info("Beginning simulation")
    start_time = time()

    sim_run(sim_duration)

    stop_time = time()
    logger.info("Finished simulation, took {}".format(stop_time - start_time))

    # Set a trace to allow inspection
    import pdb
    pdb.set_trace()


if __name__ == '__main__':
    run_simulation()
