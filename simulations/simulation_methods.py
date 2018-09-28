import netsquid as ns
import pdb
import json
import os, sys
from time import time
import math
from os.path import exists
from easysquid.easynetwork import Connections, setup_physical_network
from easysquid.puppetMaster import PM_Controller
from easysquid.toolbox import logger
from netsquid.simutil import SECOND, sim_reset, sim_run, sim_time
from qlinklayer.datacollection import EGPErrorSequence, EGPOKSequence, EGPCreateSequence, EGPStateSequence, \
    EGPQubErrSequence, EGPLocalQueueSequence, AttemptCollector
from qlinklayer.egp import NodeCentricEGP
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection
from qlinklayer.scenario import MeasureAfterSuccessScenario, MeasureBeforeSuccessScenario

# Here we add an entry into the Connection structure in Easysquid Easynetwork to give us access to load up configs
# for the simulation using connections defined here in the QLinkLayer
Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION = "node_centric_heralded_fibre_connection"
Connections._CONN_BY_NAME[Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION] = NodeCentricMHPHeraldedConnection


def setup_simulation():
    ns.set_qstate_formalism(ns.DM_FORMALISM)
    sim_reset()


def setup_data_directory(dir_path):
    """
    Creates a directory for storing simulation data
    :param dir_path: str
        path to create
    """
    if exists("{}.db".format(dir_path)):
        raise Exception("Simulation data directory {}.db already exists!".format(dir_path))
    else:
        pass


def setup_data_collection(scenarioA, scenarioB, collection_duration, dir_path, measure_directly,
                          collect_queue_data=False):
    # Create simulation data directory (simple timestamp) containing data collected by the datasequences
    setup_data_directory(dir_path)

    logger.info("Saving results to {}.db".format(dir_path))

    # Prepare our data collection (this should be made a lot nicer especially the addEvent calls)
    pm = PM_Controller()
    data_file = "{}.db".format(dir_path)

    # DataSequence for error collection
    err_ds = EGPErrorSequence(name="EGP Errors", dbFile=data_file, maxSteps=collection_duration)

    # DataSequence for create collection
    create_ds = EGPCreateSequence(name="EGP Creates", dbFile=data_file, maxSteps=collection_duration)

    # DataSequence for ok/create collection
    # Create the attempt collectors for the two nodes
    attempt_collectors = {}
    for scenario in [scenarioA, scenarioB]:
        node_id = scenario.node.nodeID
        egp = scenario.egp
        attempt_collectors[node_id] = AttemptCollector(egp)
    ok_ds = EGPOKSequence(name="EGP OKs", attempt_collectors=attempt_collectors, dbFile=data_file,
                          maxSteps=collection_duration)

    if measure_directly:
        # DataSequence for QubErr collection
        quberr_ds = EGPQubErrSequence(name="EGP QubErr", dbFile=data_file, maxSteps=collection_duration)
    else:
        # DataSequence for entangled state collection
        state_ds = EGPStateSequence(name="EGP Qubit States", dbFile=data_file, maxSteps=collection_duration)

    if collect_queue_data:
        # DataSequence for local queues
        lqAs = scenarioA.egp.dqp.queueList
        lqBs = scenarioB.egp.dqp.queueList
        lqA_ds = [EGPLocalQueueSequence(name="EGP Local Queue A {}".format(qid),
                                        dbFile=data_file, maxSteps=collection_duration) for qid in range(len(lqAs))]
        lqB_ds = [EGPLocalQueueSequence(name="EGP Local Queue B {}".format(qid),
                                        dbFile=data_file, maxSteps=collection_duration) for qid in range(len(lqBs))]

    # Hook up the datasequences to the events in that occur
    pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_CREATE, ds=create_ds)
    pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_OK, ds=ok_ds)
    if not measure_directly:
        pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_OK, ds=state_ds)
    pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_ERR, ds=err_ds)

    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_CREATE, ds=create_ds)
    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_OK, ds=ok_ds)
    if not measure_directly:
        pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_OK, ds=state_ds)
    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_ERR, ds=err_ds)

    if measure_directly:
        pm.addEventAny([scenarioA, scenarioB], [scenarioA._EVT_OK, scenarioB._EVT_OK], ds=quberr_ds)

    if collect_queue_data:
        for qid in range(len(lqAs)):
            lq = lqAs[qid]
            pm.addEvent(lq, lq._EVT_ITEM_ADDED, ds=lqA_ds[qid])
            pm.addEvent(lq, lq._EVT_ITEM_REMOVED, ds=lqA_ds[qid])
        for qid in range(len(lqBs)):
            lq = lqBs[qid]
            pm.addEvent(lq, lq._EVT_ITEM_ADDED, ds=lqB_ds[qid])
            pm.addEvent(lq, lq._EVT_ITEM_REMOVED, ds=lqB_ds[qid])

    if measure_directly:
        collectors = [create_ds, ok_ds, quberr_ds, err_ds]
    else:
        collectors = [create_ds, ok_ds, state_ds, err_ds]
    if collect_queue_data:
        collectors += lqA_ds + lqB_ds
    return collectors


def create_scenarios(egpA, egpB, create_probA, create_probB, min_pairs, max_pairs, tmax_pair,
                     request_cycle, num_requests, measure_directly,
                     additional_data=None):
    if request_cycle == 0:
        # Use t_cycle of MHP for the request cycle
        request_cycle = egpA.mhp.conn.t_cycle

    if additional_data:
        additional_data["request_t_cycle"] = request_cycle
        additional_data["create_request_probA"] = create_probA
        additional_data["create_request_probB"] = create_probB

    # Set up the Measure Immediately scenarios at nodes alice and bob
    other_request_info = {"min_pairs": min_pairs, "max_pairs": max_pairs, "tmax_pair": tmax_pair,
                          "num_requests": num_requests}
    if measure_directly:
        alice_scenario = MeasureBeforeSuccessScenario(egp=egpA, request_cycle=request_cycle, request_prob=create_probA,
                                                      **other_request_info)
        bob_scenario = MeasureBeforeSuccessScenario(egp=egpB, request_cycle=request_cycle, request_prob=create_probB,
                                                    **other_request_info)
    else:
        alice_scenario = MeasureAfterSuccessScenario(egp=egpA, request_cycle=request_cycle, request_prob=create_probA,
                                                     **other_request_info)
        bob_scenario = MeasureAfterSuccessScenario(egp=egpB, request_cycle=request_cycle, request_prob=create_probB,
                                                   **other_request_info)
    alice_scenario.start()
    bob_scenario.start()

    return alice_scenario, bob_scenario


def setup_network_protocols(network, alphaA=0.1, alphaB=0.1, collect_queue_data=False):
    # Grab the nodes and connections
    nodeA = network.get_node_by_id(0)
    nodeB = network.get_node_by_id(1)
    egp_conn = network.get_connection(nodeA, nodeB, "egp_conn")
    mhp_conn = network.get_connection(nodeA, nodeB, "mhp_conn")
    dqp_conn = network.get_connection(nodeA, nodeB, "dqp_conn")

    # Create our EGP instances and connect them
    egpA = NodeCentricEGP(nodeA, throw_local_queue_events=collect_queue_data)
    egpB = NodeCentricEGP(nodeB, throw_local_queue_events=collect_queue_data)
    egpA.connect_to_peer_protocol(other_egp=egpB, egp_conn=egp_conn, mhp_conn=mhp_conn, dqp_conn=dqp_conn,
                                  alphaA=alphaA, alphaB=alphaB)

    # Attach the protocols to the nodes and connections
    network.add_network_protocol(egpA.dqp, nodeA, dqp_conn)
    network.add_network_protocol(egpA.mhp, nodeA, mhp_conn)
    network.add_network_protocol(egpA, nodeA, egp_conn)
    network.add_network_protocol(egpB.dqp, nodeB, dqp_conn)
    network.add_network_protocol(egpB.mhp, nodeB, mhp_conn)
    network.add_network_protocol(egpB, nodeB, egp_conn)

    return egpA, egpB


def _calc_prob_success_handler(midpoint, additional_data):
    p_succ = midpoint._prob_of_success
    additional_data["p_succ"] = p_succ


# This simulation should be run from the root QLinkLayer directory so that we can load the config
def run_simulation(results_path, name=None, config=None, create_probA=1, create_probB=0, min_pairs=1, max_pairs=1,
                   tmax_pair=0,
                   request_cycle=0, num_requests=1, max_sim_time=float('inf'),
                   max_wall_time=float('inf'), max_mhp_cycle=float('inf'), enable_pdb=False, measure_directly=False,
                   t0=0, t_cycle=0,
                   alphaA=0.1, alphaB=0.1, wall_time_per_timestep=60, save_additional_data=True,
                   collect_queue_data=False):

    sim_dir_env = "SIMULATION_DIR"

    # Check that the simulation path is set
    if sim_dir_env not in os.environ:
        print("The environment variable {} must be set to the path to the simulation folder"
              "before running this script!".format(sim_dir_env))
        sys.exit()
    else:
        sim_dir = os.getenv(sim_dir_env)
        if not os.path.isdir(sim_dir):
            print("The environment variable {} is not a path to a folder.")
            sys.exit()

    # Check that sim_dir ends with '/'
    if not sim_dir[-1] == '/':
        sim_dir += "/"

    # Save additional data
    if save_additional_data:
        additional_data = {}
    else:
        additional_data = None
    # Set up the simulation
    setup_simulation()

    # Get absolute path to config
    abs_config_path = sim_dir + config

    # Create the network
    network = setup_physical_network(abs_config_path)
    # Recompute the timings of the heralded connection, depending on measure_directly
    nodeA = network.get_node_by_id(0)
    nodeB = network.get_node_by_id(1)
    # print(nodeA.qmem._memory_positions[0]._connections[1])
    # print(nodeA.qmem._memory_positions[0].get_gate(CNOTGate(), 1))
    # raise RuntimeError()
    mhp_conn = network.get_connection(nodeA, nodeB, "mhp_conn")
    mhp_conn.set_timings(t_cycle=t_cycle, t0=t0, measure_directly=measure_directly)
    if save_additional_data:
        additional_data["mhp_t_cycle"] = mhp_conn.t_cycle

    # Setup entanglement generation protocols
    egpA, egpB = setup_network_protocols(network, alphaA=alphaA, alphaB=alphaB, collect_queue_data=collect_queue_data)
    if save_additional_data:
        additional_data["alphaA"] = alphaA
        additional_data["alphaB"] = alphaB

    # Get start time
    start_time = time()

    # Check if any max_times should be infinite
    if max_wall_time == 0:
        max_wall_time = float('inf')
    if max_sim_time == 0:
        max_sim_time = float('inf')
    if max_mhp_cycle == 0:
        max_mhp_cycle = float('inf')
    max_sim_time = min(max_sim_time, mhp_conn.t_cycle * max_mhp_cycle / SECOND)

    # Create scenarios which act as higher layers communicating with the EGPs
    alice_scenario, bob_scenario = create_scenarios(egpA, egpB, create_probA, create_probB, min_pairs,
                                                    max_pairs, tmax_pair, request_cycle, num_requests,
                                                    measure_directly, additional_data)

    # Hook up data collectors to the scenarios
    collectors = setup_data_collection(alice_scenario, bob_scenario, max_sim_time, results_path, measure_directly,
                                       collect_queue_data=collect_queue_data)

    # Schedule event handler to listen the probability of success being computed
    if save_additional_data:
        midpoint = mhp_conn.midPoint
        p_succ_handler = ns.EventHandler(lambda event: _calc_prob_success_handler(midpoint, additional_data))
        ns.Entity()._wait_once(p_succ_handler, entity=midpoint, event_type=midpoint._EVT_CALC_PROB)

    # Start the simulation
    network.start()

    # Debugging
    if enable_pdb:
        pdb.set_trace()

    # Start with a step size of 1 millisecond
    timestep = min(1e3, max_sim_time * SECOND)

    logger.info("Beginning simulation")

    # Keep track of the number of steps taken
    timesteps_taken = 0

    # Keep track of total wall time (not including preparation)
    wall_time_of_simulation = 0

    try:
        # Run simulation
        while sim_time() < max_sim_time * SECOND:
            # Check wall time during this simulation step
            wall_time_sim_step_start = time()
            if timestep == float('inf') or timestep == -float('inf'):
                raise RuntimeError()
            sim_run(duration=timestep)
            previous_timestep = timestep
            wall_time_sim_step_stop = time()
            wall_time_sim_step_duration = wall_time_sim_step_stop - wall_time_sim_step_start
            wall_time_of_simulation += wall_time_sim_step_duration
            wall_time_s_per_real_time_ns = wall_time_of_simulation / sim_time()
            timesteps_taken += 1

            # Check if wall_time_sim_step_duration is zero
            if wall_time_sim_step_duration == 0:
                # Just make the timestep 10 times bigger since last duration went very quick
                timestep = timestep * 10
            else:
                # Update the timestep such that one timestep takes 'wall_time_per_timestep' seconds
                timestep = wall_time_per_timestep / wall_time_s_per_real_time_ns

            # Don't use a timestep that goes beyong max_sim_time
            if (sim_time() + timestep) > (max_sim_time * SECOND):
                timestep = (max_sim_time * SECOND) - sim_time()

            # Check clock once in a while
            now = time()
            logger.info(
                "Wall clock advanced {} s during the last {} s real time. Will now advance {} s real time.".format(
                    wall_time_sim_step_duration, previous_timestep / SECOND, timestep / SECOND))
            mhp_cycles = math.floor(sim_time() / mhp_conn.t_cycle)
            if name:
                logger.info("Simulation: \"{}\": ".format(name) +
                            "Time advanced: {}/{} s real time.  ".format(sim_time() / SECOND, max_sim_time) +
                            "{}/{} s wall time. ".format(now - start_time, max_wall_time) +
                            "{}/{} MHP cycles".format(mhp_cycles, max_mhp_cycle))
            else:
                logger.info("Time advanced: {}/{} s real time.  ".format(sim_time() / SECOND, max_sim_time) +
                            "{}/{} s wall time. ".format(now - start_time, max_wall_time) +
                            "{}/{} MHP cycles".format(mhp_cycles, max_mhp_cycle))

            # Save additional data relevant for the simulation
            if save_additional_data:
                # Collect simulation times
                additional_data["total_real_time"] = sim_time()
                additional_data["total_wall_time"] = time() - start_time
                with open(results_path + "_additional_data.json", 'w') as json_file:
                    json.dump(additional_data, json_file, indent=4)

            # Commit the data collected in the data-sequences
            for collector in collectors:
                collector.commit_data()

            if now - start_time > max_wall_time:
                logger.info("Max wall time reached, ending simulation.")
                break

        stop_time = time()
        logger.info("Finished simulation, took {} (s) wall time and {} (s) real time".format(stop_time - start_time,
                                                                                             sim_time() / SECOND))
    # Allow for Ctrl-C-ing out of a simulation in a manner that commits data to the databases
    except Exception:
        logger.exception("Something went wrong. Ending simulation early!")

    finally:
        # Debugging
        if enable_pdb:
            pdb.set_trace()
