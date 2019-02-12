import netsquid as ns
import pdb
import json
import os
from time import time
import math
import logging
import os
from os.path import exists
from easysquid.easynetwork import Connections, setup_physical_network
from easysquid.puppetMaster import PM_Controller
from easysquid.toolbox import logger
from netsquid.simutil import SECOND, sim_reset, sim_run, sim_time
from qlinklayer.datacollection import EGPErrorSequence, EGPOKSequence, EGPCreateSequence, EGPStateSequence, \
    EGPQubErrSequence, EGPLocalQueueSequence, AttemptCollector, current_version
from qlinklayer.egp import NodeCentricEGP
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection
from qlinklayer.specific_scenarios import MixedScenario
from simulations._get_configs_from_easysquid import NODE_CENTRIC_HERALDED_FIBRE_CONNECTION

logger.setLevel(logging.DEBUG)

# Here we add an entry into the Connection structure in Easysquid Easynetwork to give us access to load up configs
# for the simulation using connections defined here in the QLinkLayer
Connections.NODE_CENTRIC_HERALDED_FIBRE_CONNECTION = NODE_CENTRIC_HERALDED_FIBRE_CONNECTION
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


def setup_data_collection(scenarioA, scenarioB, collection_duration, dir_path, collect_queue_data=False):
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

    # DataSequence for QubErr collection
    quberr_ds = EGPQubErrSequence(name="EGP QubErr", dbFile=data_file, maxSteps=collection_duration)

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
    pm.addEvent(scenarioA, scenarioA._EVT_CK_OK, ds=ok_ds)
    pm.addEvent(scenarioA, scenarioA._EVT_MD_OK, ds=ok_ds)
    pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_CK_OK, ds=state_ds)
    pm.addEvent(source=scenarioA, evtType=scenarioA._EVT_ERR, ds=err_ds)

    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_CREATE, ds=create_ds)
    pm.addEvent(scenarioB, scenarioB._EVT_CK_OK, ds=ok_ds)
    pm.addEvent(scenarioB, scenarioB._EVT_MD_OK, ds=ok_ds)
    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_CK_OK, ds=state_ds)
    pm.addEvent(source=scenarioB, evtType=scenarioB._EVT_ERR, ds=err_ds)

    pm.addEventAny([scenarioA, scenarioB], [scenarioA._EVT_MD_OK, scenarioB._EVT_MD_OK], ds=quberr_ds)

    if collect_queue_data:
        for qid in range(len(lqAs)):
            lq = lqAs[qid]
            pm.addEvent(lq, lq._EVT_ITEM_ADDED, ds=lqA_ds[qid])
            pm.addEvent(lq, lq._EVT_ITEM_REMOVED, ds=lqA_ds[qid])
        for qid in range(len(lqBs)):
            lq = lqBs[qid]
            pm.addEvent(lq, lq._EVT_ITEM_ADDED, ds=lqB_ds[qid])
            pm.addEvent(lq, lq._EVT_ITEM_REMOVED, ds=lqB_ds[qid])

    collectors = [create_ds, ok_ds, state_ds, quberr_ds, err_ds]
    if collect_queue_data:
        collectors += lqA_ds + lqB_ds
    return collectors


def create_scenarios(egpA, egpB, request_cycle, request_paramsA, request_paramsB, additional_data=None):
    if request_cycle == 0:
        # Use t_cycle of MHP for the request cycle
        request_cycle = egpA.mhp.conn.t_cycle

    if additional_data:
        additional_data["request_t_cycle"] = request_cycle
        additional_data["request_paramsA"] = request_paramsA
        additional_data["request_paramsB"] = request_paramsB

    # Set up the Measure Immediately scenarios at nodes alice and bob
    alice_scenario = MixedScenario(egpA, request_cycle, request_paramsA)
    bob_scenario = MixedScenario(egpB, request_cycle, request_paramsB)
    alice_scenario.start()
    bob_scenario.start()

    return alice_scenario, bob_scenario


def setup_network_protocols(network, alphaA=0.1, alphaB=0.1, num_priorities=1, egp_queue_weights=None,
                            collect_queue_data=False):
    # Grab the nodes and connections
    nodeA = network.get_node_by_id(0)
    nodeB = network.get_node_by_id(1)
    egp_conn = network.get_connection(nodeA, nodeB, "egp_conn")
    mhp_conn = network.get_connection(nodeA, nodeB, "mhp_conn")
    dqp_conn = network.get_connection(nodeA, nodeB, "dqp_conn")

    # Create our EGP instances and connect them
    egpA = NodeCentricEGP(nodeA, num_priorities=num_priorities, scheduler_weights=egp_queue_weights,
                          throw_local_queue_events=collect_queue_data, accept_all_requests=True)
    egpB = NodeCentricEGP(nodeB, num_priorities=num_priorities, scheduler_weights=egp_queue_weights,
                          throw_local_queue_events=collect_queue_data, accept_all_requests=True)
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


def set_datacollection_version(results_path):
    """
    Writes the current datacollection version to a file.
    :param results_path:
    :return:
    """
    data_folder_path = os.path.split(results_path)[0]
    version_path = os.path.join(data_folder_path, "versions.json")
    if os.path.exists(version_path):
        with open(version_path, 'r') as f:
            versions = json.load(f)
    else:
        versions = {}
    versions["datacollection"] = current_version
    with open(version_path, 'w') as f:
        json.dump(versions, f)


# This simulation should be run from the root QLinkLayer directory so that we can load the config
def run_simulation(results_path, sim_dir, request_paramsA, request_paramsB, name=None, config=None, num_priorities=1,
                   egp_queue_weights=None, request_cycle=0, max_sim_time=float('inf'), max_wall_time=float('inf'),
                   max_mhp_cycle=float('inf'), enable_pdb=False, t0=0, t_cycle=0, alphaA=0.1, alphaB=0.1,
                   wall_time_per_timestep=60, save_additional_data=True, collect_queue_data=False):

    # Set the current datacollection version
    set_datacollection_version(results_path)

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
    mhp_conn.set_timings(t_cycle=t_cycle, t0=t0)
    if save_additional_data:
        additional_data["mhp_t_cycle"] = mhp_conn.t_cycle

    # Setup entanglement generation protocols
    egpA, egpB = setup_network_protocols(network, alphaA=alphaA, alphaB=alphaB, num_priorities=num_priorities,
                                         egp_queue_weights=egp_queue_weights, collect_queue_data=collect_queue_data)
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
    alice_scenario, bob_scenario = create_scenarios(egpA, egpB, request_cycle, request_paramsA, request_paramsB,
                                                    additional_data)

    # Hook up data collectors to the scenarios
    collectors = setup_data_collection(alice_scenario, bob_scenario, max_sim_time, results_path,
                                       collect_queue_data=collect_queue_data)

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
                info_message = "Simulation: \"{}\": ".format(name)
                info_message += "Time advanced: {}/{} s real time.  ".format(sim_time() / SECOND, max_sim_time)
                info_message += "{}/{} s wall time. ".format(now - start_time, max_wall_time)
                info_message += "{}/{} MHP cycles".format(mhp_cycles, max_mhp_cycle)
                logger.info(info_message)

            else:
                info_message = "Time advanced: {}/{} s real time.  ".format(sim_time() / SECOND, max_sim_time)
                info_message += "{}/{} s wall time. ".format(now - start_time, max_wall_time)
                info_message += "{}/{} MHP cycles".format(mhp_cycles, max_mhp_cycle)
                logger.info(info_message)


            # Save additional data relevant for the simulation
            if save_additional_data:
                # Collect simulation times
                additional_data["total_real_time"] = sim_time()
                additional_data["total_wall_time"] = time() - start_time

                # Collect probability of success
                midpoint = mhp_conn.midPoint
                if midpoint._nr_of_meas > 0:
                    additional_data["p_succ"] = midpoint._nr_of_succ / midpoint._nr_of_meas
                else:
                    additional_data["p_succ"] = None
                with open(results_path + "_additional_data.json", 'w') as json_file:
                    json.dump(additional_data, json_file, indent=4)

            # Commit the data collected in the data-sequences
            for collector in collectors:
                collector.commit_data()

            if now - start_time > max_wall_time:
                logger.info("Max wall time reached, ending simulation.")
                break

            clean_log_files(results_path, sim_dir)

        stop_time = time()
        logger.info("Finished simulation, took {} (s) wall time and {} (s) real time".format(stop_time - start_time,
                                                                                             sim_time() / SECOND))

        clean_log_files(results_path, sim_dir)

    # Allow for Ctrl-C-ing out of a simulation in a manner that commits data to the databases
    except Exception:
        logger.exception("Something went wrong. Ending simulation early!")

    finally:
        # Debugging
        if enable_pdb:
            pdb.set_trace()


def clean_log_files(results_path, sim_dir, block_size=1000):
    base_name = os.path.split(results_path)[1]
    timestamp = base_name.split("_key_")[0]
    log_file = "{}/{}_CREATE_and_measure/{}_log.out".format(sim_dir, timestamp, base_name)

    offset = int(block_size / 2)

    with open(log_file, 'r') as f:
        lines = f.readlines()

    # Find where to start
    for i in range(len(lines)):
        line = lines[i]
        if line == "PLACEHOLDER\n":
            i += 1
            min_block = i
            end_block = i
            break
    else:
        i = 0
        min_block = 0
        end_block = 0

    new_lines = lines[:i]

    while i < len(lines):
        line = lines[i]
        if ("WARNING" in line) or ("ERROR" in line):
            start_block = max(min_block, i - offset)

            # Have we skipped something?
            if start_block > end_block:
                new_lines.append("... ({} lines skipped)\n".format(start_block - end_block))

            end_block = min(len(lines), i + offset)
            new_lines += lines[start_block:end_block]
            i = end_block
            min_block = end_block
        elif "INFO" in line:

            # Have we skipped something?
            if i > end_block:
                new_lines.append("... ({} lines skipped)\n".format(i - end_block))

            new_lines.append(line)
            end_block = i + 1
            min_block = end_block
            i += 1
        else:
            i += 1


    # Add a place holder such that we know where to start next time
    new_lines.append("PLACEHOLDER\n")

    with open(log_file, 'w') as f:
        f.writelines(new_lines)
