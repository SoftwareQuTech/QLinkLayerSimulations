import netsquid as ns
import pdb
import json
from argparse import ArgumentParser
from time import time
from os.path import exists
from random import random, randint
from easysquid.easynetwork import Connections, setup_physical_network
from easysquid.puppetMaster import PM_Controller
from easysquid.toolbox import create_logger
from netsquid.simutil import SECOND, sim_reset, sim_run, sim_time
from qlinklayer.datacollection import EGPErrorSequence, EGPOKSequence, EGPCreateSequence, EGPStateSequence, \
    MHPNodeEntanglementAttemptSequence, MHPMidpointEntanglementAttemptSequence, EGPQubErrSequence
from qlinklayer.egp import EGPRequest, NodeCentricEGP
from qlinklayer.mhp import NodeCentricMHPHeraldedConnection
from qlinklayer.scenario import MeasureAfterSuccessScenario, MeasureBeforeSuccessScenario

import logging

logger = create_logger("logger", level=logging.INFO)

# Here we add an entry into the Connection structre in Easysquid Easynetwork to give us access to load up configs
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


def setup_data_collection(scenarioA, scenarioB, collection_duration, dir_path, measure_directly):
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
    ok_ds = EGPOKSequence(name="EGP OKs", dbFile=data_file, maxSteps=collection_duration)

    # DataSequences for attempt tracking
    midpoint_attempt_ds = MHPMidpointEntanglementAttemptSequence(name="Midpoint EGP Attempts", dbFile=data_file,
                                                                 maxSteps=collection_duration)

    node_attempt_ds = MHPNodeEntanglementAttemptSequence(name="Node EGP Attempts", dbFile=data_file,
                                                         maxSteps=collection_duration)

    if measure_directly:
        # DataSequence for QubErr collection
        quberr_ds = EGPQubErrSequence(name="EGP QubErr", dbFile=data_file, maxSteps=collection_duration)
    else:
        # DataSequence for entangled state collection
        state_ds = EGPStateSequence(name="EGP Qubit States", dbFile=data_file, maxSteps=collection_duration)

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

    pm.addEvent(source=scenarioA.egp.mhp.conn, evtType=scenarioA.egp.mhp.conn._EVT_ENTANGLE_ATTEMPT,
                ds=midpoint_attempt_ds)

    pm.addEvent(source=scenarioA.egp.mhp, evtType=scenarioA.egp.mhp._EVT_ENTANGLE_ATTEMPT, ds=node_attempt_ds)

    pm.addEvent(source=scenarioB.egp.mhp, evtType=scenarioB.egp.mhp._EVT_ENTANGLE_ATTEMPT, ds=node_attempt_ds)

    if measure_directly:
        return [create_ds, ok_ds, quberr_ds, err_ds, node_attempt_ds, midpoint_attempt_ds]
    else:
        return [create_ds, ok_ds, state_ds, err_ds, node_attempt_ds, midpoint_attempt_ds]


def schedule_scenario_actions(scenarioA, scenarioB, origin_bias, create_prob, min_pairs, max_pairs, tmax_pair,
                              request_overlap, request_cycle, num_requests, max_sim_time, measure_directly, additional_data=None):
    idA = scenarioA.egp.node.nodeID
    idB = scenarioB.egp.node.nodeID

    create_time = 0
    sim_duration = 0
    added_requests = 0

    if num_requests == 0:
        num_requests = float('inf')

    if min_pairs > max_pairs:
        max_pairs = min_pairs

    if request_cycle == 0:
        # Use t_cycle of MHP for the request cycle
        request_cycle = scenarioA.egp.mhp.conn.t_cycle / SECOND

    # Check so we don't have an infinite loop
    if num_requests == 0:
        if max_sim_time == float('inf'):
            raise ValueError("Cannot have infinite number of requests and infinite simulation time")
        else:
            if request_overlap and (request_cycle == 0):
                raise ValueError("Cannot have infinite number of requests, request overlap and zero request cycle")
            else:
                if tmax_pair == 0:
                    raise ValueError("Cannot have infinite number of requests, no request overlap and zero tmax_pair")

    if additional_data:
        additional_data["request_t_cycle"] = (request_cycle * SECOND)
        additional_data["create_request_prob"] = create_prob
        additional_data["create_request_origin_bias"] = origin_bias

    while (added_requests < num_requests) and (create_time < (max_sim_time * SECOND)):

        # Randomly decide if we are creating a request this cycle
        if random() <= create_prob:
            added_requests += 1

            # Randomly select a number of pairs within the configured range
            num_pairs = randint(min_pairs, max_pairs)

            # Provision time for the request based on total number of pairs
            max_time = num_pairs * tmax_pair * SECOND

            # Randomly choose the node that will create the request
            scenario = scenarioA if random() <= origin_bias else scenarioB
            otherID = idB if scenario == scenarioA else idA
            request = EGPRequest(otherID=otherID, num_pairs=num_pairs, min_fidelity=0.2, max_time=max_time,
                                 purpose_id=1, priority=10, store=False, measure_directly=measure_directly)
            logger.debug("Scheduling request at time {}".format(create_time))
            scenario.schedule_create(request=request, t=create_time)

            # If we want overlap then the next create occurs at the specified frequency
            if request_overlap:
                create_time += request_cycle * SECOND

            # Otherwise schedule the next request once this one has already completed
            else:
                create_time += max_time

            sim_duration = create_time + max_time

        else:
            create_time += request_cycle * SECOND

    return sim_duration


def setup_network_protocols(network, alphaA=0.1, alphaB=0.1):
    # Grab the nodes and connections
    nodeA = network.get_node_by_id(0)
    nodeB = network.get_node_by_id(1)
    egp_conn = network.get_connection(nodeA, nodeB, "egp_conn")
    mhp_conn = network.get_connection(nodeA, nodeB, "mhp_conn")
    dqp_conn = network.get_connection(nodeA, nodeB, "dqp_conn")

    # Create our EGP instances and connect them
    egpA = NodeCentricEGP(nodeA)
    egpB = NodeCentricEGP(nodeB)
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


# This simulation should be run from the root QLinkLayer directory so that we can load the config
def run_simulation(results_path, config=None, origin_bias=0.5, create_prob=1, min_pairs=1, max_pairs=3, tmax_pair=2,
                   request_overlap=False, request_cycle=0, num_requests=1, max_sim_time=float('inf'),
                   max_wall_time=float('inf'), max_mhp_cycle=float('inf'), enable_pdb=False, measure_directly=False, t0=0, t_cycle=0,
                   alphaA=0.1, alphaB=0.1, save_additional_data=True):
    # Save additional data
    if save_additional_data:
        additional_data = {}
    else:
        additional_data = None
    # Set up the simulation
    setup_simulation()

    # Create the network
    network = setup_physical_network(config)
    # Recompute the timings of the heralded connection, depending on measure_directly
    nodeA = network.get_node_by_id(0)
    nodeB = network.get_node_by_id(1)
    mhp_conn = network.get_connection(nodeA, nodeB, "mhp_conn")
    mhp_conn.set_timings(t_cycle=t_cycle, t0=t0, measure_directly=measure_directly)
    if save_additional_data:
        additional_data["mhp_t_cycle"] = mhp_conn.t_cycle

    # Setup entanlement generation protocols
    egpA, egpB = setup_network_protocols(network, alphaA=alphaA, alphaB=alphaB)
    if save_additional_data:
        additional_data["alphaA"] = alphaA
        additional_data["alphaB"] = alphaB

    # Set up the Measure Immediately scenarios at nodes alice and bob
    if measure_directly:
        alice_scenario = MeasureBeforeSuccessScenario(egp=egpA)
        bob_scenario = MeasureBeforeSuccessScenario(egp=egpB)
    else:
        alice_scenario = MeasureAfterSuccessScenario(egp=egpA)
        bob_scenario = MeasureAfterSuccessScenario(egp=egpB)
    sim_duration = schedule_scenario_actions(alice_scenario, bob_scenario, origin_bias, create_prob, min_pairs,
                                             max_pairs, tmax_pair, request_overlap, request_cycle, num_requests, max_sim_time, measure_directly, additional_data) + 1

    sim_duration = min(max_wall_time, sim_duration)

    # Hook up data collectors to the scenarios
    collectors = setup_data_collection(alice_scenario, bob_scenario, sim_duration, results_path, measure_directly)

    # Start the simulation
    network.start()

    # Debugging
    if enable_pdb:
        pdb.set_trace()

    logger.info("Beginning simulation")
    start_time = time()

    # Start with a step size of 1 millisecond
    if max_wall_time == 0:
        max_wall_time = float('inf')
    if max_sim_time == 0:
        max_sim_time = float('inf')
    if max_mhp_cycle == 0:
        max_mhp_cycle = float('inf')
    max_sim_time = min(max_sim_time, mhp_conn.t_cycle * max_mhp_cycle / SECOND)
    timestep = min(1e3, max_sim_time * SECOND)
    print("max_sim_time: {}".format(max_sim_time))
    print("timestep: {}".format(timestep))
    print("mhp_cycle: {}".format(mhp_conn.t_cycle))

    last_time_log = time()
    try:
        # Run simulation
        while sim_time() < max_sim_time * SECOND:

            # Check wall time during this simulation step
            wall_time_sim_step_start = time()
            if timestep == float('inf') or timestep == -float('inf'):
                raise RuntimeError()
            print("timestep: {}".format(timestep))
            sim_run(duration=timestep)
            previous_timestep = timestep
            wall_time_sim_step_stop = time()
            wall_time_sim_step_duration = wall_time_sim_step_stop - wall_time_sim_step_start

            # Calculate next duration
            wall_time_left = max_wall_time - (time() - start_time)

            # Check if wall_time_sim_step_duration is zero
            if wall_time_sim_step_duration == 0:
                # Just make the timestep 10 times bigger since last duration went very quick
                timestep = timestep * 10
            else:
                timestep = timestep * (wall_time_left / wall_time_sim_step_duration)


            # Don't use a timestep that goes beyong max_sim_time
            if (sim_time() + timestep) > (max_sim_time * SECOND):
                timestep = (max_sim_time * SECOND) - sim_time()

            # Check clock once in a while
            now = time()
            logger.info("Wall clock advanced {} s during the last {} s real time. Will now advance {} s real time.".format(wall_time_sim_step_duration, previous_timestep / SECOND, timestep / SECOND))
            logger.info("Time advanced: {}/{} s real time.  {}/{} s wall time.".format(sim_time() / SECOND, max_sim_time, now - start_time, max_wall_time))
            if now - start_time > max_wall_time:
                logger.info("Max wall time reached, ending simulation.")
                break

            elif now - last_time_log > 10:
                logger.info("Current sim time: {}".format(sim_time()))
                last_time_log = now

        stop_time = time()
        logger.info("Finished simulation, took {} (s) wall time and {} (s) real time".format(stop_time - start_time,
                                                                                             sim_time() / SECOND))
    # Allow for Ctrl-C-ing out of a simulation in a manner that commits data to the databases
    except Exception:
        logger.exception("Ending simulation early!")

    finally:
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

        # Debugging
        if enable_pdb:
            pdb.set_trace()
