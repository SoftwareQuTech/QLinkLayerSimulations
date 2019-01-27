#!/usr/bin/python

"""
This script:
    Calls the method `simulations.simulation_methods.run_simulation with the given arguments.
"""

# TODO: doing the importing makes this example simulation slow! Can/should we cache this somehow?
import sys
import os

from easysquid.simulationinputparser import SimulationInputParser
from simulations.simulation_methods import run_simulation


def main(params_received_from_start_simulation):

    # pass on the parameters to the SimulationInputParser to get them
    # in the correct form
    sip = SimulationInputParser(params_received_from_start_simulation)

    # extract the desired data from the SimulationInputParser
    paramsdict = sip.inputdict
    filebasename = sip.filebasename
    simulation_key = sip._key_in_paramcombinations

    # Get path to simulation folder
    path_to_here = os.path.dirname(os.path.abspath(__file__))
    sim_dir = "/".join(path_to_here.split("/")[:-1]) + "/"

    # Run the simulation
    run_simulation(results_path=filebasename, sim_dir=sim_dir, name=simulation_key, **paramsdict)


if __name__ == '__main__':
    # get parameters
    params_received_from_start_simulation = sys.argv[1:]
    main(params_received_from_start_simulation)
