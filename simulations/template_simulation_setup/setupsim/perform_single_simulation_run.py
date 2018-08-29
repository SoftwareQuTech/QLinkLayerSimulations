#!/usr/bin/python

"""
This script:
    Calls the method `auxilscript.sim_methods.run_simulation with the given arguments.
"""

# TODO: doing the importing makes this example simulation slow! Can/should we cache this somehow?
from easysquid.simulationinputparser import SimulationInputParser
import sys
import auxilscripts.simulation_methods as sim_methods

# get parameters
params_received_from_start_simulation = sys.argv[1:]

# pass on the parameters to the SimulationInputParser to get them
# in the correct form
sip = SimulationInputParser(params_received_from_start_simulation)

# extract the desired data from the SimulationInputParser
paramsdict = sip.inputdict
filebasename = sip.filebasename

# Run the simulation
sim_methods.run_simulation(results_path=filebasename, **paramsdict)
