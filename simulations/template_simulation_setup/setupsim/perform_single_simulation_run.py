#!/usr/bin/python

"""
This script:
    - passes on its arguments to SimulationInputParser, which 
      returns a filename (a "basename", i.e. without extension) 
      and a dictionary of parameters.
    - passes this dictionary of parameters on to the auxillary
      script `myzoo.generate_zoolist`, which returns a list of strings
    - stores this list of strings in an HDF5-file with as filename (an 
      extended string of) the filename outputted by the SimulationInputParser.
"""


# TODO: doing the importing makes this example simulation slow! Can/should we cache this somehow?
from easysquid.simulationinputparser import SimulationInputParser
import sys
import auxilscripts.simulation_methods as sim_methods

param_types = {"config":str,
               "origin_bias": float,
               "create_prob": float,
               "min_pairs": int,
               "max_pairs": int,
               "tmax_pair": float,
               "request_overlap": bool,
               "request_freq": float,
               "num_requests": int,
               "max_sim_time": float,
               "max_wall_time": float,
               "enable_pdb": bool}

# get parameters
params_received_from_start_simulation = sys.argv[1:]

# pass on the parameters to the SimulationInputParser to get them
# in the correct form
sip = SimulationInputParser(params_received_from_start_simulation, parse_numerical_value=False)

# extract the desired data from the SimulationInputParser
paramsdict = sip.inputdict
filebasename = sip.filebasename

# Convert parameters to correcty type
for (paramName, param) in paramsdict.items():
    paramsdict[paramName]=param_types[paramName](param)

print("paramsdict: {}".format(paramsdict))
print("filebasename: {}".format(filebasename))

# Run the simulation
sim_methods.run_simulation(results_path=filebasename, **paramsdict)