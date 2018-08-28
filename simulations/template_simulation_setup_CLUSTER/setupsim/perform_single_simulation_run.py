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
from auxilscripts.myzoo import generate_zoolist
from easysquid.puppetMaster import HDF5Multi


# get parameters
params_received_from_start_simulation = sys.argv[1:]

# pass on the parameters to the SimulationInputParser to get them 
# in the correct form
sip = SimulationInputParser(params_received_from_start_simulation)

# extract the desired data from the SimulationInputParser
paramsdict = sip.inputdict
filebasename = sip.filebasename

# use an auxillary function to generate a list of animals
zoolist = generate_zoolist(**paramsdict)

# store the list of animals in an HDF5-file, using the filename
# obtained from the input parameters (this is recommended)
description = "This zoo consists of lions, tigers and wildebeasts within the same area."
hdf5storage = HDF5Multi(filename=filebasename + "_zoolist",
                        datasetname="My Zoo",
                        description=description,
                        datatype="S20")  
                # Note: 'S20' is the string dataformat where all strings 
                # consist of 20 characters. If it is given a string 
                # of fewer characters, it pads it with `\000`.

for animal in zoolist:
    hdf5storage.append(animal)

hdf5storage.close()
