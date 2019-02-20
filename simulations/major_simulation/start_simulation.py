#!/usr/bin/env python3

import os

import netsquid
import easysquid
import qlinklayer
import simulaqron
from easysquid.simulations import start_simulation

if __name__ == '__main__':

    # Get the name of the directory where this script is stored
    sim_dir = os.path.abspath(os.path.dirname(__file__))

    # Parse the parameters that are provided to this script;
    # for every parameter not provided, a default value is 
    # used (see `start_simulation.parse_args` for more info).
    args = start_simulation.parse_args(sim_dir=sim_dir)

    # What modules to include the git hash for
    modules = {"NetSquid": netsquid,
               "EasySquid": easysquid,
               "QLinkLayer": qlinklayer,
               "SimulaQron": simulaqron}

    # Find the relevant files and directories where
    # it is found which simulation code should be run
    # and with which parameters, and subsequently
    # run these
    start_simulation.main(**vars(args), modules=modules)
