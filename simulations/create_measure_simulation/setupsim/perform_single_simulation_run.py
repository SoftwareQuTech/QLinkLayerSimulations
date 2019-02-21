#!/usr/bin/python

"""
This script:
    Calls the method `simulations.simulation_methods.run_simulation with the given arguments.
"""

import os

from easysquid.simulations import single_simulation_run_methods
from simulations.simulation_methods import run_simulation


def main(final_results_dir, tmp_results_dir, timestamp, run_key, run_index, paramcombinations_file):
    sim_param = single_simulation_run_methods.SimulationParameters(final_results_dir=final_results_dir,
                                                                   tmp_results_dir=tmp_results_dir, timestamp=timestamp,
                                                                   run_key=run_key, run_index=run_index,
                                                                   paramcombinations_file=paramcombinations_file)

    # extract the desired data from the SimulationInputParser
    paramsdict = sim_param.paramsdict
    tmp_filebasename = sim_param.tmp_filebasename
    final_filebasename = sim_param.final_filebasename
    simulation_key = sim_param.run_key

    # Get path to simulation folder
    path_to_here = os.path.dirname(os.path.abspath(__file__))
    sim_dir = "/".join(path_to_here.split("/")[:-1]) + "/"

    # Run the simulation
    run_simulation(tmp_filebasename=tmp_filebasename, final_filebasename=final_filebasename, sim_dir=sim_dir,
                   name=simulation_key, **paramsdict)


if __name__ == '__main__':
    args = single_simulation_run_methods.parse_args()
    main(**vars(args))
