Last updated: 8 August 2018

Welcome to the README of the simulation run template!

**NOTE BEFOREHAND:** running the simulation setup requires one to install `jq` (to be run from the console; it is **not** a Python package).

The template consists of two folders, `readonly` and `setupsim`, the shell script `start_simulation.sh` and this README text file. Below, we briefly address the use of each of these and explain how to use this template to run an EasySquid/NetSquid simulation.

Overview of the folders and files
=================================
The `start_simulation.sh` script is what should be called if one whishes to start a simulation run. It creates a new for for the simulation results to be stored in, copies the relevant information to it in order to reproduce the simulation to this folder and calls the Python scripts `setupsim/perform_single_simulation_run.py` many times for every combination of parameter values. The storage of the data is left to the script `setupsim/perform_single_simulation_run.py`.

The `setupsim` folder contains two types of files:

- files belonging to the simulation software: `setupsim/perform_single_simulation_run.py` and the folder `setupsim/auxilscripts`. The folder `auxilscripts` is optional and can be used to put auxillary Python scripts that are part of the simulation, but it may also be left empty. The file `setupsim/perform_single_simulation_run.py` is nonoptional, since it will be called by `start_simulation.sh` many times, each time with a different set of prespecified parameters. That is: a *single* simulation run consists of calling this function *once*. The idea behind this template is thus that the user adjusts `perform_single_simulation_run.py` depending on what they want to simulate.
- configuration files, which contain, for example, the number of simulation runs and parameter values that are used in the simulation. To be precise:
	+ the file `simdetails.json` (name should not be changed!) contains the parameters values, (called `opt_params`) and a bunch of nonoptional parameters (`general_params`), among which: the paths to the easysquid and netsquid directories, a description of the simulation, the total number of runs, a name for the simulation, and names and descriptions for the (HDF5)-files that the simulation outputs.
	+ the file `paramcombinations.json`, which contains all possible combinations of the parameters specified in `simdetails.json`. See `setupsim/README.md` for more information.
	+ a python script `create_simdetails_and_paramcombinations.json` that can be used to generate a `simdetails.json` and a `paramcombinations.json` file. This script is not called by any other program and thus solely serves the purpose of generating these two configuration files.
	+ a `config` folder, where the user may put as many config files as they like in order to use in the simulation (example: the user can choose to have the configuration values in these files read in `perform_single_simulation_run.py`).

The `readonly` folder contains scripts which are not meant to be changed. The folder holds auxillary scripts that are called by `start_simulation.sh`.

Running a simulation
======================
In line with the overview above, setting up and running a simulation requires one to perform a number of steps:

- adjust the script `setupsim/perform_single_simulation_run.py`
- adjust the files `setupsim/simdetails.json` and `paramcombinations.json`. The former should have at least the nonoptional parameters specified above. These files are most easily created using the script `setupsim/create_simdetails_and_paramcombinations.py`
- optional: add python programs in the folder `setupsim/auxilscripts`
- optional: add configuration files in the folder `setupsim/config`

Performing the desired number of simulation runs (as specified in `setupsim/simdetails.json` is then triggered by calling
```
./start_simulation.sh
```
In case one prefers to have less output than by default, one can use the flags. For example:
```
./start_simulation.sh --outputdescription n --copysimdetails y --copyconfiguration n --ouputlogfile n --logtoconsole n
```

Example template
================
The example template contains a simulation setup that can be run in order to get familiar with this template. The simulation data consists of a list of strings of the animals "lion", "tiger" and "wildebeast" which are supposed to live in the same area in a zoo. The number of lions and tigers in the zoo are parameters, and so is the number of wildebeasts, although the latter need not be an integer due to the fact that the two other species are predators.

The file `setupsim/simdetails.json` contains some general, nonoptional data:
```
    "general_params": {
        "easysquid_directory": "path/to/EasySquid/",
        "netsquid_directory": "path/to/NetSQUID/",
        "description": "A brief simulation test run",
        "number_of_runs": 3,
        "outputdirname": "zoo"
```
as well as several different possible numbers of animals:
```
    "opt_params": {
        "number_of_lions": 4,
        "number_of_tigers": [
            2,
            3
        ],
        "number_of_remaining_wildebeasts": [
            5.0,
            3.3,
            2.5
        ]
	}
```
The script `start_simulation.sh` will call `setupsim/perform_single_simulation_run.py` with as parameters all different 6 possible combinations of (number of lions, number of tigers, number of remaining wildebeasts), i.e.:

- 4, 2, 5.0
- 4, 2, 3.3
- 4, 2, 2.5
- 4, 3, 5.0
- 4, 3, 3.3
- 4, 3, 2.5

On top of this, the number of runs indicated in `simdetails.json` is 3, so there will be 6x3 simulations run in total: 3 runs for each possible combination of parameters.

When calling `start_simulation.sh`, some logging data will be printed to the console, and you will find a newly created folder `TIMESTAMP_zoo` which contains:

- a copy of the information provided in `simdetails.json` in human-readable form (in `description_zoo.md`)
- a log of the simulation (`simulationlog_TIMESTAMP_zoo.txt`)
- a copy of `simdetails.json`, `paramcombinations.json` and the `config` folder, which can be used to replicate the experiment
- the data produced during the simulation, to be found in the HDF5-files that were produced by the Python script `setupsim/perform_single_simulation_run.py` 

Some brief notes on the script `setupsim/perform_single_simulation_run.py`
--------------------------------------------------------------------------
The parameters with which `start_simulation.sh` calls `setupsim/perform_single_simulation_run.py` are not in a form that is directly interpretable by a Python script. For this reason, we use an object of class `easysquid.simulationinputparser.SimulationInputParser` in order to get these parameters in "Python-form" first. From this object, we extract:

- the parameter values in the form of a dictionary, which can then directly be used.
- the "basename" of the output file, which is in the form `<TIMESTAMP>_<PARAMETERNAMESANDVALUES>_run<RUNINDEX>`. Note the absence of a file extension; the idea behind this is that the user adjust this basename to whatever they want and then uses their favourite means to store data to a file with this name. In the zoo example, we append this basename by `_zoolist` and made it into an HDF5-file.
