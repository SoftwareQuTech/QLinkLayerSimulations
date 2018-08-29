Files in directory `setupsim`:

- `perform_single_simulation_run.py` : Python file that contains the code to be run for a **single** simulation run. 

- *(optional)* directory `auxilscripts` : directory where one can put auxillary scripts that are called in `perform_single_simulation_run.py`

- `simdetails.json` : Configuration file containing the simulation parameter values that will be used in the simulation. Nonoptional parameters:

	+ a brief description of the simulation experiment
	+ the paths to the EasySquid and NetSquid directories
	+ the number of simulation runs
	+ a name of the output directory

- `paramcombinations.json` : Configuration file that contains all possible combinations of parameters as specified in `simdetails.json`. Note: in theory, these combinations can be directly computed from `simdetails.json`, but since the calling of the main python script will be done by a bash script and computing the cartesian product of an arbitrary number of lists in nontrivial, we chose to have the set of all parameter combinations computed in advance.

- *(optional)* folder `config` : folder with configuration files 

- *(optional)* `create_simdetails_and_paramcombinations.py`: auxillary file that creates the files `simdetails.json` and `paramcombinations.json` in the correct form.

- this README file.
