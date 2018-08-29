#!/bin/bash
#SBATCH -p short # requested parition (normal, short, staging, ...)
#SBATCH -t 1:00:00 # wall clock time
#SBATCH -n 24 # requested processes

# This script:
# - creates a new folder <TIMESTAMP>_<OUTPUTDIRNAME>
# - extracts information about the simulation setup and 
#   parameter values from the files `simdetails.json` 
#   and `paramcombinations.json`.
# - copies this information to the newly created folder,
#   either by directly copying files or by first putting
#   the information in human readable format
#   (to be precise: it copies:
#   	+ the folder `config`
#   	+ the file `simdetails.json`
#   	+ the file `paramcombinations.json`
#    and it produces
#   	+ the file `description_<OUTPUTDIRNAME>.md`
#   	+ the file `simulationlog_<TIMESTAMP>_<OUTPUTDIRNAME>.txt`
#  - for the number of simulation runs as specified in 
#    `simdetails.json`, and for every possible combination
#    of parameter values in the file `paramcombinations.json`,
#    the Python script `setupsim/perform_single_simulation_run.py`
#    is executed with these parameters, together with some other logging
#    data such as a timestamp and the directory where to put the 
#    data gathered during or after the single simulation run.

# Author: Axel

if [[ -z "${SIMULATION_DIR}" ]]; then
    echo "The environment variable SIMULATION_DIR must be set to the path to the simulation folder before running this script!"
    exit 1
elif ! [[ -d "${SIMULATION_DIR}" ]]; then
    echo "The environment variable SIMULATION_DIR is not a path to a folder."
    exit 1
fi

echo 'Usage : --outputdescription [y/n] --copysimdetails [y/n] --copyconfiguration [y/n] --outputlogfile [y/n] --logtoconsole [y/n]'

echo $'\nNOTE BEFOREHAND: it is advised to remain paying attention to the simulation until the message "Starting simulations..." appears in case the preparation of the simulation runs does not finish successfully\n'

# relevant files
runsimulation=setupsim/perform_single_simulation_run.py
simdetailsfile=setupsim/simdetails.ini
paramcombinationsfile=setupsim/paramcombinations.json
configdir=setupsim/config
paramsetfile=setupsim/paramset.csv
archivejobfile=readonly/archivejob.sh

# Get simulation details
# TODO Check that requiered arguments are set
. $SIMULATION_DIR/$simdetailsfile

# get the date and time as a single timestamp in ISO8601 format YYYY-MM-DDTHH:MM:SS+02:00
timestamp=$(date '+%Y-%m-%dT%H:%M:%S%Z')

# Path to the Paramterer combinations file
PARAMCOMBINATIONSPATH=$SIMULATION_DIR/$paramcombinationsfile

# Set the Python Path
# TODO should we set this here?
export PYTHONPATH=$PYTHONPATH:$SIMULATION_DIR


##################
# Read arguments #
##################

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
	-cs|--copysimdetails)
	COPYSIMDETAILS="$2"
	shift
	shift
	;;
	-od|--outputdescription)
	OUTPUTDESCR="$2"
	shift
	shift
	;;
	-cc|--copyconfiguration)
	COPYCONFIGURATION="$2"
	shift
	shift
	;;
	-ol|--outputlogfile)
	OUTPUTLOGFILE="$2"
	shift
	shift
	;;
	-lc|--logtoconsole)
	LOGTOCONSOLE="$2"
	shift
	shift
	;;
    -pr|--profiling)
	PROFILING="$2"
	shift
	shift
	;;
	-rc|--runoncluster)
	RUNONCLUSTER="$2"
	shift
	shift
	;;
	*)
	echo "Unknown argument $key"
	exit 1
esac
done

COPYSIMDETAILS=${COPYSIMDETAILS:-'y'} #if COPYSIMDETAILS is not set, it gets the default value 'y'
OUTPUTDESCR=${OUTPUTDESCR:-'y'}
COPYCONFIGURATION=${COPYCONFIGURATION:-'y'}
OUTPUTLOGFILE=${OUTPUTLOGFILE:-'y'}
LOGTOCONSOLE=${LOGTOCONSOLE:-'y'}
PROFILING=${PROFILING:-'n'}
RUNONCLUSTER=${RUNONCLUSTER:-'n'}


# logging to the console
echo $timestamp
echo $'Preparing simulation\n--------------------'

# Set the paths to the repos
# TODO should we do this here?
export PYTHONPATH=$PYTHONPATH:$EASYSQUIDDIR
export PYTHONPATH=$PYTHONPATH:$NETSQUIDDIR

#########################
# Get software versions #
#########################

echo '- Getting software versions of NetSquid and EasySquid'


EASYSQUIDHASH=$(./readonly/get_git_hash.sh -dir "$EASYSQUIDDIR")
NETSQUIDHASH=$(./readonly/get_git_hash.sh -dir "$NETSQUIDDIR")


#####################################
# Create files for logging purposes #
#####################################

resultsdir=$timestamp\_$OUTPUTDIRNAME
echo "- Creating directory $resultsdir for storing data"

# create new directory
#TODO: GIVE AN ERROR WHEN THE DIRECTORY ALREADY EXISTS
mkdir -p $resultsdir

# copy the simulation details
if [ "$COPYSIMDETAILS" = 'y' ]
then
	simdetailsdestination=$resultsdir/simdetails.json
	paramcombinationsdestination=$resultsdir/paramcombinations.json
	configdirdestination=$resultsdir/config
	echo "- Copying simulation parameters and configuration"
	cp -i $simdetailsfile $simdetailsdestination
	cp -i $paramcombinationsfile $paramcombinationsdestination
	cp -r -i $configdir $configdirdestination
fi

# create a description file with a short
# description of the simulation experiment
if [ "$OUTPUTDESCR" = 'y' ]
then
	descrfilename=$resultsdir/description\_$OUTPUTDIRNAME.md
	echo "- Writing description file $descrfilename"

	# create the file
	touch $descrfilename

	# write to the file
	echo $timestamp >> $descrfilename
	echo $'\n\nNumber of runs:' >> $descrfilename
	echo $NUMRUNS >> $descrfilename
	echo $'\nSimulation experiment description\n---------------------------------\n' >> $descrfilename
	echo $DESCRIPTION >> $descrfilename
	echo $'\n\nSoftware version\n----------------\nNetSQUID:' >> $descrfilename
	echo $NETSQUIDHASH >> $descrfilename
	echo $'\nEasySquid:' >> $descrfilename
	echo $EASYSQUIDHASH >> $descrfilename
	echo $'\n\nParameter choices\n-----------------\n' >> $descrfilename
	echo $OPTPARAMS >> $descrfilename

fi


# create logfile
if [ "$OUTPUTLOGFILE" = 'y' ]
then
	logfiledestination=$resultsdir/simulationlog\_$timestamp\_$OUTPUTDIRNAME.txt
	echo $'Start time:' >> $logfiledestination
	echo $(date '+%Y-%m-%dT%H:%M:%S%Z') >> $logfiledestination
	echo $'\n\nLog of simulating: ' >> $logfiledestination
	echo $OUTPUTDIRNAME >> $logfiledestination
	echo $'\n-----------------------------\n' >> $logfiledestination
fi

#####################
# Prepare simulation
#####################

if [ "$RUNONCLUSTER" == 'y' ] ; then
    # load modules
    module load stopos
    module load python/3.6-intel-2018-u2

    # Make a temporary folder
    TMP_DIR=`mktemp -d`

    # Setup the stopos pool
    export STOPOS_POOL=pool_simulation
    stopos create
    stopos add $SIMULATION_DIR/$paramsetfile
else
    TMP_DIR=$resultsdir
fi

####################################
# Starting the simulations
####################################

echo $'\nStarting simulations...\n'

if [ "$RUNONCLUSTER" == 'y' ]; then
    # Get job ID
    if [ "${SLURM_ARRAY_JOB_ID}" != "" ]; then
        jobname="${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
    else
        jobname=${SLURM_JOB_ID}
    fi

    # Schedule moving results files after simulation
    sbatch --dependency=afterany:$SLURM_JOB_ID $archivejobfile $jobname $(readlink -f $TMP_DIR) $resultsdir

    # Get the number of cores
    nrcores=`sara-get-num-cores`
    processes=nrcores
else
    processes=1
fi

counter=0
for ((i=1; i<=processes; i++)); do
(
    while true; do
        ((counter=counter+1))
        # Check if there are more parameters to simulate
        if [ "$RUNONCLUSTER" == 'y' ]; then
            # Move to the first item in the pool
            stopos next &> /dev/null

            # Check if there are more items in the pool
            if [ "$STOPOS_RC" != "OK" ]; then
                break
            fi

            # Get the next parameters from the pool
            params=( $STOPOS_VALUE )
        else
            # Get the next line of parameters
            line=$(sed "${counter}q;d" $paramsetfile)
            if [[ -z "$line" ]]; then
                break
            fi
            params=($line)
        fi

        # Extract the parameters
        actual_key=${params[0]} # Key to parameter set in paramcombinations-file
        runindex=${params[1]} # Run index

        # logging
        logstr="$(date '+%Y-%m-%dT%H:%M:%S%Z') Running simulation key=$actual_key, run=$runindex"


        # logging to the console
        if [ "$LOGTOCONSOLE" = 'y' ]
        then
            echo $'\n'
            echo $logstr
        fi

        # logging to the logfile
        if [ "$OUTPUTLOGFILE" = 'y' ]
        then
            echo $logstr >> $logfiledestination
            echo $'\n' >> $logfiledestination
        fi

        # Schedule the simulation
        if [ "$PROFILING" == 'y' ]; then
            profile_file="$resultsdir"/"$timestamp"_key_"$actual_key"_run_"$runindex".prof
            python3 -m cProfile -o $profile_file $runsimulation $timestamp $TMP_DIR $runindex $PARAMCOMBINATIONSPATH $actual_key
        else
            python3 $runsimulation $timestamp $TMP_DIR $runindex $PARAMCOMBINATIONSPATH $actual_key
        fi

        if [ "$RUNONCLUSTER" == 'y' ]; then
            # Remove the parameters from the pool
            stopos remove -p $STOPOS_POOL
        fi
    done
) &
done
wait