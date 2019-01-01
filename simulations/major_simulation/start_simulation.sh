#!/bin/bash
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

# Author: Axel Dahlberg

SIMULATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

##################
# Read arguments #
##################

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -h|--help)
    HELP='y'
    break
    ;;
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
	-pp|--postprocessing)
	POST_PROC="$2"
	shift
	shift
	;;
    -n)
    NRCORES="$2"
    shift
    shift
    ;;
    -t)
    MAXTIME="$2"
    shift
    shift
    ;;
    -p)
    PARTITION="$2"
    shift
    shift
    ;;
    -sd|--simulation_dir)
    SIMULATION_DIR="$2"
    shift
    shift
    ;;
	*)
	echo "Unknown argument $key"
	exit 1
esac
done

if [ "$HELP" == 'y' ]; then
    echo "This script is used to start the simulation, either locally or on the Cartesius cluster @ surfsar"
    echo "Arguments:"
    echo "  -h|--help                prints this text"
    echo "  -n                       number of parallel processes to use, default: 24 (only for surfsara)"
    echo "  -t                       max wall time, default: 1:00:00 (only for surfsara)"
    echo "  -p                       which partition to use: should be either short, normal, fat, default: short (only for surfsara)"
    echo "  -pr|--profiling          whether to profile runtime of simulation script (y/n), default: n"
    echo "  -rc|--runoncluster       whether to the simulation should be submitted to the surfsara cluster (y/n) default: n"
    echo "  -pp|--postprocessing     whether to schedule post-processing after the simulation (y/n) default: n"
    echo "  -cs|--copysimdetails     whether simdetails should be copied to results folder (y/n) default: y"
    echo "  -od|--outputdescription  whether description should be outputed (y/n) default: y"
    echo "  -cc|--copyconfiguration  whether configuration should be copied to results folder (y/n) default: y"
    echo "  -ol|--outputlogfile      whether output should be logged to file (y/n) default: y"
    echo "  -lc|--logtoconsole       whether output should be logged to console (y/n) default: y"
    exit 0
fi

COPYSIMDETAILS=${COPYSIMDETAILS:-'y'} #if COPYSIMDETAILS is not set, it gets the default value 'y'
OUTPUTDESCR=${OUTPUTDESCR:-'y'}
COPYCONFIGURATION=${COPYCONFIGURATION:-'y'}
OUTPUTLOGFILE=${OUTPUTLOGFILE:-'y'}
LOGTOCONSOLE=${LOGTOCONSOLE:-'y'}
PROFILING=${PROFILING:-'n'}
RUNONCLUSTER=${RUNONCLUSTER:-'n'}
POST_PROC=${POST_PROC:-'n'}
NRCORES=${NRCORES:-'24'}
MAXTIME=${MAXTIME:-'1:00:00'}
PARTITION=${PARTITION:-'short'}


################
# Setup things #
################

# relevant files
runsimulation="$SIMULATION_DIR"/setupsim/perform_single_simulation_run.py
simdetailsfile="$SIMULATION_DIR"/setupsim/simdetails.ini
paramcombinationsfile="$SIMULATION_DIR"/setupsim/paramcombinations.json
configdir="$SIMULATION_DIR"/setupsim/config
paramsetfile="$SIMULATION_DIR"/setupsim/paramset.csv
archivejobfile="$SIMULATION_DIR"/readonly/archivejob.sh

# Get simulation details
# TODO Check that requiered arguments are set
. "$simdetailsfile"

# get the date and time as a single timestamp in ISO8601 format YYYY-MM-DDTHH:MM:SS+02:00
timestamp=$(date '+%Y-%m-%dT%H:%M:%S%Z')

# Set the Python Path
# TODO should we set this here?
# export PYTHONPATH=$PYTHONPATH:$SIMULATION_DIR

# logging to the console
echo $timestamp
echo $'Preparing simulation\n--------------------'

# Set the paths to the repos
# TODO should we do this here? Przemek: I don't think so, you need to have it ready to have EasySquid running anyway (commented out)
# export PYTHONPATH=$PYTHONPATH:$EASYSQUIDDIR
# export PYTHONPATH=$PYTHONPATH:$NETSQUIDDIR
# export PYTHONPATH=$PYTHONPATH:$QLINKLAYERDIR

#########################
# Get software versions #
#########################

echo '- Getting software versions of NetSquid and EasySquid'

EASYSQUIDHASH="$("$SIMULATION_DIR"/readonly/get_git_hash.sh -dir "$EASYSQUIDDIR")"
NETSQUIDHASH="$("$SIMULATION_DIR"/readonly/get_git_hash.sh -dir "$NETSQUIDDIR")"
QLINKLAYERHASH=$("$SIMULATION_DIR"/readonly/get_git_hash.sh -dir "$QLINKLAYERDIR")
SIMULAQRONHASH=$("$SIMULATION_DIR"/readonly/get_git_hash.sh -dir "$SIMULAQRONDIR")

#####################################
# Create files for logging purposes #
#####################################

resultsdir="$SIMULATION_DIR"/$timestamp\_$OUTPUTDIRNAME
echo "- Creating directory $resultsdir for storing data"

# create new directory
#TODO: GIVE AN ERROR WHEN THE DIRECTORY ALREADY EXISTS
mkdir -p "$resultsdir"

# copy the simulation details
if [ "$COPYSIMDETAILS" = 'y' ]
then
	simdetailsdestination=$resultsdir/simdetails.ini
	paramcombinationsdestination=$resultsdir/paramcombinations.json
	paramsetdestination=$resultsdir/paramset.csv
	configdirdestination=$resultsdir/config
	echo "- Copying simulation parameters and configuration"
	cp -i "$simdetailsfile" "$simdetailsdestination"
	cp -i "$paramcombinationsfile" "$paramcombinationsdestination"
	cp -i "$paramsetfile" "$paramsetdestination"
	cp -r -i "$configdir" "$configdirdestination"
fi

# create a description file with a short
# description of the simulation experiment
if [ "$OUTPUTDESCR" = 'y' ]
then
	descrfilename=$resultsdir/description\_$OUTPUTDIRNAME.md
	echo "- Writing description file $descrfilename"

	# create the file
	touch "$descrfilename"

	# write to the file
	echo "$timestamp" >> "$descrfilename"
	echo $'\n\nNumber of runs:' >> "$descrfilename"
	echo "$NUMRUNS" >> "$descrfilename"
	echo $'\nSimulation experiment description\n---------------------------------\n' >> "$descrfilename"
	echo $DESCRIPTION >> "$descrfilename"
	echo $'\n\nSoftware version\n----------------\nNetSQUID:' >> "$descrfilename"
	echo $NETSQUIDHASH >> "$descrfilename"
	echo $'\nEasySquid:' >> "$descrfilename"
	echo $EASYSQUIDHASH >> "$descrfilename"
	echo $'\nQLinkLayer:' >> $descrfilename
	echo $QLINKLAYERHASH >> $descrfilename
	echo $'\nSimulaQron:' >> $descrfilename
	echo $SIMULAQRONHASH >> $descrfilename
	echo $'\n\nParameter choices\n-----------------\n' >> "$descrfilename"
	echo $OPTPARAMS >> "$descrfilename"

fi


# create logfile
if [ "$OUTPUTLOGFILE" = 'y' ]
then
	logfiledestination="$resultsdir"/simulationlog\_$timestamp\_$OUTPUTDIRNAME.txt
	echo $'Start time:' >> "$logfiledestination"
	echo $(date '+%Y-%m-%dT%H:%M:%S%Z') >> "$logfiledestination"
	echo $'\n\nLog of simulating: ' >> "$logfiledestination"
	echo $OUTPUTDIRNAME >> "$logfiledestination"
	echo $'\n-----------------------------\n' >> "$logfiledestination"
fi

##################
# Run simulation #
##################

if [ "$RUNONCLUSTER" == 'y' ]; then

    # TODO how to get the correct number of cores per node
    # cores_per_node=24
    # if [ "$NRCORES" -gt "$cores_per_node" ]; then
    #     # Create paramset files for each job in the array
    #     num_nodes=$(python3 "${SIMULATION_DIR}/readonly/create_paramsets_for_array_job.py" --total_cores $NRCORES --cores_per_node $cores_per_node --paramsetfile $paramsetdestination)

    #     # Run the array of jobs
    #     sbatch --array=1-$num_nodes -n $cores_per_node -t $MAXTIME -p $PARTITION readonly/run_simulation.sh -rd $resultsdir -sd $SIMULATION_DIR -ts $timestamp -lf $logfiledestination -ol $OUTPUTLOGFILE -lc $LOGTOCONSOLE -pr $PROFILING -rc $RUNONCLUSTER -pp $POST_PROC -lai $num_nodes
    # else
    sbatch -n $NRCORES -t $MAXTIME -p $PARTITION readonly/parallelize_simulation.sh -rd $resultsdir -sd $SIMULATION_DIR -ts $timestamp -lf $logfiledestination -ol $OUTPUTLOGFILE -lc $LOGTOCONSOLE -pr $PROFILING -pp $POST_PROC -p $PARTITION
    # fi
else
    bash readonly/run_simulation.sh -rd $resultsdir -td $resultsdir -sd $SIMULATION_DIR -ts $timestamp -lf $logfiledestination -ol $OUTPUTLOGFILE -lc $LOGTOCONSOLE -pr $PROFILING -rc $RUNONCLUSTER -pp $POST_PROC
fi

# Check if we should do post-processing (for cluster this is done after the archiving job)
if ! [ "$RUNONCLUSTER" == 'y' ]; then
    if [ "$POST_PROC" == 'y' ]; then
        post_proc_file="${SIMULATION_DIR}/readonly/post_processing.sh"
        $post_proc_file $SIMULATION_DIR $resultsdir $timestamp $paramsetfile $RUNONCLUSTER $OUTPUTDIRNAME
    fi
fi
