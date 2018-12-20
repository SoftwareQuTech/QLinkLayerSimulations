#!/bin/bash

##################
# Read arguments #
##################

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

#logfiledestination

case $key in
    -rd|--resultsdir)
    resultsdir="$2"
    shift
    shift
    ;;
    -td|--tmpdir)
    TMP_DIR="$2"
    shift
    shift
    ;;
    -sd|--simulation_dir)
    SIMULATION_DIR="$2"
    shift
    shift
    ;;
    -ts|--timestamp)
    timestamp="$2"
    shift
    shift
    ;;
	-lf|--logfiledestination)
	logfiledestination="$2"
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
    -p)
    PARTITION="$2"
    shift
    shift
    ;;
	*)
	echo "Unknown argument $key"
	exit 1
esac
done

OUTPUTLOGFILE=${OUTPUTLOGFILE:-'y'}
LOGTOCONSOLE=${LOGTOCONSOLE:-'y'}
PROFILING=${PROFILING:-'n'}
RUNONCLUSTER=${RUNONCLUSTER:-'n'}
POST_PROC=${POST_PROC:-'n'}

#####################
# Prepare simulation
#####################

# relevant files
runsimulation="$SIMULATION_DIR"/setupsim/perform_single_simulation_run.py
simdetailsfile="$resultsdir"/simdetails.ini
paramcombinationsfile="$resultsdir"/paramcombinations.json
paramsetfile="$resultsdir"/paramset.csv
archivejobfile="$SIMULATION_DIR"/readonly/archivejob.sh

# Get simulation details
# TODO Check that requiered arguments are set
. "$simdetailsfile"


####################################
# Starting the simulations
####################################

echo $'\nStarting simulations...\n'

if [ "$RUNONCLUSTER" == 'y' ]; then
    stopos_pool=pool_simulation_${SLURM_JOB_ID}
fi

counter=0
while true; do
    ((counter=counter+1))
    # Check if there are more parameters to simulate
    if [ "$RUNONCLUSTER" == 'y' ]; then
        # Move to the first item in the pool
        stopos next -q -p $stopos_pool

        # Check if there are more items in the pool
        if [ "$STOPOS_RC" != "OK" ]; then
            break
        fi

        echo "got next item ${STOPOS_VALUE} from stopos pool ${stopos_pool}"

        # Get the next parameters from the pool
        params=( $STOPOS_VALUE )
    else
        # Get the next line of parameters
        line=$(sed "${counter}q;d" "$paramsetfile")
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
        echo $logstr >> "$logfiledestination"
        echo $'\n' >> "$logfiledestination"
    fi

    # Schedule the simulation
    if [ "$PROFILING" == 'y' ]; then
        profile_file="${resultsdir}/${timestamp}_key_${actual_key}_run_${runindex}.prof"
        python3 -m cProfile -o $profile_file $runsimulation $timestamp $TMP_DIR $runindex $paramcombinationsfile $actual_key
    else
        python3 $runsimulation $timestamp $TMP_DIR $runindex $paramcombinationsfile $actual_key
    fi

    if [ "$RUNONCLUSTER" == 'y' ]; then
        # Remove the parameters from the pool
        stopos remove -p $stopos_pool
    fi
done
wait
