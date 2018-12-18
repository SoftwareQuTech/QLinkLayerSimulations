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
	-lai|--lastarrayindex)
	LASTARRAYINDEX="$2"
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

if [ "$RUNONCLUSTER" == 'y' ] ; then
    # load modules
    module load stopos
    module load python/3.6-intel-2018-u2

    # Make a temporary folder
    TMP_DIR=`mktemp -d`

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

        # Setup the stopos pool
        stopos_pool=pool_simulation_${SLURM_ARRAY_TASK_ID}
        stopos create -p $stopos_pool
        array_paramsetfile=${paramsetfile%.*}_${SLURM_ARRAY_TASK_ID}.csv
        echo "adding to stopos ${array_paramsetfile}"
        stopos add -p $stopos_pool $array_paramsetfile

        jobname="${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
        jobID="${SLURM_ARRAY_JOB_ID}"

        # Only schedule archiving for one job in the array
        if [ "${SLURM_ARRAY_TASK_ID}" == "${LASTARRAYINDEX}" ]; then
            # Schedule moving results files after simulation (and possibly post-processing
            sbatch --out="${resultsdir}/archiving_log.out" --dependency=afterany:$SLURM_JOB_ID $archivejobfile $SIMULATION_DIR $jobID $(readlink -f $TMP_DIR) $resultsdir $POST_PROC $timestamp $OUTPUTDIRNAME $paramsetfile $RUNONCLUSTER
        fi
    else

        # Setup the stopos pool
        stopos_pool=pool_simulation
        stopos create -p $stopos_pool
        echo "adding to stopos ${paramsetfile}"
        stopos add -p $stopos_pool $paramsetfile

        jobname=${SLURM_JOB_ID}
        jobID="${SLURM_JOB_ID}"

        # Schedule moving results files after simulation (and possibly post-processing
        sbatch --out="${resultsdir}/archiving_log.out" --dependency=afterany:$SLURM_JOB_ID $archivejobfile $SIMULATION_DIR $jobID $(readlink -f $TMP_DIR) $resultsdir $POST_PROC $timestamp $OUTPUTDIRNAME $paramsetfile $RUNONCLUSTER
    fi


    # Get the number of cores
    nrcores=`sara-get-num-cores`
    processes=$nrcores
else
    processes=1
fi

echo "processes = $processes"

counter=0
for ((i=1; i<=processes; i++)); do
(
    while true; do
        ((counter=counter+1))
        # Check if there are more parameters to simulate
        if [ "$RUNONCLUSTER" == 'y' ]; then
            # Move to the first item in the pool
            stopos next -p $stopos_pool &> /dev/null

            # Check if there are more items in the pool
            if [ "$STOPOS_RC" != "OK" ]; then
                break
            fi

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
) &
done
wait

# Check if we should do post-processing
if ! [ "$RUNONCLUSTER" == 'y' ]; then
    if [ "$POST_PROC" == 'y' ]; then
        post_proc_file="${SIMULATION_DIR}/readonly/post_processing.sh"
        $post_proc_file $SIMULATION_DIR $resultsdir $timestamp $paramsetfile $RUNONCLUSTER $OUTPUTDIRNAME
    fi
fi
