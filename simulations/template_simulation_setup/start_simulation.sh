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

# Author: Tim

# check if `jq` has been installed
if ! command -v jq 2>/dev/null; then
	echo "Please install the program \`jq\`"
	exit 1
fi


echo 'Usage : --outputdescription [y/n] --copysimdetails [y/n] --copyconfiguration [y/n] --outputlogfile [y/n] --logtoconsole [y/n]'

echo $'\nNOTE BEFOREHAND: it is advised to remain paying attention to the simulation until the message "Starting simulations..." appears in case the preparation of the simulation runs does not finish successfully\n'


# relevant files
runsimulation=setupsim/perform_single_simulation_run.py
paramsfile=setupsim/paramcombinations.json
simdetailsfile=setupsim/simdetails.json
paramcombinationsfile=setupsim/paramcombinations.json
configdir=setupsim/config



# get the date and time as a single timestamp in ISO8601 format YYYY-MM-DDTHH:MM:SS+02:00
timestamp=$(date '+%Y-%m-%dT%H:%M:%S%z')

#get the directory that this script is located in
DIR=$( cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd)

# Path to the Paramterer combinations file
PARAMCOMBINATIONSPATH=$(echo $DIR/$paramcombinationsfile)

# logging to the console
echo $timestamp
echo $'Preparing simulation\n--------------------'

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
esac
done

COPYSIMDETAILS=${COPYSIMDETAILS:-'y'} #if COPYSIMDETAILS is not set, it gets the default value 'y'
OUTPUTDESCR=${OUTPUTDESCR:-'y'}
COPYCONFIGURATION=${COPYCONFIGURATION:-'y'}
OUTPUTLOGFILE=${OUTPUTLOGFILE:-'y'}
LOGTOCONSOLE=${LOGTOCONSOLE:-'y'}



############################################
# Extracting simulation details/parameters #
############################################



EASYSQUIDDIR=$(jq .general_params.easysquid_directory $simdetailsfile)
EASYSQUIDDIR="${EASYSQUIDDIR%\"}"
EASYSQUIDDIR="${EASYSQUIDDIR#\"}"

NETSQUIDDIR=$(jq .general_params.netsquid_directory $simdetailsfile)
NETSQUIDDIR="${NETSQUIDDIR%\"}"
NETSQUIDDIR="${NETSQUIDDIR#\"}"

DESCRIPTION=$(jq .general_params.description $simdetailsfile)
NUMRUNS=$(jq .general_params.number_of_runs $simdetailsfile)

OUTPUTDIRNAME=$(jq .general_params.outputdirname $simdetailsfile)
OUTPUTDIRNAME="${OUTPUTDIRNAME%\"}"
OUTPUTDIRNAME="${OUTPUTDIRNAME#\"}"

OUTPUTFILESNAMESDESCR=$(jq .general_params.outputfilesnamedescr $simdetailsfile)

OPTPARAMS=$(jq .opt_params $simdetailsfile)


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
	echo $(date '+%Y-%m-%dT%H:%M:%S%z') >> $logfiledestination
	echo $'\n\nLog of simulating: ' >> $logfiledestination
	echo $OUTPUTDIRNAME >> $logfiledestination
	echo $'\n-----------------------------\n' >> $logfiledestination
fi

####################################
# Starting the simulations
####################################

echo $'\nStarting simulations...\n'

keys=$(jq 'keys[]' $paramsfile)
numberofsimulations=0
for key in ${keys[@]}
do
	numberofsimulations=$((numberofsimulations+1))
done
totnumberofsimulations=$(($NUMRUNS * $numberofsimulations))
totnumberofsimulationsminusone=$(($totnumberofsimulations - 1))

for runindex in $(seq 0 $((NUMRUNS-1)))
do
	counter=0
	for key in ${keys[@]}
	do
		paramsvector=$(jq '.['$key']' $paramsfile)
		paramsvector=$(echo $paramsvector | tr -d ':{},')
	
		# logging
		currentindex=$(($runindex * $numberofsimulations + $counter))
		logstr="$(date '+%Y-%m-%dT%H:%M:%S%z') Running simulation #$runindex x $numberofsimulations + $counter = $currentindex/$totnumberofsimulationsminusone with parameters:"


		# logging to the console
		if [ "$LOGTOCONSOLE" = 'y' ]
		then
			echo $'\n'
			echo $logstr
			echo $'\t'$paramsvector
		fi
		
		# logging to the logfile
		if [ "$OUTPUTLOGFILE" = 'y' ]
		then
			echo $logstr >> $logfiledestination
			echo $paramsvector >> $logfiledestination
			echo $'\n' >> $logfiledestination
		fi
	
		counter=$((counter+1))

	    # get the key without "-characters
	    actual_key=${key:1:${#key}-2}

		# CORE OF THIS SCRIPT: running the python script with the input parameters
		python3 $runsimulation $timestamp $resultsdir $runindex $PARAMCOMBINATIONSPATH $actual_key
	

	done
done

