#!/bin/bash

# This script gets the git hash address 
# from the directory given as argument
# and prints it to the console
#
# July 2018

# Usage: ./get_git_hash.sh -dir path/to/git/directory

#reading arguments
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -dir|--dir)
    GITDIR="$2"
    shift # past argument
    shift # past value
    ;;
esac
done

#obtaining the git hashes

#go to directory
cd "$(echo $GITDIR)"
GITHASH=$(git rev-parse HEAD)
echo $GITHASH
