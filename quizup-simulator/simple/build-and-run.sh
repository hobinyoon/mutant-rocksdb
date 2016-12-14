#!/bin/bash

set -e
set -u

DN_THIS=`dirname $BASH_SOURCE`

printf "Building ...\n"
time make -j

printf "\n"
printf "Running ...\n"
#time LD_LIBRARY_PATH=.. valgrind ./quizup "$@"
time LD_LIBRARY_PATH=.. ./quizup "$@"
