#!/bin/bash

function make_command_with_args
{
    string="$HADOOP_HOME/bin/hadoop jar search.jar search /tmp/tiny_google_index /tmp/tiny_google_output"
    for a in "$@" # Loop over arguments
    do
        if [[ "${a:0:1}" != "-" ]] # Ignore flags (first character is -)
        then
            string+=" " # Delimeter
            string+="$a"
        fi
    done
    echo "$string"
}

# get directory of bash script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# remove map reduce files from previous run of job
rm -rf /tmp/tiny_google_output/
rm /tmp/tiny_google_results/results.txt

# run map reduce job
cd $DIR
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main search.java
jar cf search.jar search*.class
command="$(make_command_with_args "$@")"
eval $command
