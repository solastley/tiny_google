#!/bin/bash

# get directory of bash script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# remove map reduce files from previous run of job
rm -rf /tmp/tiny_google_temp/
rm -rf /tmp/tiny_google_output/
rm /tmp/tiny_google_index/index.txt
rm /tmp/tiny_google_tempfiles/temp.txt

# run map reduce job
cd $DIR
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main initialize.java
jar cf init.jar initialize*.class
$HADOOP_HOME/bin/hadoop jar init.jar initialize /tmp/tiny_google_input /tmp/tiny_google_temp /tmp/tiny_google_output
