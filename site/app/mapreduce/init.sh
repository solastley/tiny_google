#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR
rm -rf /tmp/tiny_google_temp/
rm -rf /tmp/tiny_google_output/

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main initialize.java
jar cf init.jar initialize*.class
$HADOOP_HOME/bin/hadoop jar init.jar initialize /tmp/tiny_google_input /tmp/tiny_google_temp /tmp/tiny_google_output
