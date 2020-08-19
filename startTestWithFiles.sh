#!/usr/bin/env bash

NAME=${1?Error: no config file given}
TEST=${2?Error: no test file given}

#clean up old logs if still there
rm "data/dNSG.log"
# get configuration for this test
testFile="testConfigurations/${TEST}"
# get current nodes configuration and combine
cat $testFile "testConfigurations/files.txt" $NAME > testConfigurations.txt
#start node
java -Xmx28g -XX:+UseG1GC -Dconfig.file="testConfigurations.txt" -jar target/scala-2.13/dNSG-assembly-0.1.0-SNAPSHOT.jar
# move log
./cleanup.sh "$TEST"