#!/usr/bin/env bash

NAME=${1?Error: no config file given}

java -Xmx32g -Dconfig.file="$NAME" -jar target/scala-2.13/dNSG-assembly-0.1.0-SNAPSHOT.jar
