#!/usr/bin/env bash

NAME=${1?Error: no folder name given}

mkdir "data/tests"
mkdir "data/tests/$NAME"
mv "data/aknng.graph" "data/tests/$NAME/akkng.graph"
mv "data/nsg.graph" "data/tests/$NAME/nsg.graph"
mv "data/dNSG.log" "data/tests/$NAME/dNSG.log"