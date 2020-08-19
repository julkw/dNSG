#!/usr/bin/env bash

NAME=${1?Error: no log name given}

mkdir "data/tests"
mv "data/dNSG.log" "data/tests/${NAME}.log"