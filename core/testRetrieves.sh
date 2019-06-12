#!/bin/bash
export PARTIES=$1
export SIZE=$2
export EVAL=true
./gradlew clean test -q --tests bft4pnt.test.ClientTest.testClientRetrieveTimes
cat eval.txt
export EVAL=false
