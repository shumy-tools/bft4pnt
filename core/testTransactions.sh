#!/bin/bash
export PARTIES=$1
export BATCH=$2
export EVAL=true
./gradlew clean test -q --tests bft4pnt.test.EvaluationTest.testTransactions
cat eval.txt
export EVAL=false
