#!/bin/bash
export EVAL=true
./gradlew test --tests bft4pnt.test.EvaluationTest.testEval
export EVAL=false