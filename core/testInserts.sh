#!/bin/bash
export EVAL=true
./gradlew test --tests bft4pnt.test.EvaluationTest.testInserts
export EVAL=false