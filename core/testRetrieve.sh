#!/bin/bash
export EVAL=true
./gradlew test --tests bft4pnt.test.ClientTest.testClientRetrieveTimes
export EVAL=false
