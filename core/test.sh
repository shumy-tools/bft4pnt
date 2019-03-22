#!/bin/bash
java -jar -Dlogback.configurationFile=logback.xml ./build/libs/bft4pnt-0.1.0.jar "$@"
