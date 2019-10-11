#!/bin/bash

cd kafka-flink-101
mvn clean install
mvn exec:java -Dexec.mainClass=com.inatel.demos.GearChange -Dexec.args="$1"
