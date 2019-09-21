#!/bin/bash

cd kafka-flink-101
mvn clean package
mvn exec:java -Dexec.mainClass=com.inatel.demos.GearChange
