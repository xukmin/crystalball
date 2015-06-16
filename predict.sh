#!/bin/bash
./build.sh &&
java -cp bin/crystal.jar:$(hadoop classpath) org.xukmin.crystal.PostPredictor \
    output train_October_9_2012_clean_1.csv
