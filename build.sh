#!/bin/bash

javac \
    -classpath "$(hadoop classpath)" \
    -sourcepath src \
    -d bin \
    src/org/xukmin/crystal/*.java &&
jar cf bin/crystal.jar -C bin org
