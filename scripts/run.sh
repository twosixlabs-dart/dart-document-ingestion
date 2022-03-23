#!/bin/bash

WORK_DIR=$(cd $(dirname $0); pwd)
cd $WORK_DIR

echo $(pwd)

echo $JAVA_OPTS
echo $PROGRAM_ARGS

if [ -z "$JAVA_OPTS" ]; then
    echo "JAVA_OPTS not set, using defaults"
    JAVA_OPTS=$DEFAULT_JAVA_OPTS
fi

if [ -z "$PROGRAM_ARGS" ]; then
    echo 'No program arguments present'
fi

RUNNABLE_JAR=$(find . -name '*.jar')

if [ ! -z $DEBUG ]; then
    echo "launching application in debug mode"
    DEBUG_CONF="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5050"
fi

echo "Starting fat jar Java application with:"
echo "JAVA_OPTS=$JAVA_OPTS"
echo "PROGRAM_ARGS=$PROGRAM_ARGS"
java $DEBUG_CONF -noverify $JAVA_OPTS $GLOBAL_JAVA_OPTS -jar $RUNNABLE_JAR $PROGRAM_ARGS
