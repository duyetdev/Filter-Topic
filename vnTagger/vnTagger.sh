#!/bin/sh

# The main program to run
PROGRAM=vn.hus.nlp.tagger-4.2.0.jar

DIR=$( dirname $0 )

# Get the java command
#
if [ -z "$JAVACMD" ] ; then 
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then 
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java 2> /dev/null`
    if [ -z "$JAVACMD" ] ; then 
      JAVACMD=java
    fi
  fi
fi

# Run the programme
#

export PATH=$DIR:$PATH

$JAVACMD -mx900m -classpath $DIR -jar $DIR/$PROGRAM $@



