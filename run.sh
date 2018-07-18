#!/bin/bash

GRAPH_FOLDER=/home/aprat/projects/oracle/repository/$1
JAR_FILE=/home/aprat/projects/BTERonHPlus/target/bteronhplus-0.0.3-jar-with-dependencies.jar

hadoop fs -mkdir /user/
hadoop fs -mkdir /user/root
hadoop fs -rm -r /user/root/*

hadoop fs -copyFromLocal $GRAPH_FOLDER/un-original-communities /user/root/
hadoop fs -copyFromLocal $GRAPH_FOLDER/un-original-communities-1 /user/root/
hadoop fs -copyFromLocal $GRAPH_FOLDER/un-original-communities-1.children /user/root/

NUM_NODES=$(wc -l ${GRAPH_FOLDER}/un-original-degrees | cut -d " " -f 1)
NUM_NODES=10000000

echo "Generating $1 graph with $NUM_NODES nodes"

hadoop jar $JAR_FILE \
-c hdfs:///user/root/un-original-communities \
-o ./output \
-s $NUM_NODES \
-b hdfs:///user/root/un-original-communities- \
-l 4 \
-t 2

rm ./part-r-*
hadoop fs -copyToLocal /user/root/output/part-r-* .
cat part-r-* > $GRAPH_FOLDER/generated-plus.el
