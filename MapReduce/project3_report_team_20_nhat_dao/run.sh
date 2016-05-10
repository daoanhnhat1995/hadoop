#!/bin/sh


#Compile java files
javac -classpath ${HADOOP_CLASSPATH} -d WeightTrend/ WeightTrend.java -Xlint
jar -cvf WeightTrend.jar -C WeightTrend/ .

#hadoop jar WeightTrend.jar WeightTrend wtInput/data.csv wtOutput/

