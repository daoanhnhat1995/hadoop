#### Map Reduce project
- Hadoop version 2.7.2 Single Node
- OS: MacOS El Capitan

##### Build: run.sh
- Note: need to export HADOOP_CLASSPATH=$(hadoop classpath) 
 
#### Run:


```
  bin/hadoop -mkdir wtInput
  bin/hadoop -put ${your_data_local} wtInput/
  bin/hadoop jar WeightTrend.jar WeightTrend ${your_input_file_on_hdfs} ${your_output_on_hdfs}

```
 see result:
 ```
  bin/hadoop -tail ${your_output_on_hdfs}
 ```
 copy to local:
 ```
   bin/hadoop -copyToLocal ${your_output_on_hdfs} ${your_target_on_local}
  ``` 
 

