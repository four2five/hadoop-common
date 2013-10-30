hdfs dfs -rm -r /text_output/
hadoop jar ~/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar damascwordcount -D mapreduce.dependency_scheduling=true -D mapreduce.dependency_scheduling=true /text_input /text_output
#hadoop jar ~/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar wordcount -D mapreduce.dependency_scheduling=true /text_input /text_output
