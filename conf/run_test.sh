hdfs dfs -rm -r /wordcount_output/
#hadoop jar ~/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar damascwordcount -D mapreduce.dependency_scheduling=true -D mapreduce.dependency_scheduling=true /text_input /text_output
#hadoop jar ~/install/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar wordcount -D mapreduce.dependency_scheduling=true /wordcount_input /wordcount_output
hadoop jar ~/install/hadoop-2.2.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar wordcount -D org.apache.hadoop.mapreduce.v2.app.MRAppMasterName=org.apache.hadoop.mapreduce.v2.app.FakeMRAppMaster /wordcount_input /wordcount_output
