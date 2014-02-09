package org.apache.hadoop.mapred.buffer.impl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
//import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.mapred.buffer.InMemoryBufferUmbilicalProtocol;
//import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.OutputInMemoryBuffer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

public class JRecordWriter<K, V> 
  extends org.apache.hadoop.mapreduce.RecordWriter<K,V> { 


  private static final Log LOG = LogFactory.getLog(JRecordWriter.class);
  private JInMemOutputBuffer buffer;
  private org.apache.hadoop.mapreduce.Partitioner<K,V> partitioner;
  int partitions;


  public JRecordWriter(org.apache.hadoop.mapreduce.JobContext jobContext,
                       InMemoryBufferUmbilicalProtocol umbilical, Task task, JobConf job,
          Reporter reporter, Progress progress, boolean pipeline,
             Class<K> keyClass, Class<V> valClass,
             Class<? extends CompressionCodec> codecClass) throws IOException {

    this.buffer = new JInMemOutputBuffer(umbilical, task, job, reporter, progress, 
                                    pipeline, keyClass, valClass, codecClass);
    this.partitions = jobContext.getNumReduceTasks();
    try { 
      this.partitioner = (org.apache.hadoop.mapreduce.Partitioner<K,V>)
                        ReflectionUtils.newInstance(jobContext.getPartitionerClass(), job);
    } catch (ClassNotFoundException cnfe) { 
      LOG.error("cnfe: " + cnfe.toString());
    }

  }

  public void write(K key, V value) throws IOException { 
    write(key, value, 1);
  }

  public void write(K key, V value, long recordsRepresented) throws IOException { 
    this.buffer.collect(key, value, recordsRepresented, this.partitioner.getPartition(key, value, this.partitions));
  }

  public void close(TaskAttemptContext context) throws IOException, InterruptedException { 
    this.buffer.close(); // should we be calling flush() here ? TODO --jbuck
  }

  public OutputInMemoryBuffer oldClose(TaskAttemptContext context) throws IOException, InterruptedException { 
    OutputInMemoryBuffer buffer = this.buffer.oldClose();
    close(context);
    return buffer;
  }

  public synchronized void free() { 
    this.buffer.free();
  }

}
