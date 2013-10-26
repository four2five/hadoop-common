/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class groups the fundamental classes associated with
 * reading/writing splits. The split information is divided into
 * two parts based on the consumer of the information. The two
 * parts are the split meta information, and the raw split 
 * information. The first part is consumed by the JobTracker to
 * create the tasks' locality data structures. The second part is
 * used by the maps at runtime to know what to do!
 * These pieces of information are written to two separate files.
 * The metainformation file is slurped by the JobTracker during 
 * job initialization. A map task gets the meta information during
 * the launch and it reads the raw split bytes directly from the 
 * file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobSplit {
  static final int META_SPLIT_VERSION = 1;
  static final byte[] META_SPLIT_FILE_HEADER;
  static {
    try {
      META_SPLIT_FILE_HEADER = "META-SPL".getBytes("UTF-8");
    } catch (UnsupportedEncodingException u) {
      throw new RuntimeException(u);
    }
  } 
  public static final TaskSplitMetaInfo EMPTY_TASK_SPLIT = 
    new TaskSplitMetaInfo();
  
  /**
   * This represents the meta information about the task split.
   * The main fields are 
   *     - start offset in actual split
   *     - data length that will be processed in this split
   *     - hosts on which this split is local
   */
  public static class SplitMetaInfo implements Writable {
    protected static final Log LOG = LogFactory.getLog(SplitMetaInfo.class);
    private long startOffset;
    private long inputDataLength;
    private String[] locations;
    private int[] reducersThatDependOnThisSplit;

    public SplitMetaInfo() {}
    
    public SplitMetaInfo(String[] locations, long startOffset, 
        long inputDataLength) {
      this(locations, startOffset, inputDataLength, new int[0]);
    }

    public SplitMetaInfo(String[] locations, long startOffset, 
        long inputDataLength, int[] reducersThatDependOnThisSplit) {
      this.locations = locations;
      this.startOffset = startOffset;
      this.inputDataLength = inputDataLength;
      this.reducersThatDependOnThisSplit = reducersThatDependOnThisSplit;
      if (null != this.reducersThatDependOnThisSplit ) { 
      } else { 
        this.reducersThatDependOnThisSplit = new int[0];
      }
    }
    
    public SplitMetaInfo(InputSplit split, long startOffset) throws IOException {
      try {
        this.locations = split.getLocations();
        this.inputDataLength = split.getLength();
        this.startOffset = startOffset;
        if( split.hasReducerDependencyInfo()) { 
          this.reducersThatDependOnThisSplit = reducersThatDependOnThisSplit;
          if( null == this.reducersThatDependOnThisSplit) { 
            LOG.info("reducersThatDependOnThisSplit is null. Not good");
          }
        } else  {
          this.reducersThatDependOnThisSplit = new int[0];
        }
        LOG.info("Constructor, " + this.reducersThatDependOnThisSplit.length + 
          " dependencies");
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    public String[] getLocations() {
      return locations;
    }
  
    public int[] getReducersThatDependOnThisSplit() { 
      return reducersThatDependOnThisSplit;
    }

    public long getStartOffset() {
      return startOffset;
    }
      
    public long getInputDataLength() {
      return inputDataLength;
    }
    
    public void setReducersThatDependOnThisSplit( int[] reducersThatDependOnThisSplit) { 
      this.reducersThatDependOnThisSplit = reducersThatDependOnThisSplit;
    }

    public void setInputDataLocations(String[] locations) {
      this.locations = locations;
    }
    
    public void setInputDataLength(long length) {
      this.inputDataLength = length;
    }
    
    public void readFields(DataInput in) throws IOException {
      int len = WritableUtils.readVInt(in);
      locations = new String[len];
      for (int i = 0; i < locations.length; i++) {
        locations[i] = Text.readString(in);
      }

      len = WritableUtils.readVInt(in);
      reducersThatDependOnThisSplit = new int[len];
      for (int i=0; i<reducersThatDependOnThisSplit.length; i++) { 
        reducersThatDependOnThisSplit[i] = WritableUtils.readVInt(in);
      }

      LOG.info("Read in " + reducersThatDependOnThisSplit.length + 
        " dependencies:\n");
      for( int i=0; i<reducersThatDependOnThisSplit.length; i++) { 
        LOG.info("\t" + reducersThatDependOnThisSplit[i]);
      }

      startOffset = WritableUtils.readVLong(in);
      inputDataLength = WritableUtils.readVLong(in);
    }
  
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVInt(out, locations.length);
      for (int i = 0; i < locations.length; i++) {
        Text.writeString(out, locations[i]);
      }

      WritableUtils.writeVInt(out, reducersThatDependOnThisSplit.length);
      for( int i=0; i<reducersThatDependOnThisSplit.length; i++) { 
        WritableUtils.writeVInt(out, reducersThatDependOnThisSplit[i]);
      }

      WritableUtils.writeVLong(out, startOffset);
      WritableUtils.writeVLong(out, inputDataLength);
    }
    
    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("data-size : " + inputDataLength + "\n");
      buf.append("start-offset : " + startOffset + "\n");
      buf.append("locations : " + "\n");
      for (String loc : locations) {
        buf.append("  " + loc + "\n");
      }
      buf.append("SMI reducers that depend on this split : " + "\n");
      buf.append(Arrays.toString(reducersThatDependOnThisSplit));
      return buf.toString();
    }
  }
  /**
   * This represents the meta information about the task split that the 
   * JobTracker creates
   */
  public static class TaskSplitMetaInfo {
    private TaskSplitIndex splitIndex;
    private long inputDataLength;
    private String[] locations;
    private int[] reducersThatDependOnThisSplit;

    public TaskSplitMetaInfo(){
      this.splitIndex = new TaskSplitIndex();
      this.locations = new String[0];
      this.reducersThatDependOnThisSplit = new int[0];
    }

    public TaskSplitMetaInfo(TaskSplitIndex splitIndex, String[] locations, 
        long inputDataLength) {
      this(splitIndex, locations, inputDataLength, new int[0]);
    }

    public TaskSplitMetaInfo(TaskSplitIndex splitIndex, String[] locations, 
        long inputDataLength, int[] reducersThatDependOnThisSplit) {
      this.splitIndex = splitIndex;
      this.locations = locations;
      this.inputDataLength = inputDataLength;
      this.reducersThatDependOnThisSplit = new int[0];
      if( null == reducersThatDependOnThisSplit) { 
        this.reducersThatDependOnThisSplit = new int[0];
      } else  {
        this.reducersThatDependOnThisSplit = reducersThatDependOnThisSplit;
      }
    }

    public TaskSplitMetaInfo(InputSplit split, long startOffset) 
    throws InterruptedException, IOException {
      this(new TaskSplitIndex("", startOffset), split.getLocations(), 
          split.getLength(), split.getReducerDependencyInfo());
    }
    
    public TaskSplitMetaInfo(String[] locations, long startOffset, 
        long inputDataLength) {
      this(new TaskSplitIndex("",startOffset), locations, inputDataLength);
    }
    
    public TaskSplitIndex getSplitIndex() {
      return splitIndex;
    }
    
    public String getSplitLocation() {
      return splitIndex.getSplitLocation();
    }
    public long getInputDataLength() {
      return inputDataLength;
    }
    public String[] getLocations() {
      return locations;
    }

    public int[] getReducerDependencyInfo() { 
      return reducersThatDependOnThisSplit;
    }

    public long getStartOffset() {
      return splitIndex.getStartOffset();
    }
    
    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("data-size : " + inputDataLength + "\n");
      buf.append("locations : " + "\n");
      for (String loc : locations) {
        buf.append("  " + loc + "\n");
      }
      buf.append("TSMI reducers that depend on this split : " + "\n");
      buf.append(Arrays.toString(reducersThatDependOnThisSplit));
      return buf.toString();
    }
  }
  
  /**
   * This represents the meta information about the task split that the 
   * task gets
   */
  public static class TaskSplitIndex {
    private String splitLocation;
    private long startOffset;
    public TaskSplitIndex(){
      this("", 0);
    }
    public TaskSplitIndex(String splitLocation, long startOffset) {
      this.splitLocation = splitLocation;
      this.startOffset = startOffset;
    }
    public long getStartOffset() {
      return startOffset;
    }
    public String getSplitLocation() {
      return splitLocation;
    }
    public void readFields(DataInput in) throws IOException {
      splitLocation = Text.readString(in);
      startOffset = WritableUtils.readVLong(in);
    }
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, splitLocation);
      WritableUtils.writeVLong(out, startOffset);
    }
  }
}
