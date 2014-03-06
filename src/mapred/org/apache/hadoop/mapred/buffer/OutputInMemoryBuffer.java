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

package org.apache.hadoop.mapred.buffer;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteBufferInputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;

public class OutputInMemoryBuffer implements Comparable<OutputInMemoryBuffer>, Writable {
	//public static enum Type {FILE, SNAPSHOT, STREAM}; // always in-memory

	public abstract static class Header implements Writable, Comparable<Header> {
		//private OutputInMemoryBuffer.Type type;

		protected TaskAttemptID owner;

		private float progress;
		
		private long compressedLength;
		
		private long decompressedLength;
		
		private boolean eof;

		public Header() {}

		//public Header(OutputInMemoryBuffer.Type type, TaskAttemptID owner, float progress, boolean eof) 
		public Header(TaskAttemptID owner, float progress, boolean eof) {
			//this.type = type;
			this.owner = owner;
			this.progress = progress;
			this.eof = eof;
		}

		@Override
		public String toString() {
			return  "InMemoryHeader: EOF? " + eof +
			       " owner " + owner +
			       " progress " + progress +
			       ". File size = " + compressedLength;
		}
		
		@Override
		public abstract int hashCode();
		
		@Override
		public abstract boolean equals(Object o);
		
		//public OutputInMemoryBuffer.Type type() { return type; }

		public TaskAttemptID owner() {  return owner;  }

		public float progress() { return this.progress; }
		
		public long compressed() { return this.compressedLength; }
		void compressed(long c) { this.compressedLength = c; }
		
		public long decompressed() { return this.decompressedLength; }
		void decompressed(long d) { this.decompressedLength = d; }
		
		public boolean eof() { return this.eof; }

		@Override
		public void readFields(DataInput in) throws IOException {
      try{
			  this.owner = new TaskAttemptID();
			  this.owner.readFields(in);

			  this.progress = in.readFloat();
			  this.compressedLength = in.readLong();
			  this.decompressedLength = in.readLong();
			  this.eof = in.readBoolean();
      } catch(EOFException eofe) { 
        LOG.error("Caught eofe: " + eofe.toString());
        String fullStackTrace = 
          org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(eofe);
        LOG.error(" " + fullStackTrace);
        throw eofe;
      }
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.owner.write(out);
			out.writeFloat(this.progress);
			out.writeLong(this.compressedLength);
			out.writeLong(this.decompressedLength);
			out.writeBoolean(this.eof);
		}

		public static Header readHeader(DataInput in) throws IOException {
      if (in == null) { 
        LOG.error("readHeader called with a null in");
        return null;
      }
			Header header = null;
      header = new InMemoryHeader();
			header.readFields(in);
			return header;
		}

		public static void writeHeader(DataOutput out, Header header) throws IOException {
      if (null == out) {  
        LOG.error("out is null in writeHeader");
      }
      if (null == header) { 
        LOG.error("header is null in writeHeader");
      }
			header.write(out);
		}

	}

	public static class InMemoryHeader extends Header {
		
		//private long sequence;
    private SortedSet<Integer> idlist; // may need this for merging outputs
		private String code;
		
		public InMemoryHeader() { 
      super(null, 0f, false); 
			this.idlist = new TreeSet<Integer>();
    }
		
		public InMemoryHeader(TaskAttemptID owner, float progress, boolean complete, 
                          SortedSet<Integer> idlist) {
			super(owner, progress, complete);
			//super(owner, 0f, false);
      this.idlist = idlist;
      init();
		}
		
		private void init() {
			code = owner.toString();
		  for (Integer id : this.idlist){
		   code += ":" + id;
		  }
		}
		
		public SortedSet<Integer> ids() {
	 	  return this.idlist;
		}

		@Override
		public String toString() {
			return "InMemoryHeader -- " + code + ". EOF? " + eof();
		}
		
		public int compareTo(Header header) {
		  InMemoryHeader otherHeader = (InMemoryHeader) header;
			TaskID me = owner().getTaskID();
			TaskID other = otherHeader.owner().getTaskID();
		  if (me.equals(other)) {
		 		Integer me_min = idlist.first();
		 		Integer o_min = otherHeader.ids().first();
		 		Integer me_max = idlist.last();
		 		Integer o_max = otherHeader.ids().last();
					
		 		if (me_max.compareTo(o_min) < 0) {
		 			return -1;
		 		} else if (me_min.compareTo(o_max) > 0) {
		 			return 1;
		 		} else {
		 			// Okay, basically one is a subset of the
		 			 // other. I want the superset to fall before
		 			 // the subset. 
		 			return me_min.compareTo(o_min) == 0 ? 
		 					o_max.compareTo(me_max) : 
		 						me_min.compareTo(o_min);
		 		}
			} else { // if (null != sequence)
			 	return me.compareTo(other);
      }
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof InMemoryHeader) {
				return this.compareTo((InMemoryHeader) o) == 0;
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return this.code.hashCode();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
      try{ 
			  super.readFields(in);
			  int length = in.readInt();
			  this.idlist = new TreeSet<Integer>();
			  for (int i = 0; i < length; i++) {
				  this.idlist.add(in.readInt());
			  }
			  init();
      } catch(NullPointerException npe) { 
        if (null == in) { 
          LOG.error("Caught an NPE in readFields(). in is null");
          String fullStackTrace = 
            org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(npe);
          LOG.error(" " + fullStackTrace);
        } else { 
          LOG.error("Caught an NPE in readFields(). in is NOT null. ");
          String fullStackTrace = 
            org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(npe);
          LOG.error(" " + fullStackTrace);
        }
        throw npe;
      }
		}

		@Override
		public void write(DataOutput out) throws IOException {
      try{ 
			  super.write(out);
        if (out == null) { 
          LOG.error("write() called with a null out");
          //return null;
        }
        if (null == this.idlist) { 
          LOG.error("write() called with header having a null idlist. Sending list length of 1");
			    //out.writeInt(0);
        }
			  out.writeInt(this.idlist.size());
			  for (Integer id : idlist) {
				  out.writeInt(id);
			  }
      } catch (NullPointerException npe) { 
        if (null == out) { 
          LOG.error("Caught an npe in write(), out is null");
          String fullStackTrace = 
            org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(npe);
          LOG.error(" " + fullStackTrace);
        } else { 
          LOG.error("Caught an npe in write(), out is NOT null. ");
          String fullStackTrace = 
            org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(npe);
          LOG.error(" " + fullStackTrace);
        }
        throw npe;
      }
		}
		
	}
	
	private Header header;
	
	//private Path data;
	//private byte[] data;
	private ByteBuffer data;

	//private Path index;
	private ByteBuffer index;

	//private FSDataInputStream dataIn = null;
	//private FSDataInputStream indexIn = null;
	
	private int partitions;
	
	public Set<TaskID> toService = new HashSet<TaskID>();
	public transient Set<TaskAttemptID> serviced = new HashSet<TaskAttemptID>();

  private static final Log LOG = LogFactory.getLog(OutputInMemoryBuffer.class.getName());


	public OutputInMemoryBuffer() { 	}
	
	public OutputInMemoryBuffer(TaskAttemptID owner, Integer id, float progress, 
                              byte[] data, byte[] index, boolean complete, int partitions) {
		//this.type = Type.FILE;
		
		this.data = ByteBuffer.wrap(data);
		this.index = ByteBuffer.wrap(index);

    LOG.info("in constructor, passed-in data size is " + data.length);
    LOG.info("in constructor, internal data size is " + this.data.capacity());
    LOG.info("in constructor, passed-in index size is " + index.length);
    LOG.info("in constructor, internal index size is " + this.index.capacity());
		
		SortedSet<Integer> idlist = new TreeSet<Integer>();
		idlist.add(id);
		this.header = new InMemoryHeader(owner, progress, complete, idlist);
		//this.header = new FileHeader(owner, progress, complete, idlist);
		this.partitions = partitions;
	}

	public OutputInMemoryBuffer(TaskAttemptID owner, SortedSet<Integer> idlist, float progress, 
                              byte[] data, byte[] index, 
                              boolean complete, int partitions) {
		//this.type = Type.FILE;
		this.data     = ByteBuffer.wrap(data);
		this.index    = ByteBuffer.wrap(index);
		this.header = new InMemoryHeader(owner, progress, complete, idlist);
		this.partitions = partitions;

    LOG.info("in constructor, passed-in data size is " + data.length);
    LOG.info("in constructor, internal data size is " + this.data.capacity());
    LOG.info("in constructor, passed-in index size is " + index.length);
    LOG.info("in constructor, internal index size is " + this.index.capacity());
	}

	@Override
	public String toString() {
		return this.header.toString() + 
           " deps " + this.toService;
	}
	
	@Override
	public int compareTo(OutputInMemoryBuffer o) {
		return this.header.compareTo(o.header);
	}
	
	public int paritions() {
		return this.partitions;
	}
	
	public int serviced() {
		return this.serviced.size();
	}
	
	public int toService() {
		return this.toService.size();
	}

  public Set<TaskID> getToService() { 
    return this.toService;
  }
	
	public boolean isServiced(TaskAttemptID taskid) {
		return this.serviced.contains(taskid);
	}
	
	public void serviced(TaskAttemptID taskid) {
    LOG.info("    " + this.header.owner());
		this.serviced.add(taskid);
    if (this.toService.contains(taskid.getTaskID())) { 
     LOG.info("   Serviced a task that I thought I should: " + taskid.getTaskID());
     if (this.serviced.size() == this.toService.size()) { 
      LOG.info("   WHELP, that should be the last task that I service");
     }
    } else { 
     LOG.info("   Serviced a task that I DID NOT think I should: " + taskid.getTaskID());
     LOG.info("     toService: " + this.toService);
    }
	}
	
	public Header header() {
		return this.header;
	}
	
	public ByteBuffer data() {
		return this.data;
	}

	public ByteBuffer index() {
		return this.index;
	}

  public void setReduceTaskDependencies(int[] reduceTasks) { 
    LOG.info("in setReduceTaskDependencies() with len: " + reduceTasks.length);
	  //private transient Set<TaskID> toService = new HashSet<TaskID>();
    // need to create a TaskID for each entry
    for (int i=0; i<reduceTasks.length; i++) { 
      this.toService.add(new TaskID(this.header.owner().getJobID(), 
                               false, 
                               reduceTasks[i]));
      LOG.info("Adding redTasks " + (new TaskID(this.header.owner().getJobID(), 
                               false,
                               reduceTasks[i])) + " to toService");
    }
    LOG.info("exiting setReduceTaskDependencies, size: " + this.toService.size());
  }
	
	public void delete() throws IOException {
    LOG.info("In OutputInMemoryBuffer, attempting to delete data for " +
              this.header.owner());
		if (this.data != null) { 
      this.data = null;
		}
		
		if (this.index != null) {
      this.index = null;
		}
	}

	public void close() throws IOException {
    //Nothing to close, since we're not using streams
	}

  public DataInputStream dataInputStream() {
    return new DataInputStream( new ByteBufferInputStream(this.data) ); 
  }


	/**
	 * Seek to partition.
	 * @param partition # to seek to.
	 * @return partition segment length.
	 * @throws IOException
	 */
	public Header seek(int partition) throws IOException {
		try {
			//indexIn.seek(partition * JOutputBuffer.MAP_OUTPUT_INDEX_RECORD_LENGTH);
      this.index.position(partition * JOutputBuffer.MAP_OUTPUT_INDEX_RECORD_LENGTH);
			long segmentOffset    = this.index.getLong();
			long rawSegmentLength = this.index.getLong();
			long segmentLength    = this.index.getLong();

      if (this.data == null) { 
        LOG.error("About to seek and data is NULL. This is bad");
      } else { 
        LOG.info("Seeking to " + (int)segmentOffset + " buf.pos() "  + this.data.position() + 
                 " buf.lim " + this.data.limit() + " buf.cap " + this.data.capacity());
      }
			//dataIn.seek(segmentOffset);
			this.data.position((int)segmentOffset);
			
			header.decompressed(rawSegmentLength);
			header.compressed(segmentLength);
			return header;
		} catch (IllegalArgumentException iae) {
      if (null == this.index) { 
        LOG.error("seek failed, index is null"); 
        String fullStackTrace = 
          org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(iae);
        LOG.error(" " + fullStackTrace);
      } else { 
        LOG.error("seek failed, partition: " + partition + 
                " recLen: " + JOutputBuffer.MAP_OUTPUT_INDEX_RECORD_LENGTH + 
                " index: cap " + this.index.capacity() + 
                " limit " + this.index.limit() + " pos: " + this.index.position());
        String fullStackTrace = 
          org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(iae);
        LOG.error(" " + fullStackTrace);
      }
		 // close();
			throw iae;
		} catch (BufferUnderflowException bue) { 
      LOG.error("bue: " + bue.toString() + 
                "\n\t for partition " + partition + 
                "\n\t this.index: cap: " + this.index.capacity() + 
                " pos: " + this.index.position() + " limit: " + this.index.limit());
      throw bue;
    }
    //return null;  
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.partitions = in.readInt();
		this.header = Header.readHeader(in);
		
		//this.data = WritableUtils.readCompressedByteArray(in);
		//this.index = WritableUtils.readCompressedByteArray(in);
		this.data = WritableUtils.readByteBufferWithLength(in);
		this.index = WritableUtils.readByteBufferWithLength(in);

    // receive the toService data
    int toServiceSize = in.readInt();
    this.toService = new HashSet<TaskID>(toServiceSize);
    for (int i=0; i<toServiceSize; i++) { 
      TaskID tempID = new TaskID();
      tempID.readFields(in);
      this.toService.add(tempID);
    }
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.partitions);
		Header.writeHeader(out, this.header);

		//WritableUtils.writeCompressedByteArray(out, this.data);
		//WritableUtils.writeCompressedByteArray(out, this.index);
    WritableUtils.writeByteBufferWithLength(this.data, out);
    WritableUtils.writeByteBufferWithLength(this.index, out);

    // set the toService set over one at a time
    out.writeInt(this.toService.size());
    Iterator<TaskID> itr = this.toService.iterator();
    while(itr.hasNext()) { 
      itr.next().write(out);
    }
	}
}
