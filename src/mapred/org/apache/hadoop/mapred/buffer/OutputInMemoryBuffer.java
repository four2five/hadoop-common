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
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.HashSet;
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

		//public Header(OutputInMemoryBuffer.Type type, TaskAttemptID owner, float progress, boolean eof) {
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
			this.owner = new TaskAttemptID();
			this.owner.readFields(in);

			this.progress = in.readFloat();
			this.compressedLength = in.readLong();
			this.decompressedLength = in.readLong();
			this.eof = in.readBoolean();
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
			//OutputInMemoryBuffer.Type type = WritableUtils.readEnum(in, OutputInMemoryBuffer.Type.class);
			Header header = null;
      /*
			switch (type) {
			case FILE: header = new FileHeader(); break;
			case SNAPSHOT: header = new SnapshotHeader(); break;
			case STREAM: header = new StreamHeader(); break;
			default: return null;
			}
      */
      header = new InMemoryHeader();
			header.readFields(in);
			return header;
		}

		public static void writeHeader(DataOutput out, Header header) throws IOException {
			//WritableUtils.writeEnum(out, header.type);
			header.write(out);
		}

	}

	public static class InMemoryHeader extends Header {
		
		//private long sequence;
    private SortedSet<Integer> idlist; // may need this for merging outputs
		private String code;
		
		public InMemoryHeader() { super(null, 0f, false); }
		
    /*
		public InMemoryHeader(TaskAttemptID owner, long sequence, float progress, boolean complete) {
			super(owner, progress, complete);
			//super(owner, 0f, false);
			this.sequence = sequence;
      this.idlist = null;
      init();
		}
    */
		
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

    /*
		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
      // need to indicate if we're sending a sequenceID or a list of ids
      if (null == this.sequence) { 
        this
			this.sequence = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
      if (null == this.sequence) { 
        out.writeBoolean(true);
      } else { 
        out.writeBoolean(false);
      }

			out.writeLong(this.sequence);
		}
    */

		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			int length = in.readInt();
			this.idlist = new TreeSet<Integer>();
			for (int i = 0; i < length; i++) {
				this.idlist.add(in.readInt());
			}
			init();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
			out.writeInt(this.idlist.size());
			for (Integer id : idlist) {
				out.writeInt(id);
			}
		}
		
	}
	
  /*
	public static class StreamHeader extends Header {
		
		private long sequence;
		
		public StreamHeader() { super(Type.STREAM, null, 0f, false); }
		
		public StreamHeader(TaskAttemptID owner, long sequence) {
			super(Type.STREAM, owner, 0f, false);
			this.sequence = sequence;
		}
		
		public long sequence() {
			return this.sequence;
		}
		
		public int compareTo(Header header) {
			if (header instanceof StreamHeader) {
				StreamHeader other = (StreamHeader) header;
				if (owner.equals(other.owner)) {
					if (sequence < other.sequence) {
						return -1;
					}
					else if (sequence > other.sequence) {
						return 1;
					}
					else return 0;
				}
			}
			return -1;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof StreamHeader) {
				return this.compareTo((StreamHeader) o) == 0;
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return Long.toString(this.sequence).hashCode();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			this.sequence = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
			out.writeLong(this.sequence);
		}
		
	}
	
	public static class FileHeader extends Header {
		//The current position. 
		private SortedSet<Integer> idlist;
		
		private String code;

		public FileHeader() { super(Type.FILE, null, 0f, false); }

		public FileHeader(TaskAttemptID owner, float progress,
				          boolean complete, SortedSet<Integer> idlist) {
			super(Type.FILE, owner, progress, complete);
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
			return "File -- " + code + ". EOF? " + eof();
		}
		
		@Override
		public int compareTo(Header header) {
			if (header instanceof FileHeader) {
				FileHeader fheader = (FileHeader) header;
				TaskID me = owner().getTaskID();
				TaskID other = fheader.owner().getTaskID();
				if (me.equals(other)) {
					Integer me_min = idlist.first();
					Integer o_min = fheader.idlist.first();
					Integer me_max = idlist.last();
					Integer o_max = fheader.idlist.last();
					
					if (me_max.compareTo(o_min) < 0) {
						return -1;
					}
					else if (me_min.compareTo(o_max) > 0) {
						return 1;
					}
					else {
						// Okay, basically one is a subset of the
						 // other. I want the superset to fall before
						 // the subset. 
						return me_min.compareTo(o_min) == 0 ? 
								o_max.compareTo(me_max) : 
									me_min.compareTo(o_min);
					}
				}
				else {
					return me.compareTo(other);
				}
			}
			return -1;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof FileHeader) {
				return this.compareTo((FileHeader) o) == 0;
			}
			return false;
		}

		@Override
		public int hashCode() {
			return code.hashCode();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			int length = in.readInt();
			this.idlist = new TreeSet<Integer>();
			for (int i = 0; i < length; i++) {
				this.idlist.add(in.readInt());
			}
			init();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
			out.writeInt(this.idlist.size());
			for (Integer id : idlist) {
				out.writeInt(id);
			}
		}
	}
  */

  /*
	public static class SnapshotHeader extends Header {

		public SnapshotHeader() { super(Type.SNAPSHOT, null, 0f, false); }

		public SnapshotHeader(TaskAttemptID owner, float progress) {
			super(Type.SNAPSHOT, owner, progress, progress == 1f);
		}
		
		@Override
		public int compareTo(Header header) {
			if (header instanceof SnapshotHeader) {
				SnapshotHeader sheader = (SnapshotHeader) header;
				TaskID me = owner().getTaskID();
				TaskID other = sheader.owner().getTaskID();
				if (me.equals(other)) {
					return this.progress() < sheader.progress() ? -1 :
							this.progress() > sheader.progress() ? 1 : 0;
				}
				else {
					return me.compareTo(other);
				}
			}
			return -1;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof SnapshotHeader) {
				return this.compareTo((SnapshotHeader) o) == 0;
			}
			return false;
		}

		@Override
		public int hashCode() {
			return (owner().getTaskID().toString() + ":" + progress()).hashCode();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
		}
	}
  */

	
	private Header header;
	
	//private Type type;
	
	//private Path data;
	//private byte[] data;
	private ByteBuffer data;

	//private Path index;
	private ByteBuffer index;

	//private FSDataInputStream dataIn = null;
	//private FSDataInputStream indexIn = null;
	
	private int partitions;
	
	private transient Set<TaskAttemptID> serviced = new HashSet<TaskAttemptID>();

  private static final Log LOG = LogFactory.getLog(OutputInMemoryBuffer.class.getName());


	public OutputInMemoryBuffer() { 	}
	
  /*
	//public OutputInMemoryBuffer(TaskAttemptID owner, long sequence, Path data, Path index, int partitions) 
	public OutputInMemoryBuffer(TaskAttemptID owner, long sequence, byte[] data, byte[] index, int partitions) {
		//this.type = Type.STREAM;
		this.data = ByteBuffer.wrap(data);
		this.index = ByteBuffer.wrap(index);
		this.header = new InMemoryHeader (owner, sequence);
		this.partitions = partitions;
	}
  */

  /*
	public OutputInMemoryBuffer(TaskAttemptID owner, float progress, byte[] data, byte[] index, int partitions) {
		//this.type = Type.SNAPSHOT;
		this.data = data;
		this.index = index;
		this.header = new SnapshotHeader(owner, progress);
		this.partitions = partitions;
	}

	public OutputInMemoryBuffer(TaskAttemptID owner, SortedSet<Integer> idlist, float progress, 
                              byte[] data, byte[] index, 
                              boolean complete, int partitions) {
		//this.type = Type.FILE;
		this.data     = data;
		this.index    = index;
		this.header = new FileHeader(owner, progress, complete, idlist);
		this.partitions = partitions;
	}
	*/
  
	public OutputInMemoryBuffer(TaskAttemptID owner, Integer id, float progress, 
                              byte[] data, byte[] index, boolean complete, int partitions) {
		//this.type = Type.FILE;
		
		this.data = ByteBuffer.wrap(data);
		this.index = ByteBuffer.wrap(index);

    LOG.info("in constructor, passed-in data size is " + data.length);
    LOG.info("in constructor, internal data size is " + this.data.capacity());
		
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
	}

	@Override
	public String toString() {
		return this.header.toString();
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
	
	public boolean isServiced(TaskAttemptID taskid) {
		return this.serviced.contains(taskid);
	}
	
	public void serviced(TaskAttemptID taskid) {
		this.serviced.add(taskid);
	}
	
	public Header header() {
		return this.header;
	}
	
	//public Type type() {
	//	return this.type;
	//}
	
	public ByteBuffer data() {
		return this.data;
	}

	public ByteBuffer index() {
		return this.index;
	}
	
	public void delete() throws IOException {
    LOG.info("In OutputInMemoryBuffer, attempting to delete data");
		if (this.data != null) { 
      this.data = null;
		}
		
		if (this.index != null) {
      this.index = null;
		}
	}

  /*
	public void open(FileSystem fs) throws IOException {
		if (this.dataIn == null) {
			this.dataIn = fs.open(data());
			this.indexIn = fs.open(index());
		}
    LOG.info("In OutputInMemoryBuffer, opening file: " + data());
	}
  */

	public void close() throws IOException {
    /*
		if (this.dataIn != null) {
      LOG.info("In OutputInMemoryBuffer, closing dataIn");
			this.dataIn.close();
			this.dataIn = null;
		}

		if (this.indexIn != null) {
			this.indexIn.close();
			this.indexIn = null;
		}
    */
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
		//try {
			//indexIn.seek(partition * JOutputBuffer.MAP_OUTPUT_INDEX_RECORD_LENGTH);
      this.index.position(partition * JOutputBuffer.MAP_OUTPUT_INDEX_RECORD_LENGTH);
			long segmentOffset    = this.index.getLong();
			long rawSegmentLength = this.index.getLong();
			long segmentLength    = this.index.getLong();

			//dataIn.seek(segmentOffset);
			this.data.position((int)segmentOffset);
			
			header.decompressed(rawSegmentLength);
			header.compressed(segmentLength);
			return header;
    /*
		} catch (IOException e) {
			close();
			throw e;
		}
    */
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.partitions = in.readInt();
		this.header = Header.readHeader(in);
		
		//this.data = WritableUtils.readCompressedByteArray(in);
		//this.index = WritableUtils.readCompressedByteArray(in);
		this.data = WritableUtils.readByteBufferWithLength(in);
		this.index = WritableUtils.readByteBufferWithLength(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.partitions);
		Header.writeHeader(out, this.header);

		//WritableUtils.writeCompressedByteArray(out, this.data);
		//WritableUtils.writeCompressedByteArray(out, this.index);
    WritableUtils.writeByteBufferWithLength(this.data, out);
    WritableUtils.writeByteBufferWithLength(this.index, out);
	}
}
