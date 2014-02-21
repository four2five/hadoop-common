package org.apache.hadoop.mapred.buffer.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
//import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.OutputInMemoryBuffer;

public abstract class InMemoryBufferExchangeSource<H extends OutputInMemoryBuffer.Header> 
	implements Comparable<InMemoryBufferExchangeSource>, BufferExchange {
	
	private static final Log LOG = LogFactory.getLog(InMemoryBufferExchangeSource.class.getName());

	
	public static final InMemoryBufferExchangeSource factory(JobConf conf, BufferRequest request) {
    /*
		if (request.bufferType() == BufferType.FILE) {
			return new FileSource(rfs, conf, request);
		}
		if (request.bufferType() == BufferType.SNAPSHOT) {
			return new SnapshotSource(rfs, conf, request);
		}
		if (request.bufferType() == BufferType.STREAM) {
			return new StreamSource(rfs, conf, request);
		}
    */
    return new InMemoryBufferSource(conf, request);
	}
	
    //private FileSystem rfs;
	
	/* Job configuration. */
	protected JobConf conf;
	
	/* The destination task identifier. */
	protected TaskAttemptID destination;
	
	/* The partition that we're interested in. */
	protected int partition;

	/* The address of the remote task (that made the request)
	 * receiving the outputs of each task. */
	protected InetSocketAddress address;

	/* Used to send the records. */
	protected DataOutputStream ostream = null;
	
	/* Used to receive control data. */
	protected DataInputStream istream = null;
	
	protected Socket socket = null;
	
	protected InMemoryBufferExchangeSource(JobConf conf, BufferRequest request) {
		//this.rfs = rfs;
		this.conf = conf;
		this.destination = request.destination();
		this.partition = request.partition();
		this.address = request.destAddress();
	}
	
	@Override
	public String toString() {
		return "RequestManager destination " + destination;
	}

	@Override
	public int hashCode() {
		return this.destination.hashCode();
	}

	@Override
	public int compareTo(InMemoryBufferExchangeSource o) {
		return this.destination.compareTo(o.destination) == 0 ? 
				this.partition - o.partition : this.destination.compareTo(o.destination);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof InMemoryBufferExchangeSource) {
			return this.destination.equals(((InMemoryBufferExchangeSource)o).destination);
		}
		return false;
	}
	
	public TaskAttemptID destination() {
		return this.destination;
	}
	
	public final Transfer send(OutputInMemoryBuffer buffer) {
		synchronized (this) {
			try {
				return transfer(buffer);
			} catch (Exception e) {
				System.err.println("ERROR: buffer " + buffer);
				e.printStackTrace();
				return Transfer.IGNORE;
			}
		}
	}
	
	protected abstract Transfer transfer(OutputInMemoryBuffer buffer);
	
	public void close() {
		synchronized (this) {
			if (socket != null && socket.isConnected()) {
				try {
					ostream.writeInt(0); // close up shop
					ostream.close();
					istream.close();
					socket.close();
				} catch (IOException e) {
					LOG.error(e);
				}
			}
			
			socket = null;
			ostream = null;
			istream = null;
		}
	}

  // implicitly this is an InMemory open
	protected BufferExchange.Connect open() {
			if (socket == null || socket.isClosed()) {
				socket = new Socket();
				try {
					socket.connect(this.address);

					ostream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
					istream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
					
					BufferExchange.Connect connection = 
						WritableUtils.readEnum(istream, BufferExchange.Connect.class);
					if (connection == BufferExchange.Connect.OPEN) {
						WritableUtils.writeEnum(ostream, BufferType.INMEMORY);
						ostream.flush();
					}
					else {
						return connection;
					}
				} catch (IOException e) {
					if (socket != null && !socket.isClosed()) {
						try { socket.close();
						} catch (Throwable t) { }
					}
					socket = null;
					ostream = null;
					istream = null;
					return BufferExchange.Connect.ERROR;
				}
			}
			return BufferExchange.Connect.OPEN;
	}
					
	protected BufferExchange.Transfer transmit(OutputInMemoryBuffer buffer) {
    /*
		try {
			file.open(rfs);
		} catch (IOException e) {
			// We don't want to send anymore of this output! 
			return BufferExchange.Transfer.TERMINATE;
		}
    */

		try {
      LOG.info("Source sending " + Integer.MAX_VALUE);
			ostream.writeInt(Integer.MAX_VALUE); // Sending something
			OutputInMemoryBuffer.Header header = buffer.seek(partition);

      LOG.info("Source sending " + header);
			OutputInMemoryBuffer.Header.writeHeader(ostream, header);
			ostream.flush();

			BufferExchange.Transfer response = WritableUtils.readEnum(istream, BufferExchange.Transfer.class);
			if (BufferExchange.Transfer.READY == response) {
				LOG.info(this + " sending " + header);
				write(header, buffer.dataInputStream());
				return BufferExchange.Transfer.SUCCESS;
			} else { 
				LOG.info("Not sending " + header + " because response != READY");
      }
			return response;
		} catch (IOException e) {
			close(); // Close so reconnect will figure out current status.
			LOG.debug(e);
		}
		return BufferExchange.Transfer.RETRY;
	}
	
	/**
	 * Helper method to send records from the output file to the socket of the
	 * remote task receiving them. 
	 * Note: The current fault tolerance model does not allow us to multiplex multiple 
	 * output files. That is, we have to send in units of output files. 
	 * @throws IOException
	 */
	private void write(OutputInMemoryBuffer.Header header, DataInputStream fstream) throws IOException {
		long length = header.compressed();
		if (length == 0 && header.progress() < 1.0f) {
			return;
		}
		
		LOG.debug("Writing data for header " + header);
		long bytesSent = 0L;
		byte[] buf = new byte[64 * 1024];
		int n = fstream.read(buf, 0, (int)Math.min(length, buf.length));
		while (n > 0) {
			bytesSent += n;
			length -= n;
			ostream.write(buf, 0, n);

			n = fstream.read(buf, 0, (int) Math.min(length, buf.length));
		}
		ostream.flush();
		LOG.debug(bytesSent + " total bytes sent for header " + header);
	}
	
	//////////////////////////////////////////////////////////////////////////////////////
	
  
	private static class InMemoryBufferSource 
      extends InMemoryBufferExchangeSource<OutputInMemoryBuffer.InMemoryHeader> {
		// Store position for each source task. 
		private Map<TaskID, Integer> cursor;

		public InMemoryBufferSource(JobConf conf, BufferRequest request) {
			super(conf, request);
			this.cursor = new HashMap<TaskID, Integer>();
		}

		@Override
		protected final Transfer transfer(OutputInMemoryBuffer buffer) {
			OutputInMemoryBuffer.InMemoryHeader header = (OutputInMemoryBuffer.InMemoryHeader) buffer.header();
			TaskID taskid = header.owner().getTaskID();
			if (!cursor.containsKey(taskid) || cursor.get(taskid) == header.ids().first()) { 
				BufferExchange.Connect result = open();
				if (result == Connect.OPEN) {
					LOG.debug("Transfer buffer " + buffer + ". Destination " + destination());
					Transfer response = transmit(buffer);
					if (response == Transfer.TERMINATE) {
						return Transfer.TERMINATE;
					}

					// Update my next cursor position. 
					int position = header.ids().last() + 1;
					try { 
						int next = istream.readInt();
						if (position != next) {
							LOG.debug("Assumed next position " + position + " != actual " + next);
							position = next;
						}
					} catch (IOException e) { e.printStackTrace(); LOG.error(e); }

					if (response == Transfer.SUCCESS) {
						if (header.eof()) {
							LOG.debug("Transfer end of file for source task " + taskid);
							close();
						}
						cursor.put(taskid, position);
						LOG.debug("Transfer complete. New position " + cursor.get(taskid) + ". Destination " + destination());
					}
					else if (response == Transfer.IGNORE){
						cursor.put(taskid, position); // Update my cursor position
					}
					else {
						LOG.debug("Unsuccessful send. Transfer response: " + response);
					}

					return response;
				}
				else if (result == Connect.BUFFER_COMPLETE) {
					cursor.put(taskid, Integer.MAX_VALUE);
					return Transfer.SUCCESS;
				}
				else {
					return Transfer.RETRY;
				}
			}
			else {
				LOG.debug("Transfer ignore header " + header + " current position " + cursor.get(taskid));
				return Transfer.IGNORE;
			}
		}
	}

  /*
	private static class InMemoryBufferSource extends InMemoryBufferExchangeSource<OutputInMemoryBuffer.InMemoryHeader> {
		private Map<TaskID, Long> cursor;
		
		public InMemoryBufferSource(JobConf conf, BufferRequest request) {
			super(conf, request);
			this.cursor = new HashMap<TaskID, Long>();
		}
		
		@Override
		protected final Transfer transfer(OutputInMemoryBuffer buffer) {
			OutputInMemoryBuffer.InMemoryHeader header = (OutputInMemoryBuffer.InMemoryHeader) buffer.header();
			TaskID taskid = header.owner().getTaskID();
			if (!cursor.containsKey(taskid) || cursor.get(taskid) == header.sequence()) { 
				BufferExchange.Connect result = open(); // implicitly, this is an InMemory open
				if (result == Connect.OPEN) {
					LOG.info("Transfer buffer " + buffer.header().owner() + " to Destination " + destination());
					LOG.info("  Buffer eof " + buffer.header().eof() + " progress " + buffer.header().progress());
					Transfer response = transmit(buffer);
					if (response == Transfer.TERMINATE) {
            LOG.info("Transfer terminated");
						return Transfer.TERMINATE;
					}

					// Update my next cursor position. 
					long position = header.sequence() + 1;
					try { 
						long next = istream.readLong();
						if (position != next) {
							position = next;
						}
					} catch (IOException e) { e.printStackTrace(); LOG.error(e); }

					if (response == Transfer.SUCCESS) {
						cursor.put(taskid, position);
						LOG.info("Transfer complete. New position " + cursor.get(taskid) + ". Destination " + destination());
					}
					else if (response == Transfer.IGNORE){
						cursor.put(taskid, position); // Update my cursor position
					}
					else {
						LOG.info("Unsuccessful send. Transfer response: " + response);
					}

					return response;
				}
				else {
          LOG.info("Transfer, returning retry");
					return Transfer.RETRY;
				}
			} else {
				LOG.info("Stream transfer ignore " + header +  " current sequence " + cursor.get(taskid));
				return Transfer.IGNORE;
			}
		}
	}
  */
	
  /*
	private static class SnapshotSource extends InMemoryBufferExchangeSource<OutputFile.SnapshotHeader> {
		private Map<TaskID, Float> cursor;
		
		public SnapshotSource(FileSystem rfs, JobConf conf, BufferRequest request) {
			super(rfs, conf, request);
			this.cursor = new HashMap<TaskID, Float>();
		}

		@Override
		protected final Transfer transfer(OutputFile file) {
			OutputFile.SnapshotHeader header = (OutputFile.SnapshotHeader) file.header();
			TaskID taskid = header.owner().getTaskID();
			if (!cursor.containsKey(taskid) || cursor.get(taskid) < header.progress()) {
				BufferExchange.Connect result = open(BufferExchange.BufferType.SNAPSHOT);
				if (result == BufferExchange.Connect.OPEN) {
					Transfer response = transmit(file);
					if (response == Transfer.TERMINATE) {
						return Transfer.TERMINATE;
					}
					
					try {
						float pos = istream.readFloat();
						cursor.put(taskid, pos);
						if (header.eof()) {
							close();
						}
					} catch (IOException e) {
						e.printStackTrace();
						LOG.error(e);
						return Transfer.RETRY;
					}
					return response;
				}
				else if (result == BufferExchange.Connect.BUFFER_COMPLETE) {
					cursor.put(taskid, 1f);
					close();
					return Transfer.SUCCESS;
				} 
				else {
					return Transfer.RETRY;
				}
			}
			return Transfer.IGNORE;
		}
	}
  */

}
