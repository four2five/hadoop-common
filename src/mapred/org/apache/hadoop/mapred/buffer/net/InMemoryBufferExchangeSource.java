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
    if (request.bufferType() != BufferType.INMEMORY) { 
      LOG.error("Received a non-in-memory buffer request: " + request);
      LOG.error("Converting it into an INMEMORY type");
    }

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
		return "RequestManager destination " + destination + " : " + address; 
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

  public InetSocketAddress address() { 
    return this.address;
  }
	
	public final Transfer send(OutputInMemoryBuffer buffer) {
    LOG.info(this + " transferring buffer " + buffer.header());
		synchronized (this) {
			try {
				return transfer(buffer);
			} catch (Exception e) {
				System.err.println(this + " ERROR: buffer " + buffer);
				e.printStackTrace();
				return Transfer.IGNORE;
			}
		}
	}
	
	protected abstract Transfer transfer(OutputInMemoryBuffer buffer);
	
	public void close() {
    LOG.info(this + " closing sink for " + destination());
		synchronized (this) {
			if (socket != null && socket.isConnected()) {
				try {
					ostream.writeInt(0); // close up shop
					ostream.close();
					istream.close();
					socket.close();
				} catch (IOException e) {
					LOG.error(e);
				} finally{
			    socket = null;
			    ostream = null;
			    istream = null;
        }
			}
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
					
					BufferExchange.Connect connection = WritableUtils.readEnum(istream, BufferExchange.Connect.class);
					if (connection == BufferExchange.Connect.OPEN) {
						WritableUtils.writeEnum(ostream, BufferType.INMEMORY);
						ostream.flush();
					} else {
            LOG.info("open() did not return OPEN, rather: " + connection);
						return connection;
					}
				} catch (IOException e) {
          LOG.error("Exception in BufferExchange.Connect open() " + e);
					if (socket != null && !socket.isClosed()) {
						try { 
              socket.close();
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

		OutputInMemoryBuffer.Header header = null;
		BufferExchange.Transfer response = null; 
		try {
      LOG.info("Source sending " + Integer.MAX_VALUE);
			ostream.writeInt(Integer.MAX_VALUE); // Sending something
			header = buffer.seek(partition);

      LOG.info("Source sending " + header);
			OutputInMemoryBuffer.Header.writeHeader(ostream, header);
			ostream.flush();

      // we're getting stuck here
			response = WritableUtils.readEnum(istream, 
                                         BufferExchange.Transfer.class);

			if (BufferExchange.Transfer.READY == response) {
				LOG.info("JB, " + this + " sending " + header);
				write(header, buffer.dataInputStream());
				return BufferExchange.Transfer.SUCCESS;
			} else { 
				LOG.info("Not sending buffer for " + header + 
                 " because response != READY. Rather it is " + response.toString());
			  return response;
      }
		} catch (IOException e) {
			close(); // Close so reconnect will figure out current status.
			LOG.debug(e);
		} catch (NullPointerException npe) { 
      LOG.info("Caught an npe transmitting header.");
      if (null == buffer) { 
        LOG.error(" buffer is null");
      } else { 
        LOG.error(" buffer is NOT null. header is " + header);
      }
    } catch (IllegalArgumentException iae) {
      LOG.info(" caught an iae, partition: " + partition); 
      //" buf.pos: " + buffer.position() + 
               //" buf.lim " + buffer.limit() + " buf.cap " + buffer.capacity());
    }

		LOG.info("Something went sideways with " + header);
    if (null != response) { 
      LOG.info("   response is " + response.toString());
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
		
		LOG.info("Writing data for header " + header);
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
			OutputInMemoryBuffer.InMemoryHeader header = 
        (OutputInMemoryBuffer.InMemoryHeader) buffer.header();
			TaskID taskid = header.owner().getTaskID();
			if (!cursor.containsKey(taskid) || cursor.get(taskid) == header.ids().first()) { 
				BufferExchange.Connect result = open();
				if (result == Connect.OPEN) {
					LOG.info(this + " transfer buffer " + buffer + ". Destination " + destination());
					Transfer response = transmit(buffer);
					if (response == Transfer.TERMINATE) {
            LOG.info(this + " response is TERMINATE");
						return Transfer.TERMINATE;
					} else if (null == istream) { 
            LOG.info(this + " istream is null, sending Transfer.IGNORE");
            return Transfer.IGNORE;
          } else { 
            LOG.info(this + " istream is fine");
          }

					// Update my next cursor position. 
					int position = header.ids().last() + 1;
					try { 
						int next = istream.readInt();
						if (position != next) {
							LOG.info(this + " assumed next position " + position + " != actual " + next);
							position = next;
						}
					} catch (IOException e) { e.printStackTrace(); LOG.error(e); }

					if (response == Transfer.SUCCESS) {
						if (header.eof()) {
							LOG.info(this + " transfer end of data for source task " + taskid);
							close();
						}
						cursor.put(taskid, position);
						LOG.info(this + " transfer complete. New position " + cursor.get(taskid) + 
                      ". Destination " + destination());
					} else if (response == Transfer.IGNORE){
            LOG.info(this + " response is IGNORE");
						cursor.put(taskid, position); // Update my cursor position
					} else {
						LOG.info(this + " unsuccessful send. Transfer response: " + response);
					}

					return response;
				} else if (result == Connect.BUFFER_COMPLETE) {
					LOG.info(this + " result is BUFFER_COMPLETE. Transfer success");
					cursor.put(taskid, Integer.MAX_VALUE);
					return Transfer.SUCCESS;
				} else if (result == Connect.ERROR) {
					LOG.info(this + " result is ERROR for buffer " + buffer + 
                   ". Destination " + destination());
          LOG.info(this + " sending retry");
					return Transfer.RETRY;
				} else {
					LOG.info(this + " result is " + result + " for buffer " + buffer + 
                   ". Destination " + destination());
          LOG.info(this + " sending retry");
					return Transfer.RETRY;
        }
			} else {
				LOG.info(this + " Transfer ignore header " + header + 
                  " current position " + cursor.get(taskid));
				return Transfer.IGNORE;
			}
		}
	}
}
