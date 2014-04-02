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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
//import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.InMemoryBufferExchangeSource;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.apache.hadoop.mapred.buffer.net.MapBufferRequest;
import org.apache.hadoop.mapred.buffer.net.ReduceBufferRequest;
import org.apache.hadoop.net.NetUtils;

/**
 * Manages the output buffers and buffer requests of all tasks running on this TaskTracker.
 * A task will generate some number of spill files and a final output. These spill files
 * are contained in {@link #output(OutputInMemoryBuffer)} objects and passed to the controller via
 * RPC call from the task process.
 * 
 * Certain tasks will request buffers from upstream tasks. A ReduceTask will request its
 * partition from all map tasks in the same job. A PipelinedMapTask will request the output
 * of the corresponding ReduceTask in the upstream job. 
 * 
 * @author tcondie
 *
 */
public class InMemoryManager implements InMemoryBufferUmbilicalProtocol {
	private static final Log LOG = LogFactory.getLog(InMemoryManager.class);

	/**
	 * The BufferController will create a single object of this class to manage the
	 * transfer of all requests. 
	 * @author tcondie
	 *
	 */
	private class RequestTransfer extends Thread {
		/**
		 * Holds all requests that need to be transfered to other BufferControllers
		 * that reside on separate TaskTrackers. We group these requests by the
		 * address of the remote BufferController so we can transfer multiple requests
		 * in a single session. 
		 */
		private Map<InetSocketAddress, Set<BufferRequest>> transfers;

		public RequestTransfer() {
			this.transfers = new HashMap<InetSocketAddress, Set<BufferRequest>>();
		}

		/**
		 * Transfer a new request. This method does not block.
		 * @param request The request to transfer.
		 */
		public void transfer(BufferRequest request) {
			synchronized(transfers) {
				InetSocketAddress source =
					NetUtils.createSocketAddr(request.srcHost() + ":" + controlPort);
				if (!transfers.containsKey(source)) {
					transfers.put(source, new HashSet<BufferRequest>());
				}
				transfers.get(source).add(request);
				transfers.notify();
			}
		}

		/**
		 * The main transfer loop. 
		 */
		public void run() {
			/* I need to ensure that I don't hold the transfer lock while
			 * sending requests. I will dump my requests into these two
			 * HashSet objects then release the transfer lock.
			 */
			Set<InetSocketAddress> locations = new HashSet<InetSocketAddress>();
			Set<BufferRequest>     handle    = new HashSet<BufferRequest>();
			while (!isInterrupted()) {
				synchronized (transfers) {
					while (transfers.size() == 0) {
						try { transfers.wait();
						} catch (InterruptedException e) { }
					}
					locations.clear();
					/* Deal with these locations. */
					locations.addAll(transfers.keySet());
				}

				for (InetSocketAddress location : locations) {
					synchronized(transfers) {
						handle.clear();
						/* Handle these requests. */
						handle.addAll(transfers.get(location));
					}
					
					Socket socket = null;
					DataOutputStream out = null;
					try {
						socket = new Socket();
						socket.connect(location);
						out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						out.writeInt(handle.size());  // Tell remote end how many requests.
            //LOG.info("Sending " + handle.size() + " to " + location);
            /*
            for (BufferRequest br : handle) { 
						  LOG.info("  Sending " + handle + " to " + location);
            }
            */

						for (BufferRequest request : handle) {
							BufferRequest.write(out, request); // Write the request to the socket.
							LOG.info("  Sent request " + request + " to " + location);
						}
						out.flush();
						out.close();
						out = null;
            socket.close();
            socket = null;

						synchronized (transfers) {
							/* Clear out the requests that were sent.  Other
							 * requests could have been added in the meantime. */
							transfers.get(location).removeAll(handle);
							if (transfers.get(location).size() == 0) {
								transfers.remove(location);
							}
						}
					} catch (IOException e) {
						LOG.warn("BufferController: Trying to connect to " + location + "."
						          + " Request transfer connection issue " + e);
					} finally {
						try {
							if (out != null) {
								out.close();
							}
						} catch (Throwable t) {
							LOG.error(t);
						}

						try {
							if (socket != null) {
								socket.close();
							}
						} catch (Throwable t) {
							LOG.error(t);
						}
					}
				}
			}
		}

	};


	/**
	 * Manages the output files generated by a given
	 * task attempt. Task attempts will always generate
	 * some number of intermediate spill files and a single
	 * final output, which is the merge of all spill files. 
	 * 
	 * This class is also responsible for calling request
	 * managers with those output files as they are generated.
	 * 
	 * @author tcondie
	 */
	private class BufferManager implements Runnable {

		/* Am I active and busy? */
		private boolean open;
		private boolean busy;
		private boolean somethingToSend;

		/* The task attempt whose output files I am managing. */
		private TaskAttemptID taskid;

		/* Intermediate buffer outputs, in generation order. */
		private SortedSet<OutputInMemoryBuffer> outputs;

    // Set of tasks to service
    private Set<TaskID> toService;

		/* Requests for partitions in my buffer. 
		 * Exchange sources will be ordered by partition so 
		 * that we service them in the correct order (according
		 * to the order of the output file). */
		private SortedSet<InMemoryBufferExchangeSource> sources;
		
		private float stallfraction = 0f;
		
		public BufferManager(TaskAttemptID taskid) {
			this.taskid = taskid;
			this.outputs = new TreeSet<OutputInMemoryBuffer>();
			this.sources = new TreeSet<InMemoryBufferExchangeSource>();
			this.open = true;
			this.busy = false;
			this.somethingToSend = false;
      this.toService = new HashSet<TaskID>();
      LOG.info("Creating a BufferManager for task " + this.taskid);
		}

		@Override
		public String toString() {
			return "BufferManager: buffer " + taskid;
		}

		@Override
		public int hashCode() {
			return this.taskid.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof BufferManager) {
				return ((BufferManager)o).taskid.equals(taskid);
			}
			return false;
		}

    public void setToService(Set<TaskID> toService) { 
      for (TaskID ts : toService) { 
        this.toService.add(ts);
      }
    }

    public Set<TaskID> getToService() { 
      return this.toService;
    }

		/**
		 * Indicates how well I'm keeping up with the generation
		 * of new output files. 
		 * @return
		 */
		public float stallfraction() {
			return this.stallfraction;
		}

		/**
		 * Called with the task attempt is done generating output files. 
		 */
		public void close() {
			synchronized (this) {
				open = false;
				this.notifyAll();
				
				while (busy) {
					try { 
            this.wait();
					} catch (InterruptedException e) { 
          }
				}
			}
		}
		
		
		@Override
		public void run() {
			try {
				/* I don't want to block calls to new output files and/or requests.
				 * I will copy the outputs and requests that I'm about to service
				 * into these objects before servicing them. */
				SortedSet<OutputInMemoryBuffer> out = new TreeSet<OutputInMemoryBuffer>();
				SortedSet<InMemoryBufferExchangeSource> src = 
            new TreeSet<InMemoryBufferExchangeSource>();
				while (open) {
          //LOG.info("\t\ttop of the while loop");
					synchronized (this) {
						while (!somethingToSend && open) {
							LOG.debug(this + " nothing to send.");
							try { 
                this.wait();
							} catch (InterruptedException e) { }
						}
						
						if (!open) return;
						LOG.debug(this + " something to send.");
            synchronized(this) { 
						  out.addAll(this.outputs); // Copy output files.
						  src.addAll(this.sources); // Copy requests.
            }
						somethingToSend = false;  // Assume we send everything.
						busy = true;
					}

					try {
						flush(out, src);
					} finally {
						synchronized (this) {
							this.busy = false;
							this.notifyAll();
						}
						
						for (InMemoryBufferExchangeSource s : src) {
							s.close();
						}
						
						out.clear();
						src.clear();
						if (open) Thread.sleep(250); // Have a coffee
					}
          //LOG.info("\t\tbottom of the while loop");
				}
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				LOG.info(this + " exiting.");
			}
		}
		
		/**
		 * Register a new request for my output files.
		 * @param request The request manager that will accept
		 * the output files managed by this object.
		 * @throws IOException
		 */
		private void add(InMemoryBufferExchangeSource source) throws IOException {
			synchronized (this) {
        LOG.info("Adding request source " + source + 
                 " to manager for " + this.taskid + 
                 " total size: " + this.sources.size());
				this.sources.add(source);
				somethingToSend = true;
				this.notifyAll();
			}
		}

		/** 
		 * Add the output file. 
		 * @param buffer The new output buffer.
		 * @throws IOException
		 */
		private void add(OutputInMemoryBuffer buffer) throws IOException {
			synchronized (this) {
        LOG.info("Adding buffer " + buffer + 
                 " capacity: " + buffer.data().capacity() + 
                 " position: " + buffer.data().position() + 
                 " to manager for " + this.taskid);
				this.outputs.add(buffer);
				somethingToSend = true;
				this.notifyAll();
			}
		}


		/**
		 * Flush the output files to the request managers. 
		 * @param outputs The output buffers.
		 * @param requests The request managers.
		 * @return Those requests that have been fully satisifed.
		 */
		private void flush(SortedSet<OutputInMemoryBuffer> outs, 
                       Collection<InMemoryBufferExchangeSource> srcs) {
			float stalls = 0f;
			float src_cnt = srcs.size();
			for (OutputInMemoryBuffer buffer : outs) {
				Iterator<InMemoryBufferExchangeSource> siter = srcs.iterator();
				while (siter.hasNext()) {
					if (!open) { 
            LOG.error("In flush(), but !open");
            return;
          }

					InMemoryBufferExchangeSource src = siter.next();
					if (!buffer.isServiced(src.destination()) && 
                buffer.getToService().contains(src.destination().getTaskID())) { 
            LOG.info("Sending buffer " + buffer.header().owner().toString() + 
                     " to " + src.destination() + 
                     " eof " + buffer.header().eof() + 
                     " progress " + buffer.header().progress());
						BufferExchange.Transfer result = src.send(buffer);
						if (result == BufferExchange.Transfer.TERMINATE) {
              LOG.info(this + "Terminating after " + buffer.header().owner().toString());
							//LOG.info("Terminating " + this);
							close();
							return;
						} else if (BufferExchange.Transfer.RETRY == result) {
              LOG.info("Will retry on " + buffer.header().owner().toString() + " to " + 
                       src.destination() + " address " + src.address()); 
							siter.remove();
							stalls++;
							somethingToSend = true; // Try again later.
					 // } else if (BufferExchange.Transfer.IGNORE == result) { 
					 // 	LOG.info("Tryed to send file " + buffer.header().owner() + 
           //             " to " + src.destination() + " but receiver ignored it");
              // Count an ignored buffer send as serviced, since the receive will never ask for it again
						//	buffer.serviced(src.destination());
            } else if (BufferExchange.Transfer.SUCCESS == result || 
                       BufferExchange.Transfer.IGNORE == result) {
              // Count an ignored buffer send as serviced, since the receive will never ask for it again
							LOG.info("Sent file " + buffer.header().owner() + 
                       " to " + src.destination());
							buffer.serviced(src.destination());
						}
					} else { 
            // log something about how this buffer is not meant to be serviced or was serviced
          }
				}
				
        // all outputs are now "streams" for this purpose
				if(buffer.toService() > 0 &&  // remove this later, sanity check for now
					 buffer.toService() == buffer.serviced()) {
					/* Assume no speculations for streaming. */
					try {

						// LOG.info("Garbage collect output" + buffer.header());
            LOG.info("Garbage collecting buffer " + buffer.header() + 
                     " size: " + buffer.header().compressed() + 
                     " from manager for " + this.taskid);
            LOG.info("\tServiced: " + buffer.serviced + "\n" + 
                     "\ttoService: " + buffer.toService);
            // remove it from outputs prior to deleting it
					  synchronized(this) {
						  this.outputs.remove(buffer);
					  }
            buffer.delete(); // hopefully the GC cleans this up in short order
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			this.stallfraction = stalls / src_cnt;
		}
	}

	/* The host TaskTracker object. */
  private TaskTracker tracker;

  /* The local file system, containing the output files. */
  //private FileSystem localFs;
    
  //private FileSystem rfs;

  /* Used to accept connections made be fellow BufferController
   * objects. The connections are used to transfer BufferRequest
   * objects. */
  private Thread acceptor;

  /* Manages the transfer of BufferRequest objects. */
  private RequestTransfer requestTransfer;

  /* Used to execute BufferManager objects. */
	private Executor executor;

	/* The RPC interface server that tasks use to communicate
	 * with the BufferController via the InMemoryBufferUmbilicalProtocol 
	 * interface. */
	private Server server;

	private ServerSocketChannel channel;

	/* The port number used by all BufferController objects for
	 * accepting BufferRequest objects. */
	private int controlPort;

	/* The host name. */
	private String hostname;

	/* InMemoryManagers for job level requests (i.e., reduce requesting map outputs). */
	private Map<JobID, Set<InMemoryBufferExchangeSource>> mapSources;

	/* InMemoryManagers for task level requests (i.e., a map requesting the output of a reduce). */
	//private Map<TaskID, Set<InMemoryBufferExchangeSource>> reduceSources;

	/* Each task will have a file manager associated with it. */
	private Map<JobID, Map<TaskAttemptID, BufferManager>> bufferManagers;
	
	private BlockingQueue<OutputInMemoryBuffer> queue;
	
	private Thread serviceQueue;

	public InMemoryManager(TaskTracker tracker) throws IOException {
		this.tracker   = tracker;
		this.requestTransfer = new RequestTransfer();
		this.executor  = Executors.newCachedThreadPool();
		this.mapSources    = new HashMap<JobID, Set<InMemoryBufferExchangeSource>>();
		//this.reduceSources = new HashMap<TaskID, Set<InMemoryBufferExchangeSource>>();
		this.bufferManagers  = new ConcurrentHashMap<JobID, Map<TaskAttemptID, BufferManager>>();
		this.hostname      = InetAddress.getLocalHost().getCanonicalHostName();
		
		this.queue = new LinkedBlockingQueue<OutputInMemoryBuffer>();
	}

	public static InetSocketAddress getControlAddress(Configuration conf) {
    InetSocketAddress retVal = null;
		try {
			int port = conf.getInt("mapred.buffer.manager.data.port", 9021);
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			address += ":" + port;
			retVal = NetUtils.createSocketAddr(address);
      //LOG.info("local hostname " + InetAddress.getLocalHost());
      //LOG.info("Canonical hostname " + InetAddress.getLocalHost().getCanonicalHostName());
      InetAddress[] allAddresses = 
        InetAddress.getAllByName(InetAddress.getLocalHost().getCanonicalHostName());
        /*
      for (InetAddress oneAddress : allAddresses) { 
        LOG.info("  address: " + oneAddress);
      }
      */
		} catch (Throwable t) {
			retVal = NetUtils.createSocketAddr("localhost:9021");
		}

    //LOG.info("getControlAddress returning address " + retVal);
    return retVal;
	}

	public static InetSocketAddress getServerAddress(Configuration conf) {
    InetSocketAddress retVal = null;
		try {
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			int port = conf.getInt("mapred.buffer.manager.control.port", 9020);
			address += ":" + port;
		  retVal = NetUtils.createSocketAddr(address);
      InetAddress[] allAddresses = 
        InetAddress.getAllByName(InetAddress.getLocalHost().getCanonicalHostName());
		} catch (Throwable t) {
			retVal = NetUtils.createSocketAddr("localhost:9020");
		}
    return retVal;
	}

	public void open() throws IOException {
		Configuration conf = tracker.conf();
		int maxMaps = conf.getInt("mapred.tasktracker.map.tasks.maximum", 4);
		int maxReduces = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 2);

		InetSocketAddress serverAddress = getServerAddress(conf);
    LOG.info("RPC.getServer starting with " + (maxMaps + maxReduces) + " concurrent threads");
		this.server = RPC.getServer(this, serverAddress.getHostName(), serverAddress.getPort(),
				maxMaps + maxReduces, false, conf);
		this.server.start();

		this.requestTransfer.setPriority(Thread.MAX_PRIORITY);
		this.requestTransfer.start();

		/** The server socket and selector registration */
		InetSocketAddress controlAddress = getControlAddress(conf);
		this.controlPort = controlAddress.getPort();
		this.channel = ServerSocketChannel.open();
		this.channel.socket().bind(controlAddress);

		this.acceptor = new Thread() {
			@Override
			public void run() {
				while (!isInterrupted()) {
					SocketChannel connection = null;
					try {
						connection = channel.accept();
						DataInputStream in = new DataInputStream(connection.socket().getInputStream());
						int numRequests = in.readInt();
            LOG.info("numRequests: " + numRequests);
						for (int i = 0; i < numRequests; i++) {
							BufferRequest request = BufferRequest.read(in);
							if (request instanceof ReduceBufferRequest) {
								add((ReduceBufferRequest) request);
							} else if (request instanceof MapBufferRequest) {
								add((MapBufferRequest) request);
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					finally {
						try {
							if (connection != null) connection.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		};
		this.acceptor.setDaemon(true);
		this.acceptor.setPriority(Thread.MAX_PRIORITY);
		this.acceptor.start();
		
		this.serviceQueue = new Thread() {
			public void run() {
				List<OutputInMemoryBuffer> service = new ArrayList<OutputInMemoryBuffer>();
				while (!isInterrupted()) {
					try {
						OutputInMemoryBuffer o = queue.take();
						service.add(o);
						queue.drainTo(service);
						for (OutputInMemoryBuffer buffer : service) {
							try {
								if (buffer != null) add(buffer);
							} catch (Throwable t) {
								t.printStackTrace();
								LOG.error("Error service buffer: " + buffer + ". " + t);
							}
						}
					} catch (Throwable t) {
						t.printStackTrace();
						LOG.error(t);
					}
					finally {
						service.clear();
					}
				}
				LOG.info("Service queue thread exit.");
			}
		};
		this.serviceQueue.setPriority(Thread.MAX_PRIORITY);
		this.serviceQueue.setDaemon(true);
		this.serviceQueue.start();
	}

	public void close() {
		this.serviceQueue.interrupt();
		this.acceptor.interrupt();
		this.server.stop();
		this.requestTransfer.interrupt();
		try { 
      this.channel.close();
		} catch (Throwable t) {
    }
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
	throws IOException {
		return 0;
	}
	
	public void free(TaskAttemptID tid) {
		synchronized (this) {
			JobID jobid = tid.getJobID();
			if (bufferManagers.containsKey(jobid)) {
				Map<TaskAttemptID, BufferManager> bm_map = this.bufferManagers.get(jobid);
				if (bm_map.containsKey(tid)) {
					bm_map.get(tid).close();
					//bm_map.get(tid).delete();
          LOG.info("Freeing buffers for tid: " + tid);
				}
			}
		}
	}

	public void free(JobID jobid) {
		synchronized (this) {
			LOG.info("BufferController freeing job " + jobid);

			/* Close all file managers. */
			if (this.bufferManagers.containsKey(jobid)) {
				Map<TaskAttemptID, BufferManager> bm_map = this.bufferManagers.get(jobid);
				for (BufferManager bm : bm_map.values()) {
					bm.close();
				}
				this.bufferManagers.remove(jobid);
			}

			/* blow away map sources. */
			if (this.mapSources.containsKey(jobid)) {
				this.mapSources.remove(jobid); 
			}
		}
	}

	@Override
	public float stallFraction(TaskAttemptID owner) throws IOException {
		BufferManager manager = null;

		if (bufferManagers.containsKey(owner.getJobID())) {
			Map<TaskAttemptID, BufferManager> bm_map = bufferManagers.get(owner.getJobID());
			if (bm_map.containsKey(owner)) {
				manager = bm_map.get(owner);
			}
		}

		return manager != null ? manager.stallfraction() : 0f;
	}

	@Override
	public void output(OutputInMemoryBuffer buffer) throws IOException {
		if (buffer != null) {
      LOG.info("just received buffer: " + buffer.header().owner().toString() + 
               " eof: " + buffer.header().eof() + 
               " progress " + buffer.header().progress() + 
               " data size " + buffer.data().capacity() + 
               " index size " + buffer.index().capacity() + 
               " toService (size) " + buffer.toService());
			this.queue.add(buffer);
		} else { 
      LOG.info("Just received a null Buffer. That's not good");
    }
	}
	
	@Override
	public void request(BufferRequest request) throws IOException {
		if (!request.srcHost().equals(hostname)) {
      LOG.info("Servicing remote buffer request: " + request.toString());
			requestTransfer.transfer(request); // request is remote.
		} else {
			if (request instanceof ReduceBufferRequest) {
        LOG.info("WTF? local ReduceBufferRequest: " + request.toString());
				add((ReduceBufferRequest) request);
			} else if (request instanceof MapBufferRequest) {
        LOG.info("Servicing local MapBufferRequest: " + request.toString());
				add((MapBufferRequest) request);
			}
		}
	}
	

	/******************** PRIVATE METHODS ***********************/
	
	private void add(OutputInMemoryBuffer buffer) throws IOException {
		TaskAttemptID taskid = buffer.header().owner();
		JobID jobid = taskid.getJobID();
		if (!bufferManagers.containsKey(jobid)) {
			LOG.info("Creating entry in bufferManagers for job " + jobid);
			bufferManagers.put(jobid, new ConcurrentHashMap<TaskAttemptID, BufferManager>());
		}
		
		Map<TaskAttemptID, BufferManager> bufferManager = bufferManagers.get(jobid);
		if (!bufferManager.containsKey(taskid)) {
			LOG.info("Create new BufferManager for task " + taskid);
      BufferManager bm = null;
			synchronized (this) {
				bm = new BufferManager(taskid);
				bufferManager.put(taskid, bm);

				/* pick up any outstanding requests and begin service thread */
				register(bm); 
			}
      // Set, in the BufferManager, which Reduce tasks to actually send data to
      bm.setToService(buffer.getToService());
		}
		bufferManager.get(taskid).add(buffer);
	}

	private void add(ReduceBufferRequest request) throws IOException {
    LOG.error("Just received a ReduceBufferRequest. That should not be");
		if (request.srcHost().equals(hostname)) {
			LOG.info("Register " + request);
			synchronized (this) {
				register(request);
			}
		} else {
			LOG.error("Request is remote!");
		}
	}

	private void add(MapBufferRequest request) throws IOException {
		if (request.srcHost().equals(hostname)) {
			LOG.info("Register MBR " + request);
			synchronized (this) {
				register(request);
			}
		} else {
			LOG.error("Request is remote!");
		}
	}

	private void register(MapBufferRequest request) throws IOException {
		JobID jobid = request.mapJobId();
		JobConf job = tracker.getJobConf("buck", jobid);
		//JobConf job = tracker.getJobConf(jobid);
		InMemoryBufferExchangeSource source = InMemoryBufferExchangeSource.factory(job, request);
		if (!this.mapSources.containsKey(jobid)) {
      LOG.info("Adding first map source for job " + jobid);
			this.mapSources.put(jobid, new HashSet<InMemoryBufferExchangeSource>());
			this.mapSources.get(jobid).add(source);
		} else if (!this.mapSources.get(jobid).contains(source)) {
      LOG.info("Adding map source for job " + source);
			this.mapSources.get(jobid).add(source);
		} else {
			LOG.debug("BufferController: request manager already exists." + request);
			source = null;
		}

		if (source != null) {
			if (this.bufferManagers.containsKey(jobid)) {
			  LOG.info("Adding request from " + source + " for all BufferManagers for job " + jobid);
				for (BufferManager bm : this.bufferManagers.get(jobid).values()) {
          // only add this source if it's meant to be serviced
          //debug only
          // TODO --jbuck make this work for task attempts that are rerun 
          LOG.debug("bm " + bm + " has toService.size(): " + bm.getToService().size() +  
                    " : " + bm.getToService());

          if (bm.getToService().contains(source.destination().getTaskID())) { 
            LOG.info("    Adding bm " + bm + " for source " + source);
					  bm.add(source);
          } else { 
            LOG.debug("    bm " + bm + " should not service source " + source);
          }
				}
			}
		}
	}

	private void register(ReduceBufferRequest request) throws IOException {
		LOG.error("BufferController register reduce request " + request + 
              ". WTF, this should not happen");
	}

	private void register(BufferManager bm) throws IOException {
		JobID jobid = bm.taskid.getJobID();
		if (bm.taskid.isMap()) {
			if (this.mapSources.containsKey(jobid)) {
        LOG.info("register() adding buffermanager for maps for job " + jobid);
				for (InMemoryBufferExchangeSource source : this.mapSources.get(jobid)) {
					bm.add(source);
				}
			}
		} else {
      LOG.error("WTF, this is a bm for a reduce task. This should not happen");
		}
		executor.execute(bm);
	}
}
