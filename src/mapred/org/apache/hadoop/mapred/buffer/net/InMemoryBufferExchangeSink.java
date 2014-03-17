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

package org.apache.hadoop.mapred.buffer.net;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.buffer.OutputInMemoryBuffer;
import org.apache.hadoop.util.Progress;


/**
 * Manages the receipt of output buffers from other tasks.
 * Any task that takes its input from other tasks will create one
 * of these objects. The address field in all BufferRequest objects
 * will contain the address associated with this object.
 * 
 * If the input consists of snapshots, then we will create a SnapshotManager
 * object.
 * @author tcondie
 *
 * @param <K> The input key type.
 * @param <V> The input value type.
 */
public class InMemoryBufferExchangeSink<K extends Object, V extends Object> implements BufferExchange {
	
	private static class Position<N extends Number> {
		N position;
		
		Position(N position) {
			this.position = position;
		}
		
		void set(N position) {
			this.position = position;
		}
		
		float floatValue() {
			return this.position.floatValue();
		}
		
		long longValue() {
			return this.position.longValue();
		}
		
		int intValue() {
			return this.position.intValue();
		}
	}
	
	private static final Log LOG = LogFactory.getLog(InMemoryBufferExchangeSink.class.getName());

	/* The job configuration w.r.t. the receiving task. */
	private final JobConf conf;
	
	private final Progress progress;

	/* The identifier of that receiving task. */
	private TaskAttemptID ownerid;

	/* The maximum number of incoming connections. */
	private int maxConnections;

	/* An executor for running incoming connections. */
	private Executor executor;

	/* A thread for accepting new connections. */
	private MyThread acceptor;

	/* The channel used for accepting new connections. */
	private ServerSocketChannel server;

	private InputCollector<K, V> collector;

	/* All live task handlers. */
	private Set<Handler> handlers;

	/* The identifiers of the tasks that have sent us their
	 * complete output. */
	private Set<TaskID> successful;
	
	/* The total number of inputs we expect. e.g., number of maps in
	   the case that the owner is a reduce task. */
	private int numInputs;
	private Map<TaskID, Float> inputProgress;
	private float progressSum = 0f;
	
	/* The current input position along each input task (max aggregate over attempts). */
	private Map<TaskID, Position> cursor;

	/* The task that owns this sink and is receiving the input. */
	private Task task;

  /* The map task dependencies of the associated Reduce Task for this sink */
  //int[] mapTaskDependencies;
  protected HashSet<TaskID> mapTaskIDs;


	public InMemoryBufferExchangeSink(JobConf conf,
			           InputCollector<K, V> collector,
			           Task task)
	throws IOException {
		this.conf = conf;
		this.progress = new Progress();
		this.ownerid = task.getTaskID();
		this.collector = collector;
		this.maxConnections = conf.getInt("mapred.reduce.parallel.copies", 20);

		this.task = task;
    //this.numInputs = task.getNumberOfInputs();
    this.numInputs = conf.getNumMapTasks();
    this.inputProgress = new HashMap<TaskID, Float>();
     
    this.cursor = new HashMap<TaskID, Position>();
	    
		this.executor = Executors.newFixedThreadPool(Math.min(maxConnections, Math.max(numInputs, 5)));
		this.handlers = Collections.synchronizedSet(new HashSet<Handler>());
		this.successful = Collections.synchronizedSet(new HashSet<TaskID>());

    int[] mapTaskDependencies = ((ReduceTask)task).getDependencies();
    LOG.info(this + " this sink depends on " + mapTaskDependencies.length + " mapTasks");
    for (int i=0; i<mapTaskDependencies.length; i++) { 
      LOG.info("  " + mapTaskDependencies[i]);
    }
    
    // now turn those map task ids into 
    TaskID reducerID = this.task.getTaskID().getTaskID();
    // and then create the corresponding map TaskIDs for the dependencies 
    this.mapTaskIDs = new HashSet<TaskID>(mapTaskDependencies.length);
    for (int i=0; i<mapTaskDependencies.length; i++) { 
      TaskID tempID = new TaskID(reducerID.getJobID(),true,mapTaskDependencies[i]);
      //LOG.info("  " + tempID);
      this.mapTaskIDs.add(tempID);
    }
    //LOG.info("mapTaskIDs size: " + this.mapTaskIDs.size());

		/* The server socket and selector registration */
		this.server = ServerSocketChannel.open();
		this.server.configureBlocking(true);
		this.server.socket().bind(new InetSocketAddress(0));
	}

	public InetSocketAddress getAddress() {
		try {
			String host = InetAddress.getLocalHost().getCanonicalHostName();
			return new InetSocketAddress(host, this.server.socket().getLocalPort());
		} catch (UnknownHostException e) {
			return new InetSocketAddress("localhost", this.server.socket().getLocalPort());
		}
	}
	
	public Progress getProgress() {
		return this.progress;
	}

	/* Create a new thread for accepting new connections. */
	protected class MyThread extends Thread {
    private HashSet<TaskID> mapTaskIDs = null;

    public MyThread() { 
      super();
      LOG.info("Starting a new MyThread");
    }

    public void setMapTaskIDs(HashSet<TaskID> mapTaskIDs) { 
      this.mapTaskIDs = mapTaskIDs;
    }

		public void run() {
			try {
 				while (server.isOpen()) {
          LOG.info("Top of server.isOpen()");
 					SocketChannel channel = server.accept();
 					channel.configureBlocking(true);
 					/* Note: no buffered input stream due to memory pressure. */
          LOG.info("  log 2");
 					DataInputStream  istream = new DataInputStream(channel.socket().getInputStream());
 					DataOutputStream ostream = 
               new DataOutputStream(new BufferedOutputStream(channel.socket().getOutputStream()));
          LOG.info("  log 3");
 					if (complete()) {
             LOG.info("in run, complete() is true, closing ostream");
 						WritableUtils.writeEnum(ostream, Connect.BUFFER_COMPLETE);
 						ostream.close();
 					} else if (handlers.size() > maxConnections) {
 						LOG.info("Connections full. connections = " + handlers.size() + 
 								 ", max allowed " + maxConnections);
 						WritableUtils.writeEnum(ostream, Connect.CONNECTIONS_FULL);
 						ostream.close();
 					} else {
 						WritableUtils.writeEnum(ostream, Connect.OPEN);
 						ostream.flush();
 						
 						BufferExchange.BufferType type = 
                WritableUtils.readEnum(istream, BufferExchange.BufferType.class);
 						Handler handler = null;

             if (BufferType.INMEMORY == type) {
               LOG.info("hander is InMemoryHandler");
 							handler = new InMemoryHandler(collector, istream, ostream, this.mapTaskIDs);
 						} else {
 							LOG.error("Unknown buffer type " + type);
 							channel.close();
 							continue;
 						}
 						
 						LOG.info("InMemoryBufferSink: " + ownerid + " opening connection. Handler size: " + handlers.size());
 						handlers.add(handler);
 						executor.execute(handler);
 					}
 				}
 				LOG.info("InMemoryBufferSink " + ownerid + " buffer response server closed.");
 			} catch (IOException e) { 
 				if (!complete()) {
 					e.printStackTrace();
 				}
 			}
 		}
 	}  // public MyThread

	/** Open the sink for incoming connections. */
	public void open() {

		this.acceptor = new MyThread();
    acceptor.setMapTaskIDs(this.mapTaskIDs);
		acceptor.setDaemon(true);
		acceptor.setPriority(Thread.MAX_PRIORITY);
		acceptor.start();
	}

	/**
	 * Close sink.
	 * @throws IOException
	 */
	public synchronized void close() throws IOException {
		LOG.info("JBufferSink is closing.");
		if (this.acceptor == null) { 
      LOG.info("  JK, already done");
      return; // Already done. 
    }
		try {
			this.acceptor.interrupt();
			this.server.close();
			this.acceptor = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Are we done yet?
	 * @return true if all inputs have sent all their input.
	 */
	public boolean complete() {
    synchronized(this.successful) { 
      LOG.info("in sink.complete(), size: " + this.successful.size() + 
              " numInputs " + numInputs);
		  return this.successful.size() >= numInputs;
    }
	}

	/**
	 * Connection is done.
	 * @param connection The completed connection.
	 */
	private void done(Handler handler) {
    LOG.info("handler " + handler + " done, current size: " + this.handlers.size());
		this.handlers.remove(handler);
	}

	private void updateProgress(OutputInMemoryBuffer.Header header) {
		TaskID taskid = header.owner().getTaskID();
		LOG.info("Task " + taskid + ": copy from "  + header.owner() + 
             " progress "+ header.progress());
		if (inputProgress.containsKey(taskid)) {
			progressSum -= inputProgress.get(taskid);
		} 
			
		inputProgress.put(taskid, header.progress());
		progressSum += header.progress();
		
		if (header.eof()) {
      synchronized(successful) { 
			  successful.add(header.owner().getTaskID());
			  LOG.info(successful.size() + " completed connections. " +
					  (numInputs - successful.size()) + " remaining.");
      }
		} else { 
      LOG.info("Header for " + taskid + " is not eof()");
    }

		if (complete()) {
			this.progress.complete();
			this.collector.close();
      LOG.info("in updateProgress, complete() is true, closing collector");
		} else {
			LOG.info("Task " + taskid + " total copy progress = " + (progressSum / (float) numInputs));
			this.progress.set(progressSum / (float) numInputs);
		}
		LOG.info("Task " + taskid + " total sink progress = " + progress.get());
	}

	/************************************** CONNECTION CLASS **************************************/
	
	abstract class Handler<H extends OutputInMemoryBuffer.Header> implements Runnable {
		protected InputCollector<K, V> collector;
		
		protected DataInputStream istream;
		
		protected DataOutputStream ostream;

    protected HashSet<TaskID> mapTaskIDs;
		
		protected Handler(InputCollector<K, V> collector,
				          DataInputStream istream, DataOutputStream ostream,
                  HashSet<TaskID> mapTaskIDs) { 
			this.collector = collector;
			this.istream = istream;
			this.ostream = ostream;
      this.mapTaskIDs = mapTaskIDs;
		}
		
		public final void close() {
			try {
				LOG.debug("Close. Owner task " + task.getTaskID());
				if (this.istream != null) {
					this.istream.close();
					this.istream = null;
				}
				
				if (this.ostream != null) {
					this.ostream.close();
					this.ostream = null;
				}
			} catch (IOException e) {
				LOG.error("Close exception -- " + e);
			}
		}
		
		public final void run() {
			try {
				int open = Integer.MAX_VALUE;
        H header = null;
				while (open == Integer.MAX_VALUE) {
					try {
						LOG.info(this + " Waiting for open signal.");
						open = istream.readInt();

            LOG.info(this + " top of while loop, open: " + open);
            
						if (open == Integer.MAX_VALUE) {
              LOG.info(this + " in run, and open == MAX_VALUE");
							header = (H) OutputInMemoryBuffer.Header.readHeader(istream);
							LOG.info(this + " Handler received " + header.compressed() + " bytes. header: " + header);
						  receive(header);
						} else if (open == 0) {
              if (header != null) { 
                LOG.info(this + " in run, received close signal from: " + header.owner());
              } else { 
                LOG.info(this + " in run, received close signal but no idea who from");
              }
            } else { 
              LOG.info(this + " in run, but open != MAX_VALUE. open: " + open);
            }
					} catch (IOException e) {
						e.printStackTrace();
						LOG.error(e);
						return;
					}
				}
			} finally {
				LOG.info(this + " Handler done");
				done(this);
				close();
			}
		}
		
		protected abstract void receive(H header) throws IOException;
		
	}
	
	final class InMemoryHandler extends Handler<OutputInMemoryBuffer.InMemoryHeader> {

		public InMemoryHandler(InputCollector<K, V> collector,
				           DataInputStream istream, DataOutputStream ostream,
                   HashSet<TaskID> mapTaskIDs) {
			super(collector, istream, ostream, mapTaskIDs);
		}
		
	//public void receive(OutputInMemoryBuffer.FileHeader header) throws IOException 
		public void receive(OutputInMemoryBuffer.InMemoryHeader header) throws IOException {
      LOG.info("In InMemoryBufferExchangeSink.receive()");
			// Get my position for this source taskid. 
			Position position = null;
			TaskID inputTaskID = header.owner().getTaskID();
			synchronized (cursor) {
				if (!cursor.containsKey(inputTaskID)) {
					cursor.put(inputTaskID, new Position(-1));
				}
				position = cursor.get(inputTaskID);
			}

			// I'm the only one that should be updating this position. 
			int pos = position.intValue() < 0 ? header.ids().first() : position.intValue(); 
			synchronized (position) {
				if (header.ids().first() == pos && 
            this.mapTaskIDs.contains(header.owner().getTaskID())) { 
					WritableUtils.writeEnum(ostream, BufferExchange.Transfer.READY);
					ostream.flush();
					//LOG.info("InMemoryBuffer handler ready to receive -- " + header);
					if (collector.read(istream, header)) {
            LOG.info("Received header " + header);
						updateProgress(header);
						synchronized (task) {
              LOG.info("pre receive.notifyAll()");
							task.notifyAll();
              LOG.info("post receive.notifyAll()");
						}
					} else { 
            LOG.info("Issue receiving " + header);
          }
					position.set(header.ids().last() + 1);
					LOG.info("InMemoryBuffer handler done receiving up to position " + position.intValue());
				} else {
					LOG.info(this + " ignoring -- " + header);
					WritableUtils.writeEnum(ostream, BufferExchange.Transfer.IGNORE);
				}
			} 
			// Indicate the next spill file that I expect. 
			pos = position.intValue();
			LOG.info("Updating source position to " + pos);
			ostream.writeInt(pos);
			ostream.flush();
		}
	}
	
  public void setNumInputs(int numInputs) { 
    LOG.info("Setting numInputs to " + numInputs);
    this.numInputs = numInputs;
  }
}
