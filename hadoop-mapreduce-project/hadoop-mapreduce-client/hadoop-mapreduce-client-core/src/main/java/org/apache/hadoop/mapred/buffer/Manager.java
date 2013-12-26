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
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
//import org.apache.hadoop.mapred.buffer.net.BufferExchange;
//import org.apache.hadoop.mapred.buffer.net.BufferExchangeSource;
//import org.apache.hadoop.mapred.buffer.net.BufferRequest;
//import org.apache.hadoop.mapred.buffer.net.MapBufferRequest;
//import org.apache.hadoop.mapred.buffer.net.ReduceBufferRequest;
//import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.net.NetUtils;

public class Manager implements BufferUmbilicalProtocol {
	private static final Log LOG = LogFactory.getLog(Manager.class.getName());
  private Configuration conf;
  private String hostname;
  private Server server;
  private int controlPort;
  private Thread serviceQueue;
  private ServerSocketChannel channel;

	public Manager(Configuration conf) throws IOException {
    this.conf = conf;
		this.hostname      = InetAddress.getLocalHost().getCanonicalHostName();
	}

	public static InetSocketAddress getControlAddress(Configuration conf) {
		try {
			int port = conf.getInt("mapred.buffer.manager.data.port", 9021);
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			address += ":" + port;
			return NetUtils.createSocketAddr(address);
		} catch (Throwable t) {
			return NetUtils.createSocketAddr("localhost:9021");
		}
	}

	public static InetSocketAddress getServerAddress(Configuration conf) {
		try {
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			int port = conf.getInt("mapred.buffer.manager.control.port", 9020);
			address += ":" + port;
			return NetUtils.createSocketAddr(address);
		} catch (Throwable t) {
			return NetUtils.createSocketAddr("localhost:9020");
		}
	}

	public void open() throws IOException {
		//Configuration conf = this.conf;
		int maxMaps = conf.getInt("mapred.tasktracker.map.tasks.maximum", 2);
		int maxReduces = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 1);

		InetSocketAddress serverAddress = getServerAddress(conf);
		this.server = 
          new RPC.Builder(conf).setProtocol(BufferUmbilicalProtocol.class)
            .setInstance(this).setBindAddress("0.0.0.0")
            .setPort(0).setNumHandlers(maxMaps + maxReduces)
                //conf.getInt(MRJobConfig.MR_AM_TASK_LISTENER_THREAD_COUNT,
                 //   MRJobConfig.DEFAULT_MR_AM_TASK_LISTENER_THREAD_COUNT))
                    .setVerbose(false)//.setSecretManager(jobTokenSecretManager)
                    .build();
		this.server.start();

		/** The server socket and selector registration */
		InetSocketAddress controlAddress = getControlAddress(conf);
		this.controlPort = controlAddress.getPort();
		this.channel = ServerSocketChannel.open();
		this.channel.socket().bind(controlAddress);

		this.serviceQueue = new Thread() {
			public void run() {
				while (!isInterrupted()) {
					try {
					} catch (Throwable t) {
						t.printStackTrace();
						LOG.error(t);
					}
					finally {
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
		this.server.stop();
		try { 
      this.channel.close(); 
		} catch (Throwable t) {}
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
	throws IOException {
		return 0;
	}

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this,
        protocol, clientVersion, clientMethodsHash);
  }
	
	public void free(TaskAttemptID tid) {
		synchronized (this) {
		}
	}

	public void free(JobID jobid) {
		synchronized (this) {
		}
	}

  @Override
  public void printLogMessage(String output) throws IOException { 
    LOG.info(output);
  }

	@Override
	public float stallFraction(TaskAttemptID owner) throws IOException {
		return 0f;
	}

	@Override
	//public void output(OutputFile file) throws IOException {
	public void output(byte[] file) throws IOException {
	}
	
	@Override
	public void request(byte[] request) throws IOException {
	}
	

	/******************** PRIVATE METHODS ***********************/
	
  /*
	// private void add(OutputFile file) throws IOException {
	private void add(byte[] file) throws IOException {
	}

	//private void add(byte[] request) throws IOException {
	}

	//private void add(byte[] request) throws IOException {
	//}

	private void register(byte[] request) throws IOException {
	}

	private void register(byte[] request) throws IOException {
	}

	private void register(byte[] fm) throws IOException {
	}
  */
}
