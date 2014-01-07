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

package org.apache.hadoop.yarn.server.rammanager;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.BufferTransferListenerImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
//import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
//import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
//import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.rammanager.metrics.RAMManagerMetrics;
//import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
//import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
//import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;

import com.google.common.annotations.VisibleForTesting;

public class RAMManager extends CompositeService 
    implements EventHandler<RAMManagerEvent>, BufferUmbilicalProtocol {

  /**
   * Priority of the RAMManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private Server server;
  private static final Log LOG = LogFactory.getLog(RAMManager.class);
  protected final RAMManagerMetrics metrics = RAMManagerMetrics.create();
  private ApplicationACLsManager aclsManager;
  private Configuration conf;
  private String hostname;
  private int controlPort;
  private Thread serviceQueue;
  private ServerSocketChannel channel;
  private Context context;
  private AsyncDispatcher dispatcher;
  private static CompositeServiceShutdownHook ramManagerShutdownHook; 
  
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  
  public RAMManager(Configuration conf) {
    super(RAMManager.class.getName());
    this.conf = conf;
		this.hostname = InetAddress.getLocalHost().getCanonicalHostName();
    LOG.info("RAMManager (), hostname is " + hostname);
  }

	public static InetSocketAddress getControlAddress(Configuration conf) {
		try {
			int port = conf.getInt("mapred.buffer.manager.control.port", 9020);
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			address += ":" + port;
			return NetUtils.createSocketAddr(address);
		} catch (Throwable t) {
			return NetUtils.createSocketAddr("localhost:9020");
		}
	}

	public static InetSocketAddress getServerAddress(Configuration conf) {
		try {
			int port = conf.getInt("mapred.buffer.manager.data.port", 9021);
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			address += ":" + port;
			return NetUtils.createSocketAddr(address);
		} catch (Throwable t) {
			return NetUtils.createSocketAddr("localhost:9021");
		}
	}

	public void open() throws IOException {

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
		return 1;
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
    LOG.info("RAMManager printLogMessage: " + output);
  }

	@Override
	public float stallFraction(TaskAttemptID owner) throws IOException {
		return 0f;
	}

	@Override
	public void output(byte[] file) throws IOException {
	}
	
	@Override
	public void request(byte[] request) throws IOException {
	}

  /*
  protected NodeStatusUpdater createNodeStatusUpdater(Context context,
      Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
    return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
      metrics);
  }
  */

  /*
  protected NodeResourceMonitor createNodeResourceMonitor() {
    return new NodeResourceMonitorImpl();
  }
  */
  /*
  protected ContainerManagerImpl createContainerManager(Context context,
      ContainerExecutor exec, DeletionService del,
      NodeStatusUpdater nodeStatusUpdater, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
      metrics, aclsManager, dirsHandler);
  }
  */

  /*
  protected WebServer createWebServer(Context rammContext,
      ResourceView resourceView, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    return new WebServer(rammContext, resourceView, aclsManager, dirsHandler);
  }
  */

/*
  protected DeletionService createDeletionService(ContainerExecutor exec) {
    return new DeletionService(exec);
  }
*/
  /*
  protected RAMManagerService createRAMManagerService(ContainerExecutor exec) {
    return new RAMManagerService(exec);
  }
  */

  protected RAMMContext createRAMMContext(
      Configuration conf
     // RAMMContainerTokenSecretManager containerTokenSecretManager,
     // RAMMTokenSecretManagerInNM rammTokenSecretManager) 
    ) { 
    //return new RAMMContext(containerTokenSecretManager, rammTokenSecretManager);
    return new RAMMContext(conf);
  }

  protected void doSecureLogin() throws IOException {
    SecurityUtil.login(getConfig(), YarnConfiguration.NM_KEYTAB,
        YarnConfiguration.NM_PRINCIPAL);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    this.context = createRAMMContext(conf);

    this.aclsManager = new ApplicationACLsManager(conf);

    // RAMManager level dispatcher
    this.dispatcher = new AsyncDispatcher();

    dispatcher.register(RAMManagerEventType.class, this);
    addService(dispatcher);

    DefaultMetricsSystem.initialize("RAMManager");

    // TODO -jbuck is this correct?
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
    
    super.serviceInit(conf);
    // TODO add local dirs to del
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed RAMManager login", e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (isStopping.getAndSet(true)) {
      return;
    }
    super.serviceStop();
    DefaultMetricsSystem.shutdown();
  }

  public String getName() {
    return "RAMManager";
  }

  protected void shutDown() {
    new Thread() {
      @Override
      public void run() {
        RAMManager.this.stop();
      }
    }.start();
  }
                                       
/*
  protected void resyncWithRM() {
    //we do not want to block dispatcher thread here
    new Thread() {
      @Override
      public void run() {
        LOG.info("Notifying ContainerManager to block new container-requests");
        containerManager.setBlockNewContainerRequests(true);
        LOG.info("Cleaning up running containers on resync");
        containerManager.cleanupContainersOnNMResync();
        ((NodeStatusUpdaterImpl) nodeStatusUpdater ).rebootNodeStatusUpdater();
      }
    }.start();
  }
*/

  public static class RAMMContext implements Context {

    private NodeId nodeId = null;
    //private WebServer webServer;
    private Configuration conf;

    public RAMMContext(Configuration conf) {
      this.conf = conf;
    }

    /**
     * Usable only after ContainerManager is started.
     */
    @Override
    public NodeId getNodeId() {
      return this.nodeId;
    }

    @Override
    public int getHttpPort() {
      //return this.webServer.getPort();
      return 80; //TODO fix this -jbuck
    }

    /*
    @Override // TODO get this working -jbuck
    public BufferManagementProtocol getBufferManager() {
      return this.containerManager;
    }
    */

    /*
    public void setWebServer(WebServer webServer) {
      this.webServer = webServer;
    }
    */

    public void setNodeId(NodeId nodeId) {
      this.nodeId = nodeId;
    }
  }

  private void initAndStartRAMManager(Configuration conf, boolean hasToReboot) {
    try {

      // Remove the old hook if we are rebooting.
      if (hasToReboot && null != ramManagerShutdownHook) {
        ShutdownHookManager.get().removeShutdownHook(ramManagerShutdownHook);
      }

      ramManagerShutdownHook = new CompositeServiceShutdownHook(this);
      ShutdownHookManager.get().addShutdownHook(ramManagerShutdownHook,
                                                SHUTDOWN_HOOK_PRIORITY);

      this.init(conf);
      this.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting RAMManager", t);
      System.exit(-1);
    }
  }

  @Override
  public void handle(RAMManagerEvent event) {
    LOG.info("in RAMManager.handle()");
    switch (event.getType()) {
    case SHUTDOWN:
      shutDown();
      break;
    default:
      LOG.warn("Invalid event " + event.getType() + ". Ignoring.");
    }
  }
  
  // For testing
  RAMManager createNewRAMManager() {
    return new RAMManager();
  }
  
  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(RAMManager.class, args, LOG);
    RAMManager nodeManager = new RAMManager();
    Configuration conf = new YarnConfiguration();
    setHttpPolicy(conf);
    nodeManager.initAndStartRAMManager(conf, false);
  }
  
  private static void setHttpPolicy(Configuration conf) {
    HttpConfig.setPolicy(Policy.fromString(conf.get(
      YarnConfiguration.YARN_HTTP_POLICY_KEY,
      YarnConfiguration.YARN_HTTP_POLICY_DEFAULT)));
  }

  void printLogMessage(String output) throws IOException {
    LOG.info("in RAMManager.printLogMessage(): " + output);
  }
}
