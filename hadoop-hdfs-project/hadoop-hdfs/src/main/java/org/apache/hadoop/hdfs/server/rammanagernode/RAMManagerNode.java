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
package org.apache.hadoop.hdfs.server.rammanagernode;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtil.ConfiguredNNAddress;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DomainPeerServer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ClientDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.ClientRAMManagernodeProtocolProtos.ClientRAMManagernodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DNTransferAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InterDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocolPB.*;
import org.apache.hadoop.hdfs.security.token.block.*;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
//import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.rammanagernode.metrics.RAMManagerNodeMetrics;
import org.apache.hadoop.hdfs.server.datanode.web.resources.DatanodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.mortbay.util.ajax.JSON;

import java.io.*;
import java.net.*;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**********************************************************
 * RAMManagerNode is based on DataNode code.
 * DataNodes maintain an open server socket so that client code 
 * or other DataNodes can read/write data.  The host/port for
 * this server is reported to the NameNode, which then sends that
 * information to clients or other DataNodes that might be interested.
 *
 **********************************************************/
@InterfaceAudience.Private
public class RAMManagerNode extends Configured 
    implements ClientRAMManagernodeProtocol {

  public static final Log LOG = LogFactory.getLog(RAMManagerNode.class);
  
  /*
  static{
    HdfsConfiguration.init();
  }
  */

  public void logMessage(String message) { 
    LOG.info("in logMessage: " + message);
  }

  public static final String RMN_CLIENTTRACE_FORMAT =
        "src: %s" +      // src IP
        ", dest: %s" +   // dst IP
        ", bytes: %s" +  // byte count
        ", op: %s" +     // operation
        ", cliID: %s" +  // DFSClient id
        ", offset: %s" + // offset
        ", srvID: %s" +  // DatanodeRegistration
        ", blockid: %s" + // block id
        ", duration: %s";  // duration time
        
  static final Log ClientTraceLog =
    LogFactory.getLog(RAMManagerNode.class.getName() + ".clienttrace");
  
  private static final String USAGE = "Usage: java RAMManagerNode";
  //static final int CURRENT_BLOCK_FORMAT_VERSION = 1;

  /**
   * Use {@link NetUtils#createSocketAddr(String)} instead.
   */
  @Deprecated
  public static InetSocketAddress createSocketAddr(String target) {
    return NetUtils.createSocketAddr(target);
  }
  
  volatile boolean shouldRun = true;
  //private BlockPoolManager blockPoolManager;
  //volatile FsDatasetSpi<? extends FsVolumeSpi> data = null;
  private String clusterId = null;

  //public final static String EMPTY_DEL_HINT = "";
  //AtomicInteger xmitsInProgress = new AtomicInteger();
  //Daemon dataXceiverServer = null;
  //Daemon localDataXceiverServer = null;
  //ThreadGroup threadGroup = null;
  private HttpServer infoServer = null;
  private int infoSecurePort;
  RAMManagerNodeMetrics metrics;
  private InetSocketAddress streamingAddr;
  private String hostName;
  private Configuration conf;

  /**
   * Create the RAMManagerNode given a configuration.
   */
   /*
  RAMManagerNode(final Configuration conf, 
           final AbstractList<File> dataDirs) throws IOException {
    this(conf, null);
  }
  
  /**
   * Create the RAMManagerNode given a configuration,
   * and a namenode proxy
   */
  RAMManagerNode(final Configuration conf 
           // final SecureResources resources) throws IOException {
           ) throws IOException {
    super(conf);

    try {
      hostName = getHostName(conf);
      LOG.info("Configured RAMManager hostname is " + hostName);
      //startRAMManagerNode(conf, resources);
      startRAMManagerNode(conf);
    } catch (IOException ie) {
      shutdown();
      throw ie;
    }
  }

  private synchronized void setClusterId(final String nsCid, final String bpid
      ) throws IOException {
    if(clusterId != null && !clusterId.equals(nsCid)) {
      throw new IOException ("Cluster IDs not matched: dn cid=" + clusterId 
          + " but ns cid="+ nsCid + "; bpid=" + bpid);
    }
    // else
    clusterId = nsCid;
  }

  /**
   * Returns the hostname for this RAMManagerNode. If the hostname is not
   * explicitly configured in the given config, then it is determined
   * via the DNS class.
   *
   * @param config
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the dfs.datanode.dns.interface
   *    option is used and the hostname can not be determined
   */
  private static String getHostName(Configuration config)
      throws UnknownHostException {
    String name = config.get(DFS_RAMMANAGERNODE_HOST_NAME_KEY); // -jbuck
    if (name == null) {
      name = DNS.getDefaultHost(
          config.get(DFS_RAMMANAGERNODE_DNS_INTERFACE_KEY,
                     DFS_RAMMANAGERNODE_DNS_INTERFACE_DEFAULT),
          config.get(DFS_RAMMANAGERNODE_DNS_NAMESERVER_KEY,
                     DFS_RAMMANAGERNODE_DNS_NAMESERVER_DEFAULT));
    }
    return name;
  }

  
  private void startInfoServer(Configuration conf) throws IOException {
    // create a servlet to serve full-file content
    InetSocketAddress infoSocAddr = RAMManagerNode.getInfoAddr(conf);
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = new HttpServer("rammanagernode", infoHost, tmpInfoPort, tmpInfoPort == 0, 
           conf, new AccessControlList(conf.get(DFS_ADMIN, " ")));

    LOG.info("Opened info server at " + infoHost + ":" + tmpInfoPort);
    if (conf.getBoolean(DFS_HTTPS_ENABLE_KEY, false)) {
      boolean needClientAuth = conf.getBoolean(DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                                               DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          DFS_DATANODE_HTTPS_ADDRESS_KEY, infoHost + ":" + 0));
      Configuration sslConf = new HdfsConfiguration(false);
      sslConf.addResource(conf.get(DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
          "ssl-server.xml"));
      this.infoServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Datanode listening for SSL on " + secInfoSocAddr);
      }
      infoSecurePort = secInfoSocAddr.getPort();
    }
    
    this.infoServer.setAttribute("rammanagernode", this);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    this.infoServer.start();
  }
  
    /*
  private void startPlugins(Configuration conf) {
    plugins = conf.getInstances(DFS_RAMMANAGERNODE_PLUGINS_KEY, ServicePlugin.class);
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
        LOG.info("Started plug-in " + p);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
  }
    */
  

  /*
  private void initIpcServer(Configuration conf) throws IOException {
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.get(DFS_RAMMANAGERNODE_IPC_ADDRESS_KEY));
    
    // Add all the RPC protocols that the Datanode implements    
    RPC.setProtocolEngine(conf, ClientRAMManagernodeProtocolPB.class,
        ProtobufRpcEngine.class);
    ClientRAMManagernodeProtocolServerSideTranslatorPB clientRAMManagernodeProtocolXlator = 
          new ClientRAMManagernodeProtocolServerSideTranslatorPB(this);
    BlockingService service = ClientRAMManagernodeProtocolService
        .newReflectiveBlockingService(clientRAMManagernodeProtocolXlator);
    ipcServer = new RPC.Builder(conf)
        .setProtocol(ClientRAMManagernodeProtocolPB.class)
        .setInstance(service)
        .setBindAddress(ipcAddr.getHostName())
        .setPort(ipcAddr.getPort())
        .setNumHandlers(
            conf.getInt(DFS_RAMMANAGERNODE_HANDLER_COUNT_KEY,
                DFS_RAMMANAGERNODE_HANDLER_COUNT_DEFAULT)).setVerbose(false)
        //.setSecretManager(blockPoolTokenSecretManager).build();
        .build();
    
    /*
    InterDatanodeProtocolServerSideTranslatorPB interDatanodeProtocolXlator = 
        new InterDatanodeProtocolServerSideTranslatorPB(this);
    service = InterDatanodeProtocolService
        .newReflectiveBlockingService(interDatanodeProtocolXlator);
    DFSUtil.addPBProtocol(conf, InterDatanodeProtocolPB.class, service,
        ipcServer);
    LOG.info("Opened IPC server at " + ipcServer.getListenerAddress());
    */
    // set service-level authorization security policy
    /*
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      ipcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
    }
  }
  */
  
  /*
  private void initDataXceiver(Configuration conf) throws IOException {
    // find free port or use privileged port provided
    TcpPeerServer tcpPeerServer;
    if (secureResources != null) {
      tcpPeerServer = new TcpPeerServer(secureResources);
    } else {
      tcpPeerServer = new TcpPeerServer(dnConf.socketWriteTimeout,
          RAMManagerNode.getStreamingAddr(conf));
    }
    tcpPeerServer.setReceiveBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    streamingAddr = tcpPeerServer.getStreamingAddr();
    LOG.info("Opened streaming server at " + streamingAddr);
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    this.dataXceiverServer = new Daemon(threadGroup, 
        new DataXceiverServer(tcpPeerServer, conf, this));
    this.threadGroup.setDaemon(true); // auto destroy when empty

    if (conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
              DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT) ||
        conf.getBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
              DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT)) {
      DomainPeerServer domainPeerServer =
                getDomainPeerServer(conf, streamingAddr.getPort());
      if (domainPeerServer != null) {
        this.localDataXceiverServer = new Daemon(threadGroup,
            new DataXceiverServer(domainPeerServer, conf, this));
        LOG.info("Listening on UNIX domain socket: " +
            domainPeerServer.getBindPath());
      }
    }
  }
  */

  /*
  static DomainPeerServer getDomainPeerServer(Configuration conf,
      int port) throws IOException {
    String domainSocketPath =
        conf.getTrimmed(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
    if (domainSocketPath.isEmpty()) {
      if (conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
            DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT) &&
         (!conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT))) {
        LOG.warn("Although short-circuit local reads are configured, " +
            "they are disabled because you didn't configure " +
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
      }
      return null;
    }
    if (DomainSocket.getLoadingFailureReason() != null) {
      throw new RuntimeException("Although a UNIX domain socket " +
          "path is configured as " + domainSocketPath + ", we cannot " +
          "start a localDataXceiverServer because " +
          DomainSocket.getLoadingFailureReason());
    }
    DomainPeerServer domainPeerServer =
      new DomainPeerServer(domainSocketPath, port);
    domainPeerServer.setReceiveBufferSize(
        HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    return domainPeerServer;
  }
  */
  
  // calls specific to BP
  /*
  protected void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivedBlock(block, delHint); 
    } else {
      LOG.error("Cannot find BPOfferService for reporting block received for bpid="
          + block.getBlockPoolId());
    }
  }
  
  // calls specific to BP
  protected void notifyNamenodeReceivingBlock(ExtendedBlock block) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivingBlock(block); 
    } else {
      LOG.error("Cannot find BPOfferService for reporting block receiving for bpid="
          + block.getBlockPoolId());
    }
  }
  */
  
  /**
   * This method starts the data node with the specified conf.
   * 
   * @param conf - the configuration
   *  if conf's CONFIG_PROPERTY_SIMULATED property is set
   *  then a simulated storage based data node is created.
   * 
   * @param dataDirs - only for a non-simulated storage data node
   * @throws IOException
   */
  void startRAMManagerNode(Configuration conf 
                     //AbstractList<File> dataDirs,
                    // DatanodeProtocol namenode,
                     //SecureResources resources
                     ) throws IOException {
    //if(UserGroupInformation.isSecurityEnabled() && resources == null) {
    if(UserGroupInformation.isSecurityEnabled()) {
      if (!conf.getBoolean("ignore.secure.ports.for.testing", false)) {
        throw new RuntimeException("Cannot start secure cluster without "
            + "privileged resources.");
      }
    }

    // settings global for all BPs in the Data Node
    //this.secureResources = resources;
    //this.dataDirs = dataDirs;
    this.conf = conf;
    //this.dnConf = new DNConf(conf);

    //storage = new DataStorage();
    
    // global DN settings
    //registerMXBean();
    //initDataXceiver(conf);
    //startInfoServer(conf);
  
    // BlockPoolTokenSecretManager is required to create ipc server.
    //this.blockPoolTokenSecretManager = new BlockPoolTokenSecretManager();
    //initIpcServer(conf);

    metrics = RAMManagerNodeMetrics.create(conf, hostName);

    //blockPoolManager = new BlockPoolManager(this);
    //blockPoolManager.refreshNamenodes(conf);

    // Create the ReadaheadPool from the DataNode context so we can
    // exit without having to explicitly shutdown its thread pool.
    //readaheadPool = ReadaheadPool.getInstance();
  }
  
  /**
   * Determine the http server's effective addr
   */
  public static InetSocketAddress getInfoAddr(Configuration conf) {
    return NetUtils.createSocketAddr(conf.get(DFS_RAMMANAGERNODE_HTTP_ADDRESS_KEY,
        DFS_RAMMANAGERNODE_HTTP_ADDRESS_DEFAULT));
  }
  
  /*
  private void registerMXBean() {
    MBeans.register("RAMManagerNode", "RAMManagerNodeInfo", this);
  }
  */
  
  @VisibleForTesting
  public int getXferPort() {
    return streamingAddr.getPort();
  }
 
  /*
  String getStorageId() {
    return storage.getStorageID();
  }
  */

  /**
   * @return name useful for logging
   */
  public String getDisplayName() {
    // NB: our DatanodeID may not be set yet
    return hostName + ":" + getXferPort();
  }

  /**
   * NB: The datanode can perform data transfer on the streaming
   * address however clients are given the IPC IP address for data
   * transfer, and that may be a different address.
   * 
   * @return socket address for data transfer
   */
  public InetSocketAddress getXferAddress() {
    return streamingAddr;
  }

  /**
   * @return the datanode's IPC port
   */
   /*
  public int getIpcPort() {
    return ipcServer.getListenerAddress().getPort();
  }
  */
  
  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
   /*
  protected Socket newSocket() throws IOException {
    return (dnConf.socketWriteTimeout > 0) ? 
           SocketChannel.open().socket() : new Socket();                                   
  }
  */

  RAMManagerNodeMetrics getMetrics() {
    return metrics;
  }
  
  /**
   * Shut down this instance of the rammanagernode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  public void shutdown() {
    /*
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
          LOG.info("Stopped plug-in " + p);
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }
    */
    
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode", e);
      }
    }
    /*
    if (ipcServer != null) {
      ipcServer.stop();
    }
    */
    
    /*
    if (dataXceiverServer != null) {
      ((DataXceiverServer) this.dataXceiverServer.getRunnable()).kill();
      this.dataXceiverServer.interrupt();
    }
    if (localDataXceiverServer != null) {
      ((DataXceiverServer) this.localDataXceiverServer.getRunnable()).kill();
      this.localDataXceiverServer.interrupt();
    }
    // wait for all data receiver threads to exit
    if (this.threadGroup != null) {
      int sleepMs = 2;
      while (true) {
        this.threadGroup.interrupt();
        LOG.info("Waiting for threadgroup to exit, active threads is " +
                 this.threadGroup.activeCount());
        if (this.threadGroup.activeCount() == 0) {
          break;
        }
        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException e) {}
        sleepMs = sleepMs * 3 / 2; // exponential backoff
        if (sleepMs > 1000) {
          sleepMs = 1000;
        }
      }
      this.threadGroup = null;
    }
    */
    /*
    if (this.dataXceiverServer != null) {
      // wait for dataXceiverServer to terminate
      try {
        this.dataXceiverServer.join();
      } catch (InterruptedException ie) {
      }
    }
    if (this.localDataXceiverServer != null) {
      // wait for localDataXceiverServer to terminate
      try {
        this.localDataXceiverServer.join();
      } catch (InterruptedException ie) {
      }
    }
    */
    
    if (metrics != null) {
      metrics.shutdown();
    }
  }
  
  /** Number of concurrent xceivers per node. */
  //@Override // DataNodeMXBean
  /*
  public int getXceiverCount() {
    return threadGroup == null ? 0 : threadGroup.activeCount();
  }
  */
  
  /*
  int getXmitsInProgress() {
    return xmitsInProgress.get();
  }
   */

  /*
  private void transferBlock(ExtendedBlock block, DatanodeInfo xferTargets[])
      throws IOException {
    BPOfferService bpos = getBPOSForBlock(block);
    DatanodeRegistration bpReg = getDNRegistrationForBP(block.getBlockPoolId());
    
    if (!data.isValidBlock(block)) {
      // block does not exist or is under-construction
      String errStr = "Can't send invalid block " + block;
      LOG.info(errStr);
      
      bpos.trySendErrorReport(DatanodeProtocol.INVALID_BLOCK, errStr);
      return;
    }

    // Check if NN recorded length matches on-disk length 
    long onDiskLength = data.getLength(block);
    if (block.getNumBytes() > onDiskLength) {
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      bpos.reportBadBlocks(block);
      LOG.warn("Can't replicate block " + block
          + " because on-disk length " + onDiskLength 
          + " is shorter than NameNode recorded length " + block.getNumBytes());
      return;
    }
    
    int numTargets = xferTargets.length;
    if (numTargets > 0) {
      if (LOG.isInfoEnabled()) {
        StringBuilder xfersBuilder = new StringBuilder();
        for (int i = 0; i < numTargets; i++) {
          xfersBuilder.append(xferTargets[i]);
          xfersBuilder.append(" ");
        }
        LOG.info(bpReg + " Starting thread to transfer " + 
                 block + " to " + xfersBuilder);                       
      }

      new Daemon(new DataTransfer(xferTargets, block,
          BlockConstructionStage.PIPELINE_SETUP_CREATE, "")).start();
    }
  }

  void transferBlocks(String poolId, Block blocks[],
      DatanodeInfo xferTargets[][]) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlock(new ExtendedBlock(poolId, blocks[i]), xferTargets[i]);
      } catch (IOException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      }
    }
  }
  */

  /* ********************************************************************
  Protocol when a client reads data from Datanode (Cur Ver: 9):
  
  Client's Request :
  =================
   
     Processed in DataXceiver:
     +----------------------------------------------+
     | Common Header   | 1 byte OP == OP_READ_BLOCK |
     +----------------------------------------------+
     
     Processed in readBlock() :
     +-------------------------------------------------------------------------+
     | 8 byte Block ID | 8 byte genstamp | 8 byte start offset | 8 byte length |
     +-------------------------------------------------------------------------+
     |   vInt length   |  <DFSClient id> |
     +-----------------------------------+
     
     Client sends optional response only at the end of receiving data.
       
  DataNode Response :
  ===================
   
    In readBlock() :
    If there is an error while initializing BlockSender :
       +---------------------------+
       | 2 byte OP_STATUS_ERROR    | and connection will be closed.
       +---------------------------+
    Otherwise
       +---------------------------+
       | 2 byte OP_STATUS_SUCCESS  |
       +---------------------------+
       
    Actual data, sent by BlockSender.sendBlock() :
    
      ChecksumHeader :
      +--------------------------------------------------+
      | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
      +--------------------------------------------------+
      Followed by actual data in the form of PACKETS: 
      +------------------------------------+
      | Sequence of data PACKETs ....      |
      +------------------------------------+
    
    A "PACKET" is defined further below.
    
    The client reads data until it receives a packet with 
    "LastPacketInBlock" set to true or with a zero length. It then replies
    to DataNode with one of the status codes:
    - CHECKSUM_OK:    All the chunk checksums have been verified
    - SUCCESS:        Data received; checksums not verified
    - ERROR_CHECKSUM: (Currently not used) Detected invalid checksums

      +---------------+
      | 2 byte Status |
      +---------------+
    
    The DataNode expects all well behaved clients to send the 2 byte
    status code. And if the the client doesn't, the DN will close the
    connection. So the status code is optional in the sense that it
    does not affect the correctness of the data. (And the client can
    always reconnect.)
    
    PACKET : Contains a packet header, checksum and data. Amount of data
    ======== carried is set by BUFFER_SIZE.
    
      +-----------------------------------------------------+
      | 4 byte packet length (excluding packet header)      |
      +-----------------------------------------------------+
      | 8 byte offset in the block | 8 byte sequence number |
      +-----------------------------------------------------+
      | 1 byte isLastPacketInBlock                          |
      +-----------------------------------------------------+
      | 4 byte Length of actual data                        |
      +-----------------------------------------------------+
      | x byte checksum data. x is defined below            |
      +-----------------------------------------------------+
      | actual data ......                                  |
      +-----------------------------------------------------+
      
      x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
          CHECKSUM_SIZE
          
      CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
      
      The above packet format is used while writing data to DFS also.
      Not all the fields might be used while reading.
    
   ************************************************************************ */
  
  /** Start a single rammanagernode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public void runRAMManagernodeDaemon() throws IOException {

    // start dataXceiveServer
    //dataXceiverServer.start();
    /*
    if (localDataXceiverServer != null) {
      localDataXceiverServer.start();
    }
    ipcServer.start();
    startPlugins(conf);
    */
  }

  /**
   * TODO: is this right?
   */
  public boolean isDatanodeUp() {
    return true;
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon()} subsequently. 
   */
   /*
  public static RAMManagerNode instantiateRAMManagerNode(String args[],
                                      Configuration conf) throws IOException {
    //return instantiateDataNode(args, conf, null);
    return instantiateRAMManagerNode(args, conf);
  }
  */
  
  /** Instantiate a single rammanager object. 
   * This must be run by invoking{@link RAMManagerNode#runRAMManagernodeDaemon()} 
   * subsequently. 
   */
  public static RAMManagerNode instantiateRAMManagerNode(String args [], Configuration conf
     // SecureResources resources
      ) throws IOException {
    if (conf == null)
      //conf = new HdfsConfiguration();
      conf = new Configuration();
    
    if (args != null) {
      // parse generic hadoop options
      GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
      args = hParser.getRemainingArgs();
    }
    
    if (!parseArguments(args, conf)) {
      //printUsage(System.err);
      return null;
    }
    //Collection<URI> dataDirs = getStorageDirs(conf);
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFS_RAMMANAGERNODE_KEYTAB_FILE_KEY,
        DFS_RAMMANAGERNODE_USER_NAME_KEY);
    //return makeInstance(dataDirs, conf, resources);
    return makeInstance(conf);
  }

  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
   /*
  public static RAMManagerNode createRAMManagerNode(String args[],
                                 Configuration conf) throws IOException {
    //return createDataNode(args, conf, null);
    return createDataNode(args, conf);
  }
  */
  
  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  @InterfaceAudience.Private
  public static RAMManagerNode createRAMManagerNode(String args[], Configuration conf
     // SecureResources resources
      ) throws IOException {
    //DataNode dn = instantiateDataNode(args, conf, resources);
    RAMManagerNode rmn = instantiateRAMManagerNode(args, conf);
    if (rmn != null) {
      rmn.runRAMManagernodeDaemon();
    }
    return rmn;
  }

  void join() {
    /*
    while (shouldRun) {
      try {
        //blockPoolManager.joinAll();
        if (blockPoolManager.getAllNamenodeThreads() != null
            && blockPoolManager.getAllNamenodeThreads().length == 0) {
          shouldRun = false;
        } 
        // TODO: something more intelligent here
        Thread.sleep(2000);
      } catch (InterruptedException ex) {
        LOG.warn("Received exception in RAMManagernode#join: " + ex);
      }
    }
    */
  }

  /**
   * Make an instance of RAMManagerNode after ensuring that at least one of the
   * given data directories (and their parent directories, if necessary)
   * can be created.
   * @param conf Configuration instance to use.
   * @param resources Secure resources needed to run under Kerberos
   * @return RAMManagerNode instance for given list of data dirs and conf, or null if
   * no directory from this directory list can be created.
   * @throws IOException
   */
  static RAMManagerNode makeInstance(Configuration conf
     // SecureResources resources
      ) throws IOException {
    //LocalFileSystem localFS = FileSystem.getLocal(conf);
    /*
    FsPermission permission = new FsPermission(
        conf.get(DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
                 DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    DataNodeDiskChecker dataNodeDiskChecker =
        new DataNodeDiskChecker(permission);
    ArrayList<File> dirs =
        getDataDirsFromURIs(dataDirs, localFS, dataNodeDiskChecker);
        */
    DefaultMetricsSystem.initialize("RAMManagerNode");

    //assert dirs.size() > 0 : "number of data directories should be > 0";
    //return new DataNode(conf, dirs, resources);
    return new RAMManagerNode(conf);
  }

  @Override
  public String toString() {
    return "RAMManagerNode{localName='" + getDisplayName()
        //+ "', storageID='" + getStorageId() + 
        //"', xmitsInProgress=" + xmitsInProgress.get() + "}"
        ;
  }

  /**
   * Parse and verify command line arguments and set configuration parameters.
   *
   * @return false if passed argements are incorrect
   */
  private static boolean parseArguments(String args[], 
                                        Configuration conf) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      LOG.error("RAMManagerNode does not support any arguments at present"); 
      return false;
    }
    setStartupOption(conf, startOpt);
    return true;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set(DFS_RAMMANAGERNODE_STARTUP_KEY, opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get(DFS_RAMMANAGERNODE_STARTUP_KEY,
                                          StartupOption.REGULAR.toString()));
  }

  public static void secureMain(String args[] 
  //SecureResources resources
  ) {
    try {
      StringUtils.startupShutdownMessage(RAMManagerNode.class, args, LOG);
      //DataNode datanode = createDataNode(args, null, resources);
      RAMManagerNode rammanagernode = createRAMManagerNode(args, null);
      if (rammanagernode != null)
        rammanagernode.join();
      LOG.info("Post-join in RAMManagerNode");
    } catch (Throwable e) {
      LOG.fatal("Exception in secureMain", e);
      terminate(1, e);
    } finally {
      // We need to terminate the process here because either shutdown was called
      // or some disk related conditions like volumes tolerated or volumes required
      // condition was not met. Also, In secure mode, control will go to Jsvc
      // and Datanode process hangs if it does not exit.
      LOG.warn("Exiting RAMManagernode");
      terminate(0);
    }
  }
  
  public static void main(String args[]) {
    if (DFSUtil.parseHelpArgument(args, RAMManagerNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    //secureMain(args, null);
    secureMain(args);
  }

  static InetSocketAddress getStreamingAddr(Configuration conf) {
    return NetUtils.createSocketAddr(
        conf.get(DFS_DATANODE_ADDRESS_KEY, DFS_DATANODE_ADDRESS_DEFAULT));
  }
  
  //@Override // DataNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }
  
  //@Override // DataNodeMXBean
  public String getRpcPort(){
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        this.getConf().get(DFS_DATANODE_IPC_ADDRESS_KEY));
    return Integer.toString(ipcAddr.getPort());
  }

  //@Override // DataNodeMXBean
  public String getHttpPort(){
    return this.getConf().get("dfs.datanode.info.port");
  }
  
  /**
   * @return the datanode's http port
   */
  public int getInfoPort() {
    return infoServer.getPort();
  }

  /**
   * @return the datanode's https port
   */
  public int getInfoSecurePort() {
    return infoSecurePort;
  }

  //@Override // DataNodeMXBean
  public synchronized String getClusterId() {
    return clusterId;
  }
  
  boolean shouldRun() {
    return shouldRun;
  }
}
