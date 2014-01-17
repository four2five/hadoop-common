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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSelector;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

/** An client-rammanagernode protocol 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_RAMMANAGERNODE_USER_NAME_KEY)
//@TokenInfo(BlockTokenSelector.class)
public interface ClientRAMManagernodeProtocol {
  /**
   * Until version 9, this class ClientRAMManagernodeProtocol served as both
   * the client interface to the DN AND the RPC protocol used to 
   * communicate with the NN.
   * 
   * This class is used by both the DFSClient and the 
   * DN server side to insulate from the protocol serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in ClientRAMManagernodeProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   * 
   * The log of historical changes can be retrieved from the svn).
   * 9: Added deleteBlockPool method
   * 
   * 9 is the last version id when this class was used for protocols
   *  serialization. DO not update this version any further. 
   */
  public static final long versionID = 1L;

  /** Return the visible length of a replica. */
  //long getReplicaVisibleLength(ExtendedBlock b) throws IOException;
  
  /**
   * Log a message via the RAMManagerNode
   **/
  void logMessage(String message) throws IOException;
}
