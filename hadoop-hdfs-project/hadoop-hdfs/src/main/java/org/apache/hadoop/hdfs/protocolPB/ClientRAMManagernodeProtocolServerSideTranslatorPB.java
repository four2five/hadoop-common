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
package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientRAMManagernodeProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolResponseProto;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsRequestProto;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto.Builder;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
//import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientRAMManagernodeProtocolProtos.LogMessageRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientRAMManagernodeProtocolProtos.LogMessageResponseProto;
//import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link ClientRAMManagernodeProtocolPB} to the
 * {@link ClientRAMManagernodeProtocol} server implementation.
 */
@InterfaceAudience.Private
public class ClientRAMManagernodeProtocolServerSideTranslatorPB implements
    ClientRAMManagernodeProtocolPB {
  private final static LogMessageResponseProto LOG_MESSAGE_RESP =
      LogMessageResponseProto.newBuilder().build();
  //private final static DeleteBlockPoolResponseProto DELETE_BLOCKPOOL_RESP =
   //   DeleteBlockPoolResponseProto.newBuilder().build();
  
  private final ClientRAMManagernodeProtocol impl;

  public ClientRAMManagernodeProtocolServerSideTranslatorPB(
      ClientRAMManagernodeProtocol impl) {
    this.impl = impl;
  }
  
  @Override
  public LogMessageResponseProto logMessage(
      RpcController unused, LogMessageRequestProto request)
      throws ServiceException {
    try {
      impl.logMessage(request.getMessage());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return LOG_MESSAGE_RESP;
  }
}
