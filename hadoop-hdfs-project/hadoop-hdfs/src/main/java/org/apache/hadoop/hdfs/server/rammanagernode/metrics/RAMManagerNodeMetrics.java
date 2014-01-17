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
package org.apache.hadoop.hdfs.server.rammanagernode.metrics;

import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 *
 * This class is for maintaining  the various RAMManagerNode statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #blocksRead}.inc()
 *
 */
@InterfaceAudience.Private
@Metrics(about="RAMManagerNode metrics", context="dfs")
public class RAMManagerNodeMetrics {

  @Metric MutableCounterLong totalRecordsRepresented;

  final MetricsRegistry registry = new MetricsRegistry("rammanagernode");
  final String name;

  public RAMManagerNodeMetrics(String name, String sessionId) {
    this.name = name;
    registry.tag(SessionId, sessionId);
  }

  public static RAMManagerNodeMetrics create(Configuration conf, String dnName) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics.create("RAMManagerNode", sessionId, ms);
    String name = "RAMManagerNodeActivity-"+ (dnName.isEmpty()
        ? "UndefinedRAMManagerNodeName"+ DFSUtil.getRandom().nextInt() 
            : dnName.replace(':', '-'));

    return ms.register(name, null, new RAMManagerNodeMetrics(name, sessionId));
  }

  public String name() { return name; }

  public void addRecordsRepresented(long recsRepresented) {
    totalRecordsRepresented.incr(recsRepresented);
  }


  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }
}
