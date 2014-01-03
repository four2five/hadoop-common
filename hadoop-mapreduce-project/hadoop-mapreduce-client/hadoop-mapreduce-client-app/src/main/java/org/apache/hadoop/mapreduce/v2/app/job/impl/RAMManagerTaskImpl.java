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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RAMManagerTaskAttemptImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

@SuppressWarnings({ "rawtypes" })
public class RAMManagerTaskImpl extends TaskImpl {

  private final NodeId nodeId;
  private static final Log LOG = LogFactory.getLog(RAMManagerTaskImpl.class);

  public RAMManagerTaskImpl(JobId jobId, 
          int partition,
          NodeId nodeId, EventHandler eventHandler,
          Path remoteJobConfFile,
          JobConf conf,
          int numRAMManagers,
          TaskAttemptListener taskAttemptListener,
          Token<JobTokenIdentifier> jobToken,
          Credentials credentials, Clock clock,
          int appAttemptId, MRAppMetrics metrics, AppContext appContext) {
    super(jobId, TaskType.RAMMANAGER, partition, eventHandler, remoteJobConfFile,
        conf, taskAttemptListener, jobToken, credentials, clock,
        appAttemptId, metrics, appContext);
    this.nodeId = nodeId;
    LOG.warn("In RAMManagerTaskImpl");
  }

  @Override
  protected int getMaxAttempts() {
    return conf.getInt(MRJobConfig.RAMMANAGER_MAX_ATTEMPTS, 2);
  }

  @Override
  protected TaskAttemptImpl createAttempt() {
    return new RAMManagerTaskAttemptImpl(getID(), nextAttemptNumber,
        eventHandler, jobFile,
        partition, nodeId, conf, taskAttemptListener,
        jobToken, credentials, clock, appContext);
  }

  @Override
  public TaskType getType() {
    return TaskType.RAMMANAGER;
  }

  protected NodeId getNodeId() {
    return this.nodeId;
  }

  /**
   * @return a String formatted as a comma-separated list of splits.
   */
  protected String getNodeIdAsString() {
    return  getNodeId().toString();
  }
}
