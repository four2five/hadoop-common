package org.apache.hadoop.mapreduce.v2.app;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor;


import com.google.common.annotations.VisibleForTesting;


public class RunningAppContext implements AppContext {

  private final Map<JobId, Job> jobs = new ConcurrentHashMap<JobId, Job>();
  private final Configuration conf;
  private final ClusterInfo clusterInfo = new ClusterInfo();
  private final ClientToAMTokenSecretManager clientToAMTokenSecretManager;
  private String appName;
  private final long startTime;
  private Dispatcher dispatcher;
  private Clock clock;
  private ContainerAllocator containerAllocator;
  protected volatile boolean isLastAMRetry = false;
  private final ApplicationAttemptId appAttemptID;
  private int maxAppAttempts;


  @VisibleForTesting
  protected AtomicBoolean successfullyUnregistered = new AtomicBoolean(false);


  public RunningAppContext(Configuration config, long startTime, Dispatcher dispatcher, 
                           Clock clock, ApplicationAttemptId appAttemptID, int maxAppAttempts) {
    this.conf = config;
    this.clientToAMTokenSecretManager =
        new ClientToAMTokenSecretManager(appAttemptID, null);
    this.appName = this.conf.get(MRJobConfig.JOB_NAME, "<missing app name>");
    this.startTime = startTime;
    this.dispatcher = dispatcher;
    this.clock = clock;
    this.appAttemptID = appAttemptID;
    this.maxAppAttempts = maxAppAttempts;

  }

  public void setContainerAllocator(ContainerAllocator allocator) { 
    this.containerAllocator = allocator;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return appAttemptID;
  }

  @Override
  public ApplicationId getApplicationID() {
    return appAttemptID.getApplicationId();
  }

  @Override
  public String getApplicationName() {
    return appName;
  }

  public Map<JobId, Job> getJobs() { 
    return jobs;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public Job getJob(JobId jobID) {
    return jobs.get(jobID);
  }

  @Override
  public Map<JobId, Job> getAllJobs() {
    return jobs;
  }

  @Override
  public EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  @Override
  public CharSequence getUser() {
    return this.conf.get(MRJobConfig.USER_NAME);
  }

  @Override
  public Clock getClock() {
    return clock;
  }
  
  @Override
  public ClusterInfo getClusterInfo() {
    return this.clusterInfo;
  }

  @Override
  public Set<String> getBlacklistedNodes() {
    return ((RMContainerRequestor) containerAllocator).getBlacklistedNodes();
  }
  
  @Override
  public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
    return clientToAMTokenSecretManager;
  }

  @Override
  public boolean isLastAMRetry(){
    computeIsLastAMRetry();
    return isLastAMRetry;
  }

  @Override
  public boolean hasSuccessfullyUnregistered() {
    return successfullyUnregistered.get();
  }

  public void markFailedUnregistration() {
    successfullyUnregistered.set(false);
  }

  public void markSuccessfulUnregistration() {
    successfullyUnregistered.set(true);
  }

  public void computeIsLastAMRetry() {
    isLastAMRetry = appAttemptID.getAttemptId() >= maxAppAttempts;
  }
}


