<%
/*
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
%>
<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.mapreduce.TaskAttemptID"
  import="org.apache.hadoop.mapreduce.TaskID"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.mapreduce.jobhistory.*"
%>
<%!private static final long serialVersionUID = 1L;
%>

<%! static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss") ; %>
<%
    String jobid = request.getParameter("jobid");
    String logFile = request.getParameter("logFile");

    Path jobFile = new Path(logFile);
    String[] jobDetails = jobFile.getName().split("_");
    String jobUniqueString = jobid;

    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    JobHistoryParser.JobInfo job = JSPUtil.getJobInfo(request, fs);
%>
<html><body>
<h2>Hadoop Job <%=jobid %> on <a href="jobhistory.jsp">History Viewer</a></h2>

<b>User: </b> <%=job.getUsername() %><br/> 
<b>JobName: </b> <%=job.getJobname() %><br/> 
<b>JobConf: </b> <a href="jobconf_history.jsp?jobid=<%=jobid%>&jobLogDir=<%=new Path(logFile).getParent().toString()%>&jobUniqueString=<%=jobUniqueString%>"> 
                 <%=job.getJobConfPath() %></a><br/> 
<b>Submitted At: </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getSubmitTime(), 0 )  %><br/> 
<b>Launched At: </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLaunchTime(), job.getSubmitTime()) %><br/>
<b>Finished At: </b>  <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getFinishTime(), job.getLaunchTime()) %><br/>
<b>Status: </b> <%= ((job.getJobStatus()) == null ? "Incomplete" :job.getJobStatus()) %><br/> 
<%
    HistoryViewer.SummarizedJob sj = new HistoryViewer.SummarizedJob(job);
%>
<b><a href="analysejobhistory.jsp?jobid=<%=jobid %>&logFile=<logFile%>">Analyse This Job</a></b> 
<hr/>
<center>
<table border="2" cellpadding="5" cellspacing="2">
<tr>
<td>Kind</td><td>Total Tasks(successful+failed+killed)</td><td>Successful tasks</td><td>Failed tasks</td><td>Killed tasks</td><td>Start Time</td><td>Finish Time</td>
</tr>
<tr>
<td>Setup</td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=JOB_SETUP&status=all">
        <%=sj.getTotalSetups()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=JOB_SETUP&status=SUCCEEDED">
        <%=sj.getNumFinishedSetups()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=JOB_SETUP&status=FAILED">
        <%=sj.getNumFailedSetups()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=JOB_SETUP&status=KILLED">
        <%=sj.getNumKilledSetups()%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getSetupStarted(), 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getSetupFinished(), sj.getSetupStarted()) %></td>
</tr>
<tr>
<td>Map</td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=MAP&status=all">
        <%=sj.getTotalMaps()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=MAP&status=SUCCEEDED">
        <%=job.getFinishedMaps() %></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=MAP&status=FAILED">
        <%=sj.getNumFailedMaps()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=MAP&status=KILLED">
        <%=sj.getNumKilledMaps()%></a></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getMapStarted(), 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getMapFinished(), sj.getMapStarted()) %></td>
</tr>
<tr>
<td>Reduce</td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=REDUCE&status=all">
        <%=sj.getTotalReduces()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=REDUCE&status=SUCCEEDED">
        <%=job.getFinishedReduces()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=REDUCE&status=FAILED">
        <%=sj.getNumFailedReduces()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=REDUCE&status=KILLED">
        <%=sj.getNumKilledReduces()%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getReduceStarted(), 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getReduceFinished(), sj.getReduceStarted()) %></td>
</tr>
<tr>
<td>Cleanup</td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=JOB_CLEANUP&status=all">
        <%=sj.getTotalCleanups()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=JOB_CLEANUP&status=SUCCEEDED">
        <%=sj.getNumFinishedCleanups()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=JOB_CLEANUP&status=FAILED">
        <%=sj.getNumFailedCleanups()%></a></td>
    <td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&logFile=<%=logFile%>&taskType=JOB_CLEANUP&status=KILLED>">
        <%=sj.getNumKilledCleanups()%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getCleanupStarted(), 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getCleanupFinished(), sj.getCleanupStarted()) %></td>
</tr>
</table>

<br/>
 <%
    HistoryViewer.FilteredJob filter = new HistoryViewer.FilteredJob(job,TaskStatus.State.FAILED.toString()); 
    Map<String, Set<TaskID>> badNodes = filter.getFilteredMap(); 
    if (badNodes.size() > 0) {
 %>
<h3>Failed tasks attempts by nodes </h3>
<table border="1">
<tr><td>Hostname</td><td>Failed Tasks</td></tr>
 <%	  
      for (Map.Entry<String, Set<TaskID>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<TaskID> failedTasks = entry.getValue();
%>
        <tr>
        <td><%=node %></td>
        <td>
<%
          boolean firstId = true;
          for (TaskID tid : failedTasks) {
             if (firstId) {
              firstId = false;
%>
            <a href="taskdetailshistory.jsp?jobid=<%=jobid%>&logFile=<%=logFile%>&taskid=<%=tid %>"><%=tid %></a>
<%		  
          } else {
%>	
            ,&nbsp<a href="taskdetailshistory.jsp?jobid=<%=jobid%>&logFile=<%=logFile%>&taskid=<%=tid %>"><%=tid %></a>
<%		  
          }
        }
%>	
        </td>
        </tr>
<%	  
      }
	}
 %>
</table>
<br/>
 <%
    filter = new HistoryViewer.FilteredJob(job, TaskStatus.State.KILLED.toString());
    badNodes = filter.getFilteredMap(); 
    if (badNodes.size() > 0) {
 %>
<h3>Killed tasks attempts by nodes </h3>
<table border="1">
<tr><td>Hostname</td><td>Killed Tasks</td></tr>
 <%	  
      for (Map.Entry<String, Set<TaskID>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<TaskID> killedTasks = entry.getValue();
%>
        <tr>
        <td><%=node %></td>
        <td>
<%
        boolean firstId = true;
        for (TaskID tid : killedTasks) {
             if (firstId) {
              firstId = false;
%>
            <a href="taskdetailshistory.jsp?jobid=<%=jobid%>&logFile=<%=logFile%>&taskid=<%=tid %>"><%=tid %></a>
<%		  
          } else {
%>	
            ,&nbsp<a href="taskdetailshistory.jsp?jobid=<%=jobid%>&logFile=<%=logFile%>&taskid=<%=tid %>"><%=tid %></a>
<%		  
          }
        }
%>	
        </td>
        </tr>
<%	  
      }
    }
%>
</table>
</center>
</body></html>
