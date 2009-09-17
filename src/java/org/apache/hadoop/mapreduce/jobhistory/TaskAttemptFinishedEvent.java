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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Event to record successful task completion
 *
 */
public class TaskAttemptFinishedEvent  implements HistoryEvent {

  private EventCategory category;
  private TaskID taskid;
  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String taskStatus;
  private long finishTime;
  private String hostname;
  private String state;
  private Counters counters;

  enum EventFields { EVENT_CATEGORY,
    TASK_ID,
    TASK_ATTEMPT_ID,
    TASK_TYPE,
    TASK_STATUS,
    FINISH_TIME,
    HOSTNAME,
    STATE,
    COUNTERS }

  TaskAttemptFinishedEvent() {
  }

  /**
   * Create an event to record successful finishes for setup and cleanup 
   * attempts
   * @param id Attempt ID
   * @param taskType Type of task
   * @param taskStatus Status of task
   * @param finishTime Finish time of attempt
   * @param hostname Host where the attempt executed
   * @param state State string
   * @param counters Counters for the attempt
   */
  public TaskAttemptFinishedEvent(TaskAttemptID id, 
      TaskType taskType, String taskStatus, 
      long finishTime,
      String hostname, String state, Counters counters) {
    this.taskid = id.getTaskID();
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.state = state;
    this.counters = counters;
    this.category = EventCategory.TASK_ATTEMPT;
  }

  /** Get the task ID */
  public TaskID getTaskId() { return taskid; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the task attempt id */
  public TaskAttemptID getAttemptId() { return attemptId; }
  /** Get the task type */
  public TaskType getTaskType() { return taskType; }
  /** Get the task status */
  public String getTaskStatus() { return taskStatus; }
  /** Get the attempt finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the host where the attempt executed */
  public String getHostname() { return hostname; }
  /** Get the state string */
  public String getState() { return state; }
  /** Get the counters for the attempt */
  public Counters getCounters() { return counters; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.MAP_ATTEMPT_FINISHED;
  }

  public void readFields(JsonParser jp) throws IOException {
    if (jp.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Unexpected token while reading");
    }

    while (jp.nextToken() != JsonToken.END_OBJECT) {
      String fieldname = jp.getCurrentName();
      jp.nextToken(); // move to value
      switch (Enum.valueOf(EventFields.class, fieldname)) {
      case EVENT_CATEGORY:
        category = Enum.valueOf(EventCategory.class, jp.getText());
        break;
      case TASK_ID:
        taskid = TaskID.forName(jp.getText());
        break;
      case TASK_ATTEMPT_ID: 
        attemptId = TaskAttemptID.forName(jp.getText());
        break;
      case TASK_TYPE:
        taskType = TaskType.valueOf(jp.getText());
        break;
      case TASK_STATUS:
        taskStatus = jp.getText();
        break;
      case FINISH_TIME:
        finishTime = jp.getLongValue();
        break;
      case HOSTNAME:
        hostname = jp.getText();
        break;
      case STATE:
        state = jp.getText();
        break;
      case COUNTERS:
        counters = EventReader.readCounters(jp);
        break;
      default: 
        throw new IOException("Unrecognized field '"+fieldname+"'!");
      }
    }
  }

  public void writeFields(JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(EventFields.EVENT_CATEGORY.toString(),
        category.toString());
    gen.writeStringField(EventFields.TASK_ID.toString(), taskid.toString());
    gen.writeStringField(EventFields.TASK_ATTEMPT_ID.toString(),
        attemptId.toString());
    gen.writeStringField(EventFields.TASK_TYPE.toString(), 
        taskType.toString());
    gen.writeStringField(EventFields.TASK_STATUS.toString(),
        taskStatus);
    gen.writeNumberField(EventFields.FINISH_TIME.toString(), finishTime);
    gen.writeStringField(EventFields.HOSTNAME.toString(), hostname);
    gen.writeStringField(EventFields.STATE.toString(), state);
    EventWriter.writeCounters(counters, gen);
    gen.writeEndObject();
  }
}
