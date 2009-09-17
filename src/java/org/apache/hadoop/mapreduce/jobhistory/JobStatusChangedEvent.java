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

import org.apache.hadoop.mapreduce.JobID;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Event to record the change of status for a job
 *
 */
public class JobStatusChangedEvent implements HistoryEvent {

  private EventCategory category;
  private JobID jobid;
  private  String jobStatus;

  enum EventFields { EVENT_CATEGORY,
    JOB_ID,
    JOB_STATUS }

  /**
   * Create an event to record the change in the Job Status
   * @param id Job ID
   * @param jobStatus The new job status
   */
  public JobStatusChangedEvent(JobID id, String jobStatus) {
    this.jobid = id;
    this.jobStatus = jobStatus;
    this.category = EventCategory.JOB;
  }

  JobStatusChangedEvent() {
  }

  /** Get the Job Id */
  public JobID getJobId() { return jobid; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the event status */
  public String getStatus() { return jobStatus; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.JOB_STATUS_CHANGED;
  }

  public void readFields(JsonParser jp) throws IOException {
    if (jp.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Unexpected Token while reading");
    }

    while (jp.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = jp.getCurrentName();
      jp.nextToken(); // Move to the value
      switch (Enum.valueOf(EventFields.class, fieldName)) {
      case EVENT_CATEGORY:
        category = Enum.valueOf(EventCategory.class, jp.getText());
        break;
      case JOB_ID:
        jobid = JobID.forName(jp.getText());
        break;
      case JOB_STATUS:
        jobStatus = jp.getText();
        break;
      default: 
        throw new IOException("Unrecognized field '"+fieldName+"'!");
      }
    }
  }

  public void writeFields(JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(EventFields.EVENT_CATEGORY.toString(),
        category.toString());
    gen.writeStringField(EventFields.JOB_ID.toString(), jobid.toString());
    gen.writeStringField(EventFields.JOB_STATUS.toString(), jobStatus);
    gen.writeEndObject();
  }
}
