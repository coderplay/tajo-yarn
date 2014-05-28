/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Context for sharing information across components in YARN App.
 */
public class AppContext {

  private Configuration conf;
  // Application Attempt Id ( combination of attemptId and fail count )
  private ApplicationAttemptId appAttemptID;


  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";

  // App Master configuration
  // Memory to request for the container on which a Query Master will getLaunchContext
  private int qmMemory = 512;
  // VirtualCores to request for the container on which a Query Master will getLaunchContext
  private int qmVCores = 2;
  // Memory to request for the container on which a Query Master will getLaunchContext
  private int trMemory = 1024;
  // VirtualCores to request for the container on which a Query Master will getLaunchContext
  private int trVCores = 4;

  // Memory to request for the container on which a worker will getLaunchContext
  private int workerMemory = 2048;
  // VirtualCores to request for the container on which a worker will getLaunchContext
  private int workerVCores = 4;


  // Priority of the request
  private int requestPriority;


  private String masterHost;

  public AppContext(Configuration conf,
                    ApplicationAttemptId appAttemptID,
                    int workerMemory,
                    int workerVCores,
                    int requestPriority,
                    String masterHost) {
    this.conf = conf;
    this.appAttemptID = appAttemptID;
    this.workerMemory = workerMemory;
    this.workerVCores = workerVCores;
    this.requestPriority = requestPriority;
    this.masterHost = masterHost;
  }

  @Deprecated
  public AppContext(Configuration conf,
                    ApplicationAttemptId appAttemptID,
                    int qmMemory,
                    int qmVCores,
                    int trMemory,
                    int trVCores,
                    int workerMemory,
                    int workerVCores,
                    int requestPriority,
                    String masterHost) {
    this.conf = conf;
    this.appAttemptID = appAttemptID;
    this.qmMemory = qmMemory;
    this.qmVCores = qmVCores;
    this.trMemory = trMemory;
    this.trVCores = trVCores;
    this.workerMemory = workerMemory;
    this.workerVCores = workerVCores;
    this.requestPriority = requestPriority;
    this.masterHost = masterHost;
  }


  public ApplicationId getApplicationId() {
    return appAttemptID.getApplicationId();
  }

  public ApplicationAttemptId getApplicationAttemptID() {
    return appAttemptID;
  }


  public Configuration getConfiguration() {
    return conf;
  }

  @Deprecated
  public int getQmMemory() {
    return qmMemory;
  }

  @Deprecated
  public int getQmVCores() {
    return qmVCores;
  }

  @Deprecated
  public int getTrMemory() {
    return trMemory;
  }

  @Deprecated
  public int getTrVCores() {
    return trVCores;
  }

  public int getWorkerMemory() {
    return workerMemory;
  }

  public int getWorkerVCores() {
    return workerVCores;
  }

  public int getRequestPriority() {
    return requestPriority;
  }

  public String getMasterHost() {
    return masterHost;
  }

}
