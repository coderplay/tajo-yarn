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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TajoOnYarn {

  private static final Log LOG = LogFactory.getLog(TajoOnYarn.class);

  private final AppContext appContext;

  public TajoOnYarn(AppContext appContext) {
    this.appContext = appContext;
  }

  public void startMaster() throws IOException {
    LOG.info("Current working dir:"  + System.getProperty("user.dir"));
    String tajoHome = System.getenv("TAJO_HOME");
    List<String> script = new ArrayList<String>();
    script.add("bin/tajo-daemon.sh");
    script.add("start");
    script.add("master");
    Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor(
        script.toArray(new String[script.size()]), new File(tajoHome));
    try {
      shell.execute();
      LOG.info(shell.getOutput());
    } catch (Shell.ExitCodeException e) {
      LOG.warn(shell.getOutput());
    }

  }

  public void startQueryMaster() throws IOException {
    LOG.info("Current working dir:"  + System.getProperty("user.dir"));
    String tajoHome = System.getenv("TAJO_HOME");
    List<String> script = new ArrayList<String>();
    script.add("bin/tajo-daemon.sh");
    script.add("start");
    script.add("worker");
    script.add("standby");
    Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor(
        script.toArray(new String[script.size()]), new File(tajoHome));
    try {
      shell.execute();
      LOG.info(shell.getOutput());
    } catch (Shell.ExitCodeException e) {
      LOG.warn(shell.getOutput());
    }
  }

  public void startTaskRunner() {

  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  TajoContainerRequest setupContainerAskForQM() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(appContext.getRequestPriority());

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(appContext.getQmMemory());
    capability.setVirtualCores(appContext.getQmVCores());

    TajoContainerRequest request = new TajoContainerRequest(capability, null, null,
        pri, new QMContainerTask());
    LOG.info("Requested QueryMaster container ask: " + request.toString());
    return request;
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  TajoContainerRequest setupContainerAskForTR() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(appContext.getRequestPriority());

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(appContext.getTrMemory());
    capability.setVirtualCores(appContext.getTrVCores());

    TajoContainerRequest request = new TajoContainerRequest(capability, null, null,
        pri, new QMContainerTask());
    LOG.info("Requested TaskRunner container ask: " + request.toString());
    return request;
  }

}
