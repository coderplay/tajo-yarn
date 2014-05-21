
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
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class QMContainerTask implements LaunchContainerTask {
  private static final Log LOG = LogFactory.getLog(QMContainerTask.class);

  @Override
  public ContainerLaunchContext getLaunchContext() throws IOException {
    // create a container launch context
    ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    try {
      Credentials credentials = user.getCredentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      launchContext.setTokens(securityTokens);
    } catch (IOException e) {
      LOG.warn("Getting current user info failed when trying to launch the container"
          + e.getMessage());
    }

    // Set the environment

    // Set the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    launchContext.setLocalResources(localResources);

    // Set the necessary command to execute on the allocated container
    Vector<CharSequence> vargs = new Vector<CharSequence>(5);

    // Set executable command
    // Set args for the shell command if any

    // Add log redirect params
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    launchContext.setCommands(commands);

    return launchContext;
  }

}
