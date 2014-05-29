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

package org.apache.tajo.yarn.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.yarn.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class WorkerContainerTask implements org.apache.tajo.yarn.ContainerTask {
  private static final Log LOG = LogFactory.getLog(WorkerContainerTask.class);

  private AppContext appContext;

  public WorkerContainerTask(AppContext appContext) {
    this.appContext = appContext;
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  @Override
  public TajoContainerRequest getContainerRequest() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(appContext.getRequestPriority());

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(appContext.getWorkerMemory());
    capability.setVirtualCores(appContext.getWorkerVCores());

    TajoContainerRequest request = new TajoContainerRequest(capability, null, null,
        pri, this);
    LOG.info("Requested QueryMaster container ask: " + request.toString());
    return request;
  }

  @Override
  public ContainerLaunchContext getLaunchContext(Container container) throws IOException {
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

    FileSystem fs = FileSystem.get(appContext.getConfiguration());

    // Set the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    String suffix =
        "Tajo" + "/" + appContext.getApplicationId().getId();
    Path parentPath = new Path(fs.getHomeDirectory(), suffix);

    // tar ball
    Path archivePath = new Path(parentPath, "tajo-dist/tajo-0.8.0.tar.gz");
    FileStatus archiveFs = fs.getFileStatus(archivePath);
    LocalResource archiveRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(archivePath.toUri()),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
            archiveFs.getLen(), archiveFs.getModificationTime());
    localResources.put("tajo", archiveRsrc);


    Configuration tajoWorkerConf = new Configuration(false);
    tajoWorkerConf.addResource(new Path("conf", "tajo-site.xml"));
    tajoWorkerConf.set(Constants.TAJO_MASTER_UMBILICAL_RPC_ADDRESS, appContext.getMasterHost() + ":26001");
    Path dst = new Path(parentPath, container.getId() + Path.SEPARATOR + "worker-conf");
    fs.mkdirs(dst);
    Path confFile = new Path(dst, "tajo-site.xml");
    FSDataOutputStream fdos = fs.create(confFile);
    tajoWorkerConf.writeXml(fdos);
    fdos.close();
    FileStatus scFileStatus = fs.getFileStatus(dst);
    LocalResource scRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
            scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put("conf", scRsrc);
    launchContext.setLocalResources(localResources);

    // Set the environment
    setupEnv(launchContext);

    // Set the necessary command to execute on the allocated container
    Vector<CharSequence> vargs = new Vector<CharSequence>(5);

    // Set executable command
    // Set args for the shell command if any
    vargs.add("${" + Constants.TAJO_HOME + "}/bin/tajo");
    vargs.add("--config");
    vargs.add("${" + Constants.TAJO_CONF_DIR + "}");
    vargs.add("worker");
    // Add log redirect params
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
    commands.add("iostat -x 1");
    launchContext.setCommands(commands);
    return launchContext;
  }

  private void setupEnv(ContainerLaunchContext amContainer) throws IOException {
    LOG.info("Set the environment for the worker");
    Map<String, String> env = new HashMap<String, String>();

    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$())
        .append(File.pathSeparatorChar).append("./*");

    env.put("CLASSPATH", classPathEnv.toString());
    env.put(Constants.TAJO_ARCHIVE_ROOT, System.getenv(Constants.TAJO_ARCHIVE_ROOT));
    env.put(Constants.TAJO_HOME, "$PWD/${" + Constants.TAJO_ARCHIVE_ROOT + "}");
    env.put(Constants.TAJO_CONF_DIR, "$PWD/conf");
    env.put(Constants.TAJO_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    env.put(Constants.TAJO_CLASSPATH, "/export/apps/hadoop/site/lib/*");
    amContainer.setEnvironment(env);
  }



}
