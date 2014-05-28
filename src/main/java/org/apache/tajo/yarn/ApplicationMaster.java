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

package org.apache.tajo.yarn;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.tajo.yarn.thrift.TajoYarnService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Tajo-Yarn Application Master for YARN
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  // Configuration
  private Configuration conf;

  private AppContext appContext;

  // Hardcoded path to custom log_properties
  private static final String log4jPath = "log4j.properties";

  private ClusterScheduler scheduler;

  private TServer server;

  /**
   * @param args Command line args
   */
  public static void main(String[] args) {
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      System.exit(1);
    }
  }

  public ApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
  }

  /**
   * Parse command line options
   *
   * @param args Command line args
   * @return Whether init successful and getLaunchContext should be invoked
   * @throws org.apache.commons.cli.ParseException
   * @throws java.io.IOException
   */
  public boolean init(String[] args) throws ParseException, IOException {

    Options opts = new Options();
    opts.addOption("app_attempt_id", true,
        "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("qm_memory", true,
        "Amount of memory in MB to be requested to launch a QueryMaster. Default 512");
    opts.addOption("qm_vcores", true,
        "Amount of virtual cores to be requested to launch a QueryMaster. Default 2");
    opts.addOption("tr_memory", true,
        "Amount of memory in MB to be requested to launch a TaskRunner. Default 1024");
    opts.addOption("tr_vcores", true,
        "Amount of virtual cores to be requested to launch a TaskRunner. Default 4");
    opts.addOption("worker_memory", true,
        "Amount of memory in MB to be requested to launch a worker. Default 2048");
    opts.addOption("worker_vcores", true,
        "Amount of virtual cores to be requested to launch a worker. Default 4");
    opts.addOption("help", false, "Print usage");

    CommandLine cliParser = new GnuParser().parse(opts, args);

    // Check whether customer log4j.properties file exists
    if (fileExist(log4jPath)) {
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class,
            log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    ApplicationAttemptId appAttemptID = null;
    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs
          .get(Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HOST.name())) {
      throw new RuntimeException(Environment.NM_HOST.name()
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(Environment.NM_HTTP_PORT
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_PORT.name())) {
      throw new RuntimeException(Environment.NM_PORT.name()
          + " not set in the environment");
    }

    LOG.info("Application master for app" + ", appId="
        + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
        + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    int qmMemory = Integer.parseInt(cliParser.getOptionValue(
        "qm_memory", "512"));
    int qmVCores = Integer.parseInt(cliParser.getOptionValue(
        "qm_vcores", "2"));
    int trMemory = Integer.parseInt(cliParser.getOptionValue(
        "tr_memory", "1024"));
    int trVCores = Integer.parseInt(cliParser.getOptionValue(
        "tr_vcores", "4"));
    int workerMemory = Integer.parseInt(cliParser.getOptionValue(
        "worker_memory", "2048"));
    int workerVCores = Integer.parseInt(cliParser.getOptionValue(
        "worker_vcores", "4"));

    int requestPriority = Integer.parseInt(cliParser
        .getOptionValue("priority", "0"));

    String appMasterHostName = InetAddress.getLocalHost().getHostName();

    this.appContext = new AppContext(
        conf, appAttemptID, workerMemory, workerVCores, requestPriority, appMasterHostName);

    return true;
  }

  /**
   * Helper function to print usage
   *
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  /**
   * Main getLaunchContext function for the application master
   *
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   */
  @SuppressWarnings({ "unchecked" })
  public void run() throws YarnException, IOException {
    LOG.info("Starting ApplicationMaster");

    startTajoMaster();

    TNonblockingServerSocket serverSocket;
    int appMasterRpcPort;
    try {
      serverSocket = new TNonblockingServerSocket(0);
      appMasterRpcPort  = ThriftHelper.getServerSocketFor(serverSocket).getLocalPort();
    } catch (TTransportException tte) {
      LOG.error("Error throws when starting rpc server", tte);
      throw new IOException(tte);
    }

    LOG.info("Starting cluster scheduler");
    String appMasterTrackingUrl = StringHelper.join("http://", appContext.getMasterHost(), ":26080");
    this.scheduler = new ClusterScheduler(appContext, appMasterTrackingUrl, appMasterRpcPort);
    scheduler.init(conf);
    scheduler.service();

    LOG.info("Starting rpc server, listening on" + appContext.getMasterHost() + ":" + appMasterRpcPort);
    TProcessor processor = new TajoYarnService.Processor<TajoYarnService.Iface>(
        new TajoYarnServiceImpl(scheduler, appContext));
    this.server = new TNonblockingServer(new TNonblockingServer.Args(serverSocket).processor(processor));
    this.server.serve();
  }


  public void startTajoMaster() throws IOException {
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
      throw new IOException(e);
    }

  }

  private boolean fileExist(String filePath) {
    return new File(filePath).exists();
  }




}
