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
package org.apache.tajo.yarn.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.yarn.ApplicationMaster;
import org.apache.tajo.yarn.Constants;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class LaunchCommand extends TajoCommand {
  private static final Log LOG = LogFactory.getLog(LaunchCommand.class);

  // Application master specific info to register a new Application with RM/ASM
  private String appName = "";
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "";
  // Amt. of memory resource to request for to getLaunchContext the App Master
  private int amMemory = 2048;
  // Amt. of virtual core resource to request for to getLaunchContext the App Master
  private int amVCores = 4;

  private int qmMemory = 512;
  private int qmVCores = 2;

  private int trMemory = 1024;
  private int trVCores = 4;

  private String confDir = "";

  private String tajoArchive = "";

  // log4j.properties file
  // if available, add to local resources and set into classpath
  private String log4jPropFile = "";

  // Debug flag
  boolean debugFlag = false;

  private static final String appMasterJarPath = "AppMaster.jar";
  // Directory where jars in
  private static final String libDir = "lib";
  // Hardcoded path to custom log_properties
  private static final String log4jPath = "log4j.properties";

  public LaunchCommand(Configuration conf) {
    super(conf);
  }

  @Override
  public String getHeaderDescription() {
    return "tajo-yarn launch";
  }

  @Override
  public Options getOpts() {
    Options opts = new Options();
    opts.addOption("archive", true, "(Required) File path of tajo-*.tar.gz");
    opts.addOption("appname", true, "Application Name. Default value - Tajo");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true,
        "RM Queue in which this application is to be submitted. Default value - default");
    opts.addOption("master_memory", true,
        "Amount of memory in MB to be requested to getLaunchContext the application master and Tajo Master. Default 2048");
    opts.addOption("master_vcores", true,
        "Amount of virtual cores to be requested to getLaunchContext the application master and Tajo Master. Default 4");
    opts.addOption("qm_memory", true,
        "Amount of memory in MB to be requested to launch a QueryMaster. Default 512");
    opts.addOption("qm_vcores", true,
        "Amount of virtual cores to be requested to launch a QueryMaster. Default 2");
    opts.addOption("tr_memory", true,
        "Amount of memory in MB to be requested to launch a TaskRunner. Default 1024");
    opts.addOption("tr_vcores", true,
        "Amount of virtual cores to be requested to launch a TaskRunner. Default 4");
    opts.addOption("conf_dir", true,
        "local dir which will be distributed as Tajo's TAJO_CONF_DIR. Default - tajo-conf");
    opts.addOption("log_properties", true, "log4j.properties file");
    return opts;
  }

  @Override
  public void process(CommandLine cl) throws Exception {
    if (!cl.hasOption("archive") || (cl.getOptionValue("archive") == null)) {
      throw new IllegalArgumentException("-archive is required");
    }

    tajoArchive = cl.getOptionValue("archive");
    appName = cl.getOptionValue("appname", "Tajo");
    amPriority = Integer.parseInt(cl.getOptionValue("priority", "0"));
    amQueue = cl.getOptionValue("queue", "default");
    amMemory = Integer.parseInt(cl.getOptionValue("master_memory", "2048"));
    amVCores = Integer.parseInt(cl.getOptionValue("master_vcores", "4"));
    qmMemory = Integer.parseInt(cl.getOptionValue("qm_memory", "512"));
    qmVCores = Integer.parseInt(cl.getOptionValue("qm_vcores", "2"));
    trMemory = Integer.parseInt(cl.getOptionValue("tr_memory", "1024"));
    trVCores = Integer.parseInt(cl.getOptionValue("tr_vcores", "4"));
    confDir = cl.getOptionValue("conf_dir", "tajo-conf");
    log4jPropFile = cl.getOptionValue("log_properties", "");

    validateOptions();

    launch();
  }

  private void validateOptions() throws IllegalArgumentException {
    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
          + " Specified virtual cores=" + amVCores);
    }
    if (qmMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for QueryMaster, exiting."
          + " Specified memory=" + qmMemory);
    }
    if (qmVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for QueryMaster, exiting."
          + " Specified virtual cores=" + qmVCores);
    }
    if (trMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for TaskRunner, exiting."
          + " Specified memory=" + trMemory);
    }
    if (trVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for TaskRunner, exiting."
          + " Specified virtual cores=" + trVCores);
    }
  }

  /**
   * Main getLaunchContext function for launch this application
   *
   * @return true if application completed successfully
   * @throws java.io.IOException
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   */
  private void launch() throws IOException, YarnException {
    LOG.info("Running Client");
    yarnClient.start();

    displayClusterSummary();

    // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();

    // validate resource capacity for launch an application amster
    validateResourceForAM(app);

    // set the application name
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appContext.setApplicationName(appName);

    // Set up the container launch context for the application master
    setupAMContainerLaunchContext(appContext);

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and
    // vcores requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    capability.setVirtualCores(amVCores);
    appContext.setResource(capability);

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(amPriority);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    LOG.info("Submitting application to ASM");

    yarnClient.submitApplication(appContext);

    // TODO
    // Try submitting the same request again
    // app submission failure?

    // Monitor the application
    // return monitorApplication(appId);

  }

  /**
   * If we do not have min/max, we may not be able to correctly request
   * the required resources from the RM for the app master
   * Memory ask has to be a multiple of min and less than max.
   * Dump out information about cluster capability as seen by the resource manager
   * @param app
   */
  private void validateResourceForAM(YarnClientApplication app) {
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    // TODO get min/max resource capabilities from RM and change memory ask if needed
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // A resource ask cannot exceed the max.
    if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + amMemory
          + ", max=" + maxMem);
      amMemory = maxMem;
    }

    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

    if (amVCores > maxVCores) {
      LOG.info("AM virtual cores specified above max threshold of cluster. "
          + "Using max value." + ", specified=" + amVCores
          + ", max=" + maxVCores);
      amVCores = maxVCores;
    }
  }

  private void setupAMContainerLaunchContext(ApplicationSubmissionContext appContext) throws IOException {
    ApplicationId appId = appContext.getApplicationId();
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    FileSystem fs = FileSystem.get(conf);
    // Set local resource info into app master container launch context
    setupLocalResources(amContainer, fs, appId);

    // Set the necessary security tokens as needed
    //amContainer.setContainerTokens(containerToken);

    // Set the env variables to be setup in the env where the application master will be getLaunchContext
    setupEnv(amContainer);

    // Set the necessary command to getLaunchContext the application master
    setupAMCommand(amContainer);

    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    // amContainer.setServiceData(serviceData);

    // Setup security tokens
    setupSecurityTokens(amContainer, fs);

    appContext.setAMContainerSpec(amContainer);
  }

  private void setupAMCommand(ContainerLaunchContext amContainer) {
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx32m");
    // Set class name
    vargs.add(ApplicationMaster.class.getName());

    // Set params for Application Master
    vargs.add("--qm_memory " + String.valueOf(qmMemory));
    vargs.add("--qm_vcores " + String.valueOf(qmVCores));
    vargs.add("--tr_memory " + String.valueOf(trMemory));
    vargs.add("--tr_vcores " + String.valueOf(trVCores));

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    amContainer.setCommands(commands);
  }

  private void setupLocalResources(ContainerLaunchContext amContainer, FileSystem fs,
                                   ApplicationId appId)
      throws IOException {
    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    LOG.info("Copy App Master jar from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path

    String appMasterJar = findContainingJar(ApplicationMaster.class);
    addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.getId(),
        localResources, LocalResourceType.FILE);

    addToLocalResources(fs, libDir, libDir, appId.getId(),
        localResources, LocalResourceType.FILE);

    addToLocalResources(fs, tajoArchive, "tajo", appId.getId(),
        localResources, LocalResourceType.ARCHIVE);

    addToLocalResources(fs, confDir, "conf", appId.getId(),
        localResources, LocalResourceType.FILE);

    // Set the log4j properties if needed
    if (!log4jPropFile.isEmpty()) {
      addToLocalResources(fs, log4jPropFile, log4jPath, appId.getId(),
          localResources, LocalResourceType.FILE);
    }

    amContainer.setLocalResources(localResources);
  }

  private void setupEnv(ContainerLaunchContext amContainer) throws IOException {
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();

    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$())
        .append(File.pathSeparatorChar).append("./*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar);
      classPathEnv.append(c.trim());
    }

    classPathEnv.append(File.pathSeparatorChar).append("./").append(libDir).append("/*");

    classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");

    // add the runtime classpath needed for tests to work
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }

    env.put("CLASSPATH", classPathEnv.toString());
    env.put(Constants.TAJO_HOME, "$PWD/tajo/" + getTajoHomeInArchive(tajoArchive));
    env.put(Constants.TAJO_CONF_DIR, "$PWD/conf");
    env.put(Constants.TAJO_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    env.put(Constants.TAJO_CLASSPATH, "/export/apps/hadoop/site/lib/*");
    amContainer.setEnvironment(env);
  }

  private void setupSecurityTokens(ContainerLaunchContext amContainer, FileSystem fs) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException(
            "Can't get Master Kerberos principal for the RM to use as renewer");
      }

      // For now, only getting tokens for the default file-system.
      final Token<?> tokens[] =
          fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }
  }

  private void displayClusterSummary() throws YarnException, IOException {
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Got Cluster metric info from ASM"
        + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
        NodeState.RUNNING);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      LOG.info("Got node report from ASM for"
          + ", nodeId=" + node.getNodeId()
          + ", nodeAddress" + node.getHttpAddress()
          + ", nodeRackName" + node.getRackName()
          + ", nodeNumContainers" + node.getNumContainers());
    }

    QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
    LOG.info("Queue info"
        + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue"
            + ", queueName=" + aclInfo.getQueueName()
            + ", userAcl=" + userAcl.name());
      }
    }
  }


  /**
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   *
   * @param clazz the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException on any error
   */
  private static String findContainingJar(Class<?> clazz) throws IOException {
    ClassLoader loader = clazz.getClassLoader();
    String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
    for (Enumeration<URL> itr = loader.getResources(classFile);
         itr.hasMoreElements(); ) {
      URL url = itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        return toReturn.replaceAll("!.*$", "");
      }
    }

    throw new IOException("Fail to locat a JAR for class: " + clazz.getName());
  }

  private void addToLocalResources(FileSystem fs,
                                   String fileSrcPath,
                                   String fileDstPath,
                                   int appId,
                                   Map<String, LocalResource> localResources,
                                   LocalResourceType type) throws IOException {
    String suffix =
        appName + "/" + appId + "/" + fileSrcPath;
    Path dst =
        new Path(fs.getHomeDirectory(), suffix);
    fs.copyFromLocalFile(new Path(fileSrcPath), dst);
    FileStatus scFileStatus = fs.getFileStatus(dst);
    LocalResource scRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            type, LocalResourceVisibility.APPLICATION,
            scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put(fileDstPath, scRsrc);
  }

  private String getTajoHomeInArchive(String archiveName) throws IOException {
    String lower = archiveName.toLowerCase();
    if (lower.endsWith(".zip")) {
      return getTajoHomeInZip(archiveName);
    } else if (lower.endsWith(".tar.gz") ||
        lower.endsWith(".tgz")) {
      return getTajoHomeInTar(archiveName);
    }
    throw new IOException("Unable to get tajo home dir from " + archiveName);
  }

  private String getTajoHomeInZip(String zip) throws IOException {
    ZipInputStream inputStream = null;
    try {
      inputStream = new ZipInputStream(new FileInputStream(zip));
      for (ZipEntry entry = inputStream.getNextEntry(); entry != null; ) {
        String entryName = entry.getName();
        if (entry.isDirectory() && entryName.startsWith("tajo-")) {
          return entryName.substring(0, entryName.length() - 1);
        }
        entry = inputStream.getNextEntry();
      }
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }

    throw new IOException("Unable to get tajo home dir from " + zip);
  }

  private String getTajoHomeInTar(String tar) throws IOException {
    TarArchiveInputStream inputStream = null;
    try {
      inputStream =
          new TarArchiveInputStream(new GZIPInputStream(
              new FileInputStream(tar)));
      for (TarArchiveEntry entry = inputStream.getNextTarEntry(); entry != null; ) {
        String entryName = entry.getName();
        if (entry.isDirectory() && entryName.startsWith("tajo-")) {
          return entryName.substring(0, entryName.length() - 1);
        }
        entry = inputStream.getNextTarEntry();
      }
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }

    throw new IOException("Unable to get tajo home dir from " + tar);
  }

}
