
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

public class  QMContainerTask implements  LaunchContainerTask {

  @Override
  public void run() {
//      LOG.info("Setting up container launch container for containerid="
//          + container.getId());
//      ContainerLaunchContext ctx = Records
//          .newRecord(ContainerLaunchContext.class);
//
//      // Set the environment
//      ctx.setEnvironment(shellEnv);
//
//      // Set the local resources
//      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
//
//      // The container for the eventual shell commands needs its own local
//      // resources too.
//      // In this scenario, if a shell script is specified, we need to have it
//      // copied and made available to the container.
//      if (!shellScriptPath.isEmpty()) {
//        LocalResource shellRsrc = Records.newRecord(LocalResource.class);
//        shellRsrc.setType(LocalResourceType.FILE);
//        shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
//        try {
//          shellRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(
//              shellScriptPath)));
//        } catch (URISyntaxException e) {
//          LOG.error("Error when trying to use shell script path specified"
//              + " in env, path=" + shellScriptPath);
//          e.printStackTrace();
//
//          // A failure scenario on bad input such as invalid shell script path
//          // We know we cannot continue launching the container
//          // so we should release it.
//          // TODO
//          numCompletedContainers.incrementAndGet();
//          numFailedContainers.incrementAndGet();
//          return;
//        }
//        shellRsrc.setTimestamp(shellScriptPathTimestamp);
//        shellRsrc.setSize(shellScriptPathLen);
//        localResources.put(Shell.WINDOWS ? ExecBatScripStringtPath :
//            ExecShellStringPath, shellRsrc);
//        shellCommand = Shell.WINDOWS ? windows_command : linux_bash_command;
//      }
//      ctx.setLocalResources(localResources);
//
//      // Set the necessary command to execute on the allocated container
//      Vector<CharSequence> vargs = new Vector<CharSequence>(5);
//
//      // Set executable command
//      vargs.add(shellCommand);
//      // Set shell script path
//      if (!shellScriptPath.isEmpty()) {
//        vargs.add(Shell.WINDOWS ? ExecBatScripStringtPath
//            : ExecShellStringPath);
//      }
//
//      // Set args for the shell command if any
//      vargs.add(shellArgs);
//      // Add log redirect params
//      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
//      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
//
//      // Get final commmand
//      StringBuilder command = new StringBuilder();
//      for (CharSequence str : vargs) {
//        command.append(str).append(" ");
//      }
//
//      List<String> commands = new ArrayList<String>();
//      commands.add(command.toString());
//      ctx.setCommands(commands);
//
//      // Set up tokens for the container too. Today, for normal shell commands,
//      // the container in distribute-shell doesn't need any tokens. We are
//      // populating them mainly for NodeManagers to be able to download any
//      // files in the distributed file-system. The tokens are otherwise also
//      // useful in cases, for e.g., when one is running a "hadoop dfs" command
//      // inside the distributed shell.
//      ctx.setTokens(allTokens.duplicate());
//
//      containerListener.addContainer(container.getId(), container);
//      nmClientAsync.startContainerAsync(container, ctx);
//    }
  }
}
