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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tajo.yarn.command.*;

import java.util.Arrays;
import java.util.HashMap;

/**
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

  private static final Log LOG = LogFactory.getLog(Client.class);

  // Configuration  for tajo-yarn rather than tajo
  private Configuration conf;

  public Client(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @param args the command line arguments
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  public void run(String[] args) throws Exception {
    HashMap<String, Command> commands = new HashMap<String, Command>();
    HelpCommand help = new HelpCommand(commands);
    commands.put("help", help);
    commands.put("launch", new LaunchCommand(conf));
    commands.put("qm", new QueryMasterOpCommand(conf));
    commands.put("tr", new TaskRunnerOpCommand(conf));

    String commandName = null;
    String[] commandArgs = null;
    if (args.length < 1) {
      commandName = "help";
      commandArgs = new String[0];
    } else {
      commandName = args[0];
      commandArgs = Arrays.copyOfRange(args, 1, args.length);
    }
    Command command = commands.get(commandName);
    if (command == null) {
      LOG.error("ERROR: " + commandName + " is not a supported command.");
      help.printHelpFor(null);
      System.exit(1);
    }
    Options opts = command.getOpts();
    if (!opts.hasOption("h")) {
      opts.addOption("h", "help", false, "print out a help message");
    }
    CommandLine cl = new GnuParser().parse(command.getOpts(), commandArgs);
    if (cl.hasOption("help")) {
      help.printHelpFor(Arrays.asList(commandName));
    } else {

      command.process(cl);
    }
  }

  public static void main(String[] args) throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    Client client = new Client(conf);
    client.run(args);
  }

}
