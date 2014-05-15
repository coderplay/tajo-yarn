
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
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.Collection;
import java.util.HashMap;

public class HelpCommand implements ClientCommand {
  HashMap<String, ClientCommand> commands;

  public HelpCommand(HashMap<String, ClientCommand> commands) {
    this.commands = commands;
  }

  @Override
  public Options getOpts() {
    return new Options();
  }

  @Override
  public String getHeaderDescription() {
    return "tajo-yarn help";
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(CommandLine cl) throws Exception {
    printHelpFor(cl.getArgList());
  }

  public void printHelpFor(Collection<String> args) {
    if (args == null || args.size() < 1) {
      args = commands.keySet();
    }
    HelpFormatter f = new HelpFormatter();
    for (String command : args) {
      ClientCommand c = commands.get(command);
      if (c != null) {
        //TODO Show any arguments to the commands.
        f.printHelp(command, c.getHeaderDescription(), c.getOpts(), null);
      } else {
        System.err.println("ERROR: " + c + " is not a supported command.");
        //TODO make this exit with an error at some point
      }
    }
  }
}

