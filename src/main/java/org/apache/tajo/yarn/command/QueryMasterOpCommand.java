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
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.yarn.thrift.TajoYarnService;

public class QueryMasterOpCommand extends TajoCommand {
  public QueryMasterOpCommand(Configuration conf) {
    super(conf);
  }

  @Override
  public String getHeaderDescription() {
    return "tajo-yarn qm";
  }

  @Override
  public Options getOpts() {
    Options opts = super.getOpts();
    opts.addOption("add", true, "add a number of querymasters to the cluster");
    opts.addOption("remove", true, "decomission a number of queryMasters from the cluster");
    return opts;
  }

  @Override
  public void process(CommandLine cl) throws Exception {
    super.process(cl);
    if((!cl.hasOption("add")) && (!cl.hasOption("remove"))) {
      throw new IllegalArgumentException(
          "You need to specify at least one of two options: -add or -remove");
    }

    TajoYarnService.Client client = getProtocol();
    if(cl.hasOption("add")) {
      int num = Integer.parseInt(cl.getOptionValue("add"));
      if(num < 0) {
        throw new IllegalArgumentException(
            "number of query masters must be > 0");
      }
      client.addQueryMaster(num);
    } else {
      int num = Integer.parseInt(cl.getOptionValue("remove"));
      if(num < 0) {
        throw new IllegalArgumentException(
            "number of query masters must be > 0");
      }
      client.removeQueryMaster(num);
    }

    closeProtocol();
  }



}
