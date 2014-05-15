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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tajo.yarn.thrift.TajoYarnService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;

import java.io.IOException;

public class StartMasterCommand extends TajoAppCommand {
  private static final Log LOG = LogFactory.getLog(StartMasterCommand.class);

  protected ApplicationId applicationId;

  public StartMasterCommand(Configuration conf) {
    super(conf);
  }

  @Override
  public String getHeaderDescription() {
    return "tajo-yarn master ";
  }

  @Override
  public Options getOpts() {
    Options opts = new Options();
    opts.addOption("appId", true, "(Required) The tajo clusters app ID");
    return opts;
  }

  protected TajoYarnService.Client getClient() throws YarnException, IOException, TTransportException {
    ApplicationReport app = yarnClient.getApplicationReport(applicationId);
    LOG.info("Connecting to application rpc server " + app.getHost() + ":" + app.getRpcPort());
    TTransport transport = new TFramedTransport(new TSocket(app.getHost(), app.getRpcPort()));
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    TajoYarnService.Client client = new TajoYarnService.Client(protocol);
    return client;
  }

  @Override
  public void process(CommandLine cl) throws Exception {
    LOG.info("Running Client");
    yarnClient.start();

    String appIdStr = cl.getOptionValue("appId");
    if (appIdStr == null) {
      throw new IllegalArgumentException("-appId is required");
    }
    applicationId = ConverterUtils.toApplicationId(appIdStr);
    getClient().startMaster();
  }
}
