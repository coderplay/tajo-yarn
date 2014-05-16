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
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tajo.yarn.thrift.TajoYarnService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

/**
 * Base command class for Tajo operations
 */
public abstract class TajoCommand implements Command {

  private static final Log LOG = LogFactory.getLog(TajoCommand.class);

  /**
   * appliction id sepcify a certain tajo cluster
   */
  protected ApplicationId applicationId;
  /**
   * Thrift server connection
   */
  private TTransport transport;

  // Configuration
  protected Configuration conf;
  protected YarnClient yarnClient;

  public TajoCommand(Configuration conf) {
    this.conf = conf;
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
  }

  @Override
  public Options getOpts() {
    Options opts = new Options();
    opts.addOption("appId", true, "(Required) The tajo clusters app ID");
    return opts;
  }

  protected TajoYarnService.Client getProtocol()
      throws YarnException, IOException, TTransportException {
    ApplicationReport app = yarnClient.getApplicationReport(applicationId);
    LOG.info("Connecting to application rpc server " + app.getHost() + ":" + app.getRpcPort());
    transport = new TFramedTransport(new TSocket(app.getHost(), app.getRpcPort()));
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    TajoYarnService.Client client = new TajoYarnService.Client(protocol);
    return client;
  }

  protected void closeProtocol() {
    transport.close();
  }


  @Override
  public void process(CommandLine cl) throws Exception {
    String appIdStr = cl.getOptionValue("appId");
    if (appIdStr == null) {
      throw new IllegalArgumentException("-appId is required");
    }
    applicationId = ConverterUtils.toApplicationId(appIdStr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running Client");
    }
    yarnClient.start();
  }

}
