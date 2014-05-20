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
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.yarn.thrift.TajoYarnService;
import org.apache.thrift.TException;

public class TajoYarnServiceImpl implements TajoYarnService.Iface {
  private static final Log LOG = LogFactory.getLog(TajoYarnServiceImpl.class);


  private final Priority DEFAULT_PRIORITY = Records.newRecord(Priority.class);

  private ClusterScheduler scheduler;

  private TajoOnYarn tajoOnYarn;

  public TajoYarnServiceImpl(ClusterScheduler scheduler, TajoOnYarn tajoOnYarn) {
    this.scheduler = scheduler;
    this.tajoOnYarn = tajoOnYarn;
  }

  @Override
  public void addQueryMaster(int number) throws TException {
    for(int i = 0; i < number; ++i) {
      TajoContainerRequest containerAsk = tajoOnYarn.setupContainerAskForQM();
      scheduler.addContainerRequest(containerAsk);
    }
  }

  @Override
  public void removeQueryMaster(int number) throws TException {

  }

  @Override
  public void addTaskRunners(int number) throws TException {
    for(int i = 0; i < number; ++i) {
      TajoContainerRequest containerAsk = tajoOnYarn.setupContainerAskForTR();
      scheduler.addContainerRequest(containerAsk);
    }
  }

  @Override
  public void removeTaskRunners(int number) throws TException {

  }

  @Override
  public void shutdown() throws TException {

  }


}
