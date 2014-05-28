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
import org.apache.tajo.yarn.thrift.TajoYarnService;
import org.apache.thrift.TException;

import java.io.IOException;

public class TajoYarnServiceImpl implements TajoYarnService.Iface {
  private static final Log LOG = LogFactory.getLog(TajoYarnServiceImpl.class);

  private ClusterScheduler scheduler;

  private AppContext appContext;

  public TajoYarnServiceImpl(ClusterScheduler scheduler, AppContext appContext) {
    this.scheduler = scheduler;
    this.appContext = appContext;
  }

  @Deprecated
  @Override
  public void addQueryMaster(int number) throws TException {
  }

  @Deprecated
  @Override
  public void removeQueryMaster(int number) throws TException {

  }

  @Deprecated
  @Override
  public void addTaskRunners(int number) throws TException {
  }

  @Deprecated
  @Override
  public void removeTaskRunners(int number) throws TException {

  }

  @Override
  public void addWorker(int number) throws TException {
    for(int i = 0; i < number; ++i) {
      try {
        ContainerTask task = new WorkerContainerTask(appContext);
        TajoContainerRequest containerAsk = task.getContainerRequest();
        scheduler.addContainerRequest(containerAsk);
      } catch(IOException ioe) {
        throw new TException(ioe);
      }
    }
  }

  @Override
  public void removeWorker(int number) throws TException {

  }

  @Override
  public void shutdown() throws TException {

  }


}
