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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ClusterScheduler {
  private static final Log LOG = LogFactory.getLog(ClusterScheduler.class);

  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NMCallbackHandler containerListener;

  private final AppContext appContext;

  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private final AMRMClientAsync amRmClient;

  private final String appMasterTrackingUrl;

  private final int appMasterRpcPort;

  private volatile boolean done;

  public ClusterScheduler(AppContext appContext, String trackingUrl, int rpcPort) {
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    this.amRmClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    this.containerListener = new NMCallbackHandler(this);
    this.nmClientAsync = new NMClientAsyncImpl(containerListener);
    this.appContext = appContext;
    this.appMasterTrackingUrl = trackingUrl;
    this.appMasterRpcPort = rpcPort;
  }

  public void init(Configuration conf) throws  YarnException, IOException {
    amRmClient.init(conf);
    nmClientAsync.init(conf);
  }

  public void service() throws YarnException, IOException {
    // Register self with ResourceManager
    amRmClient.start();
    nmClientAsync.start();
    RegisterApplicationMasterResponse response = amRmClient
        .registerApplicationMaster(appContext.getMasterHost(), appMasterRpcPort,
            appMasterTrackingUrl);
  }


  public void addContainerRequest(TajoContainerRequest containerRequest) {
    amRmClient.addContainerRequest(containerRequest);
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      for(ContainerStatus status: statuses) {
        LOG.info("STATUS:" + status.getExitStatus());
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {

      if (LOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        for (Container container : containers) {
          sb.append(container.getId()).append(", ");
        }
        LOG.debug("Assigned New Containers: " + sb.toString());
      }

      List<Container> modifiableContainerList = Lists.newLinkedList(containers);

      Iterator<Container> containerIterator = modifiableContainerList.iterator();
      while (containerIterator.hasNext()) {
        Container container = containerIterator.next();
        TajoContainerRequest containerRequest = getMatchingRequest(container, ResourceRequest.ANY);
        if(containerRequest != null) {
          LOG.info("Start container: " + container.getId());
          ContainerTask task = containerRequest.getTask();
          try {
            nmClientAsync.startContainerAsync(container, task.getLaunchContext(container));
          } catch (IOException ioe) {
            LOG.error("", ioe);
          }
        }
      }

     }

    private TajoContainerRequest getMatchingRequest(Container container,
                                                    String location) {

      Priority priority = container.getPriority();
      Resource capability = container.getResource();
      List<? extends Collection<TajoContainerRequest>> requestsList =
          amRmClient.getMatchingRequests(priority, location, capability);

      if (!requestsList.isEmpty()) {
        // pick first one
        for (Collection<TajoContainerRequest> requests : requestsList) {
          for (TajoContainerRequest cookieContainerRequest : requests) {
              return cookieContainerRequest;
          }
        }
      }
      return null;
    }

    @Override
    public void onShutdownRequest() {
      LOG.info("SHUT DOWN");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {

    }

    @Override
    public float getProgress() {
      return 50;
    }

    @Override
    public void onError(Throwable e) {
      LOG.info("Error", e);
    }
  }

  @VisibleForTesting
  class NMCallbackHandler
      implements NMClientAsync.CallbackHandler {
    private final ClusterScheduler scheduler;

    public NMCallbackHandler(ClusterScheduler scheduler) {
      this.scheduler = scheduler;
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

    }

    @Override
    public void onContainerStopped(ContainerId containerId) {

    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.info("Get Error", t);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.info("Get Status Error", t);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.info("Get Stop Error", t);
    }
  }




}
