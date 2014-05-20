package org.apache.tajo.yarn;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;

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
public class TajoContainerRequest extends AMRMClient.ContainerRequest {
  LaunchContainerTask task;

  public TajoContainerRequest(
      Resource capability,
      String[] hosts,
      String[] racks,
      Priority priority,
      LaunchContainerTask cookie) {
    super(capability, hosts, racks, priority);
    this.task = cookie;
  }

  LaunchContainerTask getTask() {
    return task;
  }
}

