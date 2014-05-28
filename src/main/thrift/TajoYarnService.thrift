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

namespace java org.apache.tajo.yarn.thrift

service TajoYarnService {
  // add a number of tajo workers to the cluster
  void addWorker(1: i32 number);

  // add a number of tajo workers to the cluster
  void removeWorker(1: i32 number);

  // shutdown the cluster
  void shutdown();


  // add a number of querymasters to the cluster
  void addQueryMaster(1: i32 number);

  // decomission a number of queryMasters from the cluster
  void removeQueryMaster(1: i32 number);

  // add a number of taskrunners to the cluster
  void addTaskRunners(1: i32 number);

  // decomission a number of taskrunners from the cluster
  void removeTaskRunners(1: i32 number);


}
