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

import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.Field;
import java.net.ServerSocket;


public class ThriftHelper {
  /**
   * Thrift doesn't provide access to socket it creates,
   * this is the only way to know what ephemeral port we bound to.
   * TODO: Patch thrift to provide access so we don't have to do this.
   */
  static ServerSocket getServerSocketFor(TNonblockingServerSocket thriftSocket)
      throws TTransportException {
    try {
      Field field = TNonblockingServerSocket.class.getDeclaredField("serverSocket_");
      field.setAccessible(true);
      return (ServerSocket) field.get(thriftSocket);
    } catch (NoSuchFieldException e) {
      throw new TTransportException("Couldn't get listening port", e);
    } catch (SecurityException e) {
      throw new TTransportException("Couldn't get listening port", e);
    } catch (IllegalAccessException e) {
      throw new TTransportException("Couldn't get listening port", e);
    }
  }
}
