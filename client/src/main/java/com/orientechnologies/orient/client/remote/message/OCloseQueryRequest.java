/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;

public final class OCloseQueryRequest implements OBinaryRequest<OCloseQueryResponse> {

  String queryId;

  public OCloseQueryRequest(String queryId) {
    this.queryId = queryId;
  }

  public OCloseQueryRequest() {
  }

  @Override public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    network.writeString(queryId);
  }

  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    this.queryId = channel.readString();
  }

  @Override public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_CLOSE_QUERY;
  }

  @Override public String getDescription() {
    return "Close remote query";
  }

  @Override public OCloseQueryResponse createResponse() {
    return new OCloseQueryResponse();
  }

  @Override public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.closeQuery(this);
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }
}