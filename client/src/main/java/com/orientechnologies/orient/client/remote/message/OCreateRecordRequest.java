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

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryAsyncRequest;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OCreateRecordRequest implements OBinaryAsyncRequest<OCreateRecordResponse> {
  private byte[]    content;
  private ORecordId rid;
  private byte      recordType;
  private byte      mode;

  public OCreateRecordRequest() {
  }

  public byte getMode() {
    return mode;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_CREATE;
  }

  @Override
  public String getDescription() {
    return "Create Record";
  }

  public OCreateRecordRequest(byte[] iContent, ORecordId iRid, byte iRecordType) {
    this.content = iContent;
    this.rid = iRid;
    this.recordType = iRecordType;
  }

  @Override
  public void write(final OChannelBinaryAsynchClient network, final OStorageRemoteSession session) throws IOException {
    network.writeShort((short) rid.getClusterId());
    network.writeBytes(content);
    network.writeByte(recordType);
    network.writeByte((byte) mode);
  }

  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    final int dataSegmentId = protocolVersion < 24 ? channel.readInt() : 0;

    rid = new ORecordId(channel.readShort(), ORID.CLUSTER_POS_INVALID);
    content = channel.readBytes();
    recordType = channel.readByte();
    mode = channel.readByte();
  }

  public ORecordId getRid() {
    return rid;
  }

  public byte[] getContent() {
    return content;
  }

  public byte getRecordType() {
    return recordType;
  }

  @Override
  public void setMode(byte mode) {
    this.mode = mode;
  }

  @Override
  public OCreateRecordResponse createResponse() {
    return new OCreateRecordResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeCreateRecord(this);
  }

}