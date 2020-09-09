package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;

final class EdgeKeySerializer implements OBinarySerializer<EdgeKey> {

  static final EdgeKeySerializer INSTANCE = new EdgeKeySerializer();

  @Override
  public int getObjectSize(EdgeKey object, Object... hints) {
    return IntSerializer.INSTANCE.getObjectSize(object.targetCluster)
        + LongSerializer.getObjectSize(object.targetPosition);
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return doGetObjectSize(stream, startPosition);
  }

  private int doGetObjectSize(byte[] stream, int startPosition) {
    int size = IntSerializer.INSTANCE.getObjectSize(stream, startPosition);
    return size + LongSerializer.getObjectSize(stream, startPosition + size);
  }

  @Override
  public void serialize(EdgeKey object, byte[] stream, int startPosition, Object... hints) {
    doSerialize(object, stream, startPosition);
  }

  private void doSerialize(EdgeKey object, byte[] stream, int startPosition) {
    startPosition =
        IntSerializer.INSTANCE.serializePrimitive(stream, startPosition, object.targetCluster);

    LongSerializer.serialize(object.targetPosition, stream, startPosition);
  }

  @Override
  public EdgeKey deserialize(byte[] stream, int startPosition) {
    return doDeserialize(stream, startPosition);
  }

  private EdgeKey doDeserialize(byte[] stream, int startPosition) {
    int size = IntSerializer.INSTANCE.getObjectSize(stream, startPosition);
    final int targetCluster = IntSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += size;

    final long targetPosition = LongSerializer.deserialize(stream, startPosition);

    return new EdgeKey(targetCluster, targetPosition);
  }

  @Override
  public byte getId() {
    return -1;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    return -1;
  }

  @Override
  public void serializeNativeObject(
      EdgeKey object, byte[] stream, int startPosition, Object... hints) {
    doSerialize(object, stream, startPosition);
  }

  @Override
  public EdgeKey deserializeNativeObject(byte[] stream, int startPosition) {
    return doDeserialize(stream, startPosition);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return doGetObjectSize(stream, startPosition);
  }

  @Override
  public EdgeKey preprocess(EdgeKey value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(EdgeKey object, ByteBuffer buffer, Object... hints) {
    IntSerializer.INSTANCE.serializeInByteBufferObject(object.targetCluster, buffer);
    LongSerializer.serialize(object.targetPosition, buffer);
  }

  @Override
  public EdgeKey deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int targetCluster = IntSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    final long targetPosition = LongSerializer.deserialize(buffer);

    return new EdgeKey(targetCluster, targetPosition);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    final int position = buffer.position();
    int size = IntSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer);

    buffer.position(position + size);

    return size + LongSerializer.getObjectSize(buffer);
  }

  @Override
  public EdgeKey deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    int size = IntSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer, walChanges, offset);
    final int targetCluster =
        IntSerializer.INSTANCE.deserializeFromByteBufferObject(buffer, walChanges, offset);
    offset += size;

    final long targetPosition = LongSerializer.deserialize(buffer, walChanges, offset);

    return new EdgeKey(targetCluster, targetPosition);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    int size = IntSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer, walChanges, offset);
    return size + LongSerializer.getObjectSize(buffer, walChanges, offset + size);
  }
}
