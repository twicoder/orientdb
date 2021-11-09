package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LinkKeyNormalizer implements KeyNormalizer {
  @Override
  public int normalizedSize(Object key) {
    final ORID rid = ((OIdentifiable) key).getIdentity();
    return OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int normalize(Object key, int offset, byte[] stream) {
    final ORID rid = ((OIdentifiable) key).getIdentity();
    final ByteBuffer buffer = ByteBuffer.wrap(stream);

    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.position(offset);

    buffer.putShort((short) (((short) rid.getClusterId()) + Short.MAX_VALUE + 1));
    buffer.putLong(rid.getClusterPosition() + Long.MAX_VALUE + 1);
    return buffer.position();
  }
}
