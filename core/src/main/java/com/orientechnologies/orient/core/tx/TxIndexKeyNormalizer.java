package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.id.ORID;

public interface TxIndexKeyNormalizer {
  byte[] normalize(final Object key, final ORID rid);
}
