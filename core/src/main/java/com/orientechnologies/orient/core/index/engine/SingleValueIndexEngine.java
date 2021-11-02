package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;

public interface SingleValueIndexEngine extends V1IndexEngine {
  boolean validatedPut(
      OAtomicOperation atomicOperation, Object key, ORID value, Validator<Object, ORID> validator);

  boolean remove(OAtomicOperation atomicOperation, Object key) throws IOException;

  @Override
  default boolean isMultiValue() {
    return false;
  }
}
