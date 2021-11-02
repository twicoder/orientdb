package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;

public interface MultiValueBinaryKeyIndexEngine extends BaseBinaryKeyIndexEngine {
  boolean rawRemove(OAtomicOperation atomicOperation, byte[] key, ORID value) throws IOException;
}
