package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;

public interface SingleValueBinaryKeyIndexEngine
    extends SingleValueIndexEngine, BaseBinaryKeyIndexEngine {
  boolean rawRemove(OAtomicOperation atomicOperation, byte[] key) throws IOException;
}
