package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.index.engine.BaseIndexEngine;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 9/4/2015
 */
public interface OIndexEngineCallback<T> {
  T callEngine(BaseIndexEngine engine);
}
