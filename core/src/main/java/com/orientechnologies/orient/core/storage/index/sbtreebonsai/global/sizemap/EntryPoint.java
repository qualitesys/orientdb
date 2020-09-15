package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.sizemap;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public final class EntryPoint extends ODurablePage {

  private static final int SIZE_OFFSET = NEXT_FREE_POSITION;

  public EntryPoint(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void setFileSize(int fileSize) {
    setIntValue(SIZE_OFFSET, fileSize);
  }

  public int getFileSize() {
    return getIntValue(SIZE_OFFSET);
  }
}
