package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.sizemap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public class Bucket extends ODurablePage {

  private static final int ENTRY_SIZE = Integer.SIZE / 8;
  private static final int PAGE_SIZE =
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
  public static final int MAX_BUCKET_SIZE = PAGE_SIZE / ENTRY_SIZE;

  private static final int SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int ENTRIES_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  public Bucket(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setIntValue(SIZE_OFFSET, 0);
  }

  public int addEntry() {
    final int currentSize = getIntValue(SIZE_OFFSET);
    if (currentSize >= MAX_BUCKET_SIZE) {
      return -1;
    }

    setIntValue(SIZE_OFFSET, currentSize + 1);
    final int entryPosition = ENTRIES_OFFSET + (currentSize + 1) * ENTRY_SIZE;
    setIntValue(entryPosition, 0);

    return currentSize + 1;
  }

  public void incrementSize(int ridBagId) {
    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int currentSize = getIntValue(entryPosition);
    if (currentSize < 0) {
      throw new OStorageException("RidBag is deleted and can not be used");
    }

    setIntValue(entryPosition, currentSize + 1);
  }

  public void decrementSize(int ridBagId) {
    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int currentSize = getIntValue(entryPosition);
    if (currentSize < 0) {
      throw new OStorageException("RidBag is deleted and can not be used");
    }

    if (currentSize == 0) {
      throw new OStorageException("RidBag is in invalid state because it does not have any items");
    }

    setIntValue(entryPosition, currentSize - 1);
  }

  public int getSize(int ridBagId) {
    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int currentSize = getIntValue(entryPosition);
    if (currentSize < 0) {
      throw new OStorageException("RidBag is deleted and can not be used");
    }

    return currentSize;
  }

  public void delete(int ridBagId) {
    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int currentSize = getIntValue(entryPosition);
    if (currentSize < 0) {
      throw new OStorageException("RidBag is already deleted and can not be used");
    }

    setIntValue(entryPosition, -1);
  }
}
