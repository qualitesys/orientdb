package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.sizemap;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import java.io.IOException;

public final class SizeMap extends ODurableComponent {

  private long fileId;

  public SizeMap(
      OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }

  public void create(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        (operation) -> {
          acquireExclusiveLock();
          try {
            fileId = addFile(atomicOperation, getFullName());

            final OCacheEntry cacheEntry = addPage(atomicOperation, fileId);
            try {
              assert cacheEntry.getPageIndex() == 0;

              final EntryPoint entryPoint = new EntryPoint(cacheEntry);
              entryPoint.setFileSize(0);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void load() {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Exception during loading of rid bag " + getName()), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            deleteFile(atomicOperation, fileId);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public int addTree(final OAtomicOperation atomicOperation) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            int fileSize;
            {
              final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, 0, false);
              try {
                final EntryPoint entryPoint = new EntryPoint(cacheEntry);
                fileSize = entryPoint.getFileSize();
              } finally {
                releasePageFromRead(atomicOperation, cacheEntry);
              }
            }

            if (fileSize == 0) {
              // add new page and entry

              final int localRidBagId;
              final OCacheEntry newEntry = addPage(atomicOperation, fileId);
              try {
                final Bucket bucket = new Bucket(newEntry);
                bucket.init();

                localRidBagId = bucket.addEntry();
                assert localRidBagId >= 0;

              } finally {
                releasePageFromWrite(atomicOperation, newEntry);
              }

              final OCacheEntry stateEntry =
                  loadPageForWrite(atomicOperation, fileId, 0, false, true);
              try {
                final EntryPoint entryPoint = new EntryPoint(stateEntry);
                entryPoint.setFileSize(1);
              } finally {
                releasePageFromWrite(atomicOperation, stateEntry);
              }

              return localRidBagId;
            } else {
              // 1. load page
              // 2. try to add and if it is failed add new page and repeat

              int localRidBagId;
              OCacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, fileId, fileSize, false, true);
              while (true) {
                try {
                  final Bucket bucket = new Bucket(cacheEntry);
                  localRidBagId = bucket.addEntry();
                  if (localRidBagId >= 0) {
                    break;
                  }
                } finally {
                  releasePageFromWrite(atomicOperation, cacheEntry);
                }

                cacheEntry = addPage(atomicOperation, fileId);

                final OCacheEntry stateEntry =
                    loadPageForWrite(atomicOperation, fileId, 0, false, true);
                try {
                  final EntryPoint entryPoint = new EntryPoint(stateEntry);
                  entryPoint.setFileSize(fileSize + 1);
                  fileSize++;
                } finally {
                  releasePageFromWrite(atomicOperation, stateEntry);
                }
              }

              return localRidBagId + (fileSize - 1) * Bucket.MAX_BUCKET_SIZE;
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void incrementSize(final OAtomicOperation atomicOperation, final int ridBagId) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int pageIndex = ridBagId / Bucket.MAX_BUCKET_SIZE;
            final int localRidBagId = pageIndex - ridBagId * Bucket.MAX_BUCKET_SIZE;

            final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            try {
              final Bucket bucket = new Bucket(cacheEntry);
              bucket.incrementSize(localRidBagId);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void decrementSize(final OAtomicOperation atomicOperation, final int ridBagId) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int pageIndex = ridBagId / Bucket.MAX_BUCKET_SIZE;
            final int localRidBagId = pageIndex - ridBagId * Bucket.MAX_BUCKET_SIZE;

            final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            try {
              final Bucket bucket = new Bucket(cacheEntry);
              bucket.decrementSize(localRidBagId);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public int getSize(final int ridBagId) {
    acquireSharedLock();
    try {
      final int pageIndex = ridBagId / Bucket.MAX_BUCKET_SIZE;
      final int localRidBagId = pageIndex - ridBagId * Bucket.MAX_BUCKET_SIZE;

      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final Bucket bucket = new Bucket(cacheEntry);
        return bucket.getSize(localRidBagId);
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during reading the size of rid bag"), e);
    } finally {
      releaseSharedLock();
    }
  }

  public void remove(final OAtomicOperation atomicOperation, final int ridBagId) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int pageIndex = ridBagId / Bucket.MAX_BUCKET_SIZE;
            final int localRidBagId = pageIndex - ridBagId * Bucket.MAX_BUCKET_SIZE;

            final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            try {
              final Bucket bucket = new Bucket(cacheEntry);
              bucket.delete(localRidBagId);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }
}
