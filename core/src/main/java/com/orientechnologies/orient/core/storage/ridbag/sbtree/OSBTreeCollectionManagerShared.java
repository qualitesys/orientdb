/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OAccessToSBtreeCollectionManagerIsProhibitedException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.BTreeBonsaiGlobal;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.BTree;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.EdgeKey;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/** @author Artem Orobets (enisher-at-gmail.com) */
public final class OSBTreeCollectionManagerShared
    implements OSBTreeCollectionManager, OOrientStartupListener, OOrientShutdownListener {
  public static final String DEFAULT_EXTENSION = ".grb";
  public static final String FILE_NAME_PREFIX = "global_collection_";

  /**
   * Message which is provided during throwing of {@link
   * OAccessToSBtreeCollectionManagerIsProhibitedException}.
   */
  private static final String PROHIBITED_EXCEPTION_MESSAGE =
      "Access to the manager of RidBags "
          + "which are based on B-Tree "
          + "implementation is prohibited. Typically it means that you use database under distributed "
          + "cluster configuration. Please check "
          + "that following setting in your server configuration "
          + OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getKey()
          + " is set to "
          + Integer.MAX_VALUE;

  private final OAbstractPaginatedStorage storage;

  /**
   * If this flag is set to {@code true} then all access to the manager will be prohibited and
   * exception {@link OAccessToSBtreeCollectionManagerIsProhibitedException} will be thrown.
   */
  private volatile boolean prohibitAccess = false;

  private final ConcurrentHashMap<Integer, BTree> fileIdBTreeMap = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, Integer> clusterIdFileIdMap = new ConcurrentHashMap<>();

  private final AtomicLong ridBagIdCounter = new AtomicLong();

  public OSBTreeCollectionManagerShared(OAbstractPaginatedStorage storage) {
    this.storage = storage;
  }



  /**
   * Once this method is called any attempt to load/create/delete b-tree will be resulted in
   * exception thrown.
   */
  public void prohibitAccess() {
    prohibitAccess = true;
  }

  private void checkAccess() {
    if (prohibitAccess) {
      throw new OAccessToSBtreeCollectionManagerIsProhibitedException(PROHIBITED_EXCEPTION_MESSAGE);
    }
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(
      final OAtomicOperation atomicOperation, final int clusterId) {
    checkAccess();

    return doCreateRidBag(atomicOperation, clusterId);
  }

  private BTreeBonsaiGlobal doCreateRidBag(OAtomicOperation atomicOperation, int clusterId) {
    final BTree[] bTree = new BTree[1];
    final int fileId =
        clusterIdFileIdMap.compute(
            clusterId,
            (key, value) -> {
              if (value != null) {
                throw new OStorageException(
                    "RidBag for cluster id " + clusterId + " already exists");
              }

              bTree[0] = new BTree(storage, FILE_NAME_PREFIX + clusterId, DEFAULT_EXTENSION);
              bTree[0].create(atomicOperation);

              final int intFileId = OWOWCache.extractFileId(bTree[0].getFileId());
              fileIdBTreeMap.put(intFileId, bTree[0]);

              return intFileId;
            });

    long nextRidBagId;

    while (true) {
      nextRidBagId = ridBagIdCounter.incrementAndGet();

      if (nextRidBagId < 0) {
        ridBagIdCounter.compareAndSet(nextRidBagId, -nextRidBagId);
        continue;
      }

      try (final Stream<ORawPair<EdgeKey, Integer>> stream =
          bTree[0].iterateEntriesBetween(
              new EdgeKey(nextRidBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
              true,
              new EdgeKey(nextRidBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
              true,
              true)) {
        if (!stream.findAny().isPresent()) {
          break;
        }
      }
    }

    return new BTreeBonsaiGlobal(
        bTree[0],
        fileId,
        clusterId,
        nextRidBagId,
        OLinkSerializer.INSTANCE,
        OIntegerSerializer.INSTANCE);
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(
      OBonsaiCollectionPointer collectionPointer) {
    final int fileId = (int) collectionPointer.getFileId();
    final BTree bTree = fileIdBTreeMap.get(fileId);

    return new BTreeBonsaiGlobal(
        bTree,
        fileId,
        collectionPointer.getRootPointer().getPageOffset(),
        collectionPointer.getRootPointer().getPageIndex(),
        OLinkSerializer.INSTANCE,
        OIntegerSerializer.INSTANCE);
  }

  @Override
  public void releaseSBTree(final OBonsaiCollectionPointer collectionPointer) {}

  @Override
  public void delete(final OBonsaiCollectionPointer collectionPointer) {}

  @Override
  public OBonsaiCollectionPointer createSBTree(
      OAtomicOperation atomicOperation, int clusterId, UUID ownerUUID) {
    checkAccess();

    final BTreeBonsaiGlobal bonsaiGlobal = doCreateRidBag(atomicOperation, clusterId);
    final OBonsaiCollectionPointer pointer = bonsaiGlobal.getCollectionPointer();

    if (ownerUUID != null) {
      Map<UUID, OBonsaiCollectionPointer> changedPointers =
          ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges();
      if (pointer != null && pointer.isValid()) {
        changedPointers.put(ownerUUID, pointer);
      }
    }

    return pointer;
  }

  /** Change UUID to null to prevent its serialization to disk. */
  @Override
  public UUID listenForChanges(ORidBag collection) {
    UUID ownerUUID = collection.getTemporaryId();
    if (ownerUUID != null) {
      final OBonsaiCollectionPointer pointer = collection.getPointer();
      ODatabaseDocumentInternal session = ODatabaseRecordThreadLocal.instance().get();
      Map<UUID, OBonsaiCollectionPointer> changedPointers = session.getCollectionsChanges();
      if (pointer != null && pointer.isValid()) {
        changedPointers.put(ownerUUID, pointer);
      }
    }

    return null;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer) {}

  @Override
  public void clearPendingCollections() {}

  @Override
  public Map<UUID, OBonsaiCollectionPointer> changedIds() {
    return ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges();
  }

  @Override
  public void clearChangedIds() {
    ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges().clear();
  }

  @Override
  public void onShutdown() {}

  @Override
  public void onStartup() {}

  public void close() {
    fileIdBTreeMap.clear();
    clusterIdFileIdMap.clear();
  }

  public boolean delete(
      OAtomicOperation atomicOperation, OBonsaiCollectionPointer collectionPointer) {
    final int fileId = (int) collectionPointer.getFileId();
    final BTree bTree = fileIdBTreeMap.get(fileId);
    if (bTree == null) {
      throw new OStorageException(
          "RidBug for with collection pointer " + collectionPointer + " does not exist");
    }

    final long ridBagId = collectionPointer.getRootPointer().getPageIndex();

    try (Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      stream.forEach(pair -> bTree.remove(atomicOperation, pair.first));
    }

    return true;
  }
}
