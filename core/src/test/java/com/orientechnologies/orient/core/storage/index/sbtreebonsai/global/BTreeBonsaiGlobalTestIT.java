package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BTreeBonsaiGlobalTestIT {

  private static OrientDB orientDB;
  private static BTreeBonsaiGlobal bTree;
  private static OAtomicOperationsManager atomicOperationsManager;
  private static OAbstractPaginatedStorage storage;
  private static String buildDirectory;

  @Parameterized.Parameters
  public static Iterable<Integer> keysCount() {
    return IntStream.range(1, 21).map(val -> 1 << val).boxed().collect(Collectors.toList());
  }

  private final int keysCount;

  public BTreeBonsaiGlobalTestIT(int keysCount) {
    this.keysCount = keysCount;
  }

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) {
      buildDirectory = "./target/globalSBTreeBonsaiTest";
    } else {
      buildDirectory += "/globalSBTreeBonsaiTest";
    }

    OFileUtils.deleteRecursively(new File(buildDirectory));

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());

    if (orientDB.exists("bonsaiTest")) {
      orientDB.drop("bonsaiTest");
    }

    orientDB.create("bonsaiTest", ODatabaseType.PLOCAL);

    ODatabaseSession databaseSession = orientDB.open("bonsaiTest", "admin", "admin");
    storage = (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseSession).getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    databaseSession.close();
  }

  @AfterClass
  public static void afterClass() {
    orientDB.drop("bonsaiTest");
    orientDB.close();

    OFileUtils.deleteRecursively(new File(buildDirectory));
  }

  @Before
  public void beforeMethod() throws Exception {

    bTree = new BTreeBonsaiGlobal(storage, "bonsaiGlobal", ".sbc");
    atomicOperationsManager.executeInsideAtomicOperation(
        (atomicOperation) -> bTree.create(atomicOperation));
  }

  @After
  public void afterMethod() throws Exception {
    atomicOperationsManager.executeInsideAtomicOperation(
        (atomicOperation) -> bTree.delete(atomicOperation));
  }

  @Test
  public void testKeyPut() throws Exception {
    EdgeKey firstKey = null;
    EdgeKey lastKey = null;
    long start = System.nanoTime();
    for (int i = 0; i < keysCount; i++) {
      final int index = i;
      final EdgeKey key = new EdgeKey(42, index % 32000, index);
      atomicOperationsManager.executeInsideAtomicOperation(
          (atomicOperation) -> bTree.put(atomicOperation, key, index + 1));

      if (firstKey == null) {
        firstKey = key;
        lastKey = key;
      } else {
        if (key.compareTo(lastKey) > 0) {
          lastKey = key;
        }
        if (key.compareTo(firstKey) < 0) {
          firstKey = key;
        }
      }
    }
    long end = System.nanoTime();
    System.out.printf("%d us per insert%n", (end - start) / 1_000 / keysCount);

    start = System.nanoTime();
    for (int i = 0; i < keysCount; i++) {
      Assertions.assertThat(bTree.get(new EdgeKey(42, i % 32000, i))).isEqualTo(i + 1);
    }
    end = System.nanoTime();

    System.out.printf("%d us per get%n", (end - start) / 1_000 / keysCount);

    Assert.assertEquals(firstKey, bTree.firstKey());
    Assert.assertEquals(lastKey, bTree.lastKey());

    for (int i = keysCount; i < keysCount + 100; i++) {
      Assert.assertEquals(bTree.get(new EdgeKey(42, i % 32000, i)), -1);
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<EdgeKey> keys = new TreeSet<>();
    final Random random = new Random();
    final int keysCount = 1_000_000;

    while (keys.size() < keysCount) {
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> {
            int val = random.nextInt(Integer.MAX_VALUE);
            final EdgeKey key = new EdgeKey(42, val, val);
            bTree.put(atomicOperation, key, val);

            keys.add(key);
            Assert.assertEquals(bTree.get(key), val);
          });
    }

    Assert.assertEquals(bTree.firstKey(), keys.first());
    Assert.assertEquals(bTree.lastKey(), keys.last());

    for (EdgeKey key : keys) {
      Assert.assertEquals(bTree.get(key), key.targetPosition);
    }
  }

  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    NavigableSet<EdgeKey> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();
    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);
    final int keysCount = 1_000_000;

    while (keys.size() < keysCount) {
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> {
            int val;
            do {
              val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
            } while (val < 0);

            final EdgeKey key = new EdgeKey(42, val, val);
            bTree.put(atomicOperation, key, val);

            keys.add(key);
            Assert.assertEquals(bTree.get(key), val);
          });
    }

    Assert.assertEquals(bTree.firstKey(), keys.first());
    Assert.assertEquals(bTree.lastKey(), keys.last());

    for (EdgeKey key : keys) {
      Assert.assertEquals(bTree.get(key), key.targetPosition);
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    final int keysCount = 10214 * 1024;

    NavigableSet<EdgeKey> keys = new TreeSet<>();
    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      final int val = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> bTree.put(atomicOperation, key, val));
      keys.add(key);
    }

    Iterator<EdgeKey> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      EdgeKey key = keysIterator.next();
      if (key.targetPosition % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> bTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
    }

    if (!keys.isEmpty()) {
      Assert.assertEquals(bTree.firstKey(), keys.first());
      Assert.assertEquals(bTree.lastKey(), keys.last());
    } else {
      Assert.assertNull(bTree.firstKey());
      Assert.assertNull(bTree.lastKey());
    }

    for (final EdgeKey key : keys) {
      if (key.targetPosition % 3 == 0) {
        Assert.assertEquals(-1, bTree.get(key));
      } else {
        Assert.assertEquals(key.targetPosition, bTree.get(key));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws Exception {
    int keysCount = 1024 * 1024;

    NavigableSet<EdgeKey> keys = new TreeSet<>();

    long seed = System.currentTimeMillis();
    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < keysCount) {
      final int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (val < 0) {
        continue;
      }

      EdgeKey key = new EdgeKey(42, val, val);
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> bTree.put(atomicOperation, key, val));
      keys.add(key);

      Assert.assertEquals(bTree.get(key), val);
    }

    Iterator<EdgeKey> keysIterator = keys.iterator();

    while (keysIterator.hasNext()) {
      EdgeKey key = keysIterator.next();

      if (key.targetPosition % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> bTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
    }

    if (!keys.isEmpty()) {
      Assert.assertEquals(bTree.firstKey(), keys.first());
      Assert.assertEquals(bTree.lastKey(), keys.last());
    } else {
      Assert.assertNull(bTree.firstKey());
      Assert.assertNull(bTree.lastKey());
    }

    for (EdgeKey key : keys) {
      if (key.targetPosition % 3 == 0) {
        Assert.assertEquals(-1, bTree.get(key));
      } else {
        Assert.assertEquals(bTree.get(key), key.targetPosition);
      }
    }
  }

  @Test
  public void testKeyDelete() throws Exception {
    System.out.println("Keys count " + keysCount);

    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      final int val = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> bTree.put(atomicOperation, key, val));
    }

    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      if (key.targetPosition % 3 == 0) {

        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> Assert
                .assertEquals(bTree.remove(atomicOperation, key), key.targetPosition));
      }
    }

    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      if (i % 3 == 0) {
        Assert.assertEquals(-1, bTree.get(key));
      } else {
        Assert.assertEquals(i, bTree.get(key));
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    System.out.println("Keys count " + keysCount);

    for (int i = 0; i < keysCount; i++) {
      final EdgeKey key = new EdgeKey(42, i, i);
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> bTree.put(atomicOperation, key, key.targetCluster % 5));

      Assert.assertEquals(bTree.get(key), key.targetCluster % 5);
    }

    for (int i = 0; i < keysCount; i++) {
      final int index = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> {
            if (index % 3 == 0) {
              final EdgeKey key = new EdgeKey(42, index, index);
              Assert.assertEquals(bTree.remove(atomicOperation, key), key.targetCluster % 5);
            }

            if (index % 2 == 0) {
              final EdgeKey key = new EdgeKey(42, index + keysCount, index + keysCount);
              bTree.put(atomicOperation, key, (index + keysCount) % 5);
            }
          });
    }

    for (int i = 0; i < keysCount; i++) {
      {
        final EdgeKey key = new EdgeKey(42, i, i);
        if (i % 3 == 0) {
          Assert.assertEquals(-1, bTree.get(key));
        } else {
          Assert.assertEquals(i % 5, bTree.get(key));
        }
      }

      if (i % 2 == 0) {
        final EdgeKey key = new EdgeKey(42, i + keysCount, i + keysCount);
        Assert.assertEquals(bTree.get(key), (i + keysCount) % 5);
      }
    }
  }
}
