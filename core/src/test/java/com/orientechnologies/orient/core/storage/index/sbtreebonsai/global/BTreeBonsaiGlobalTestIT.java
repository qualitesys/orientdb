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
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BTreeBonsaiGlobalTestIT {

  private static final int KEYS_COUNT = 2_000_000;

  private static OrientDB orientDB;
  private static BTreeBonsaiGlobal bTree;
  private static OAtomicOperationsManager atomicOperationsManager;
  private static OAbstractPaginatedStorage storage;
  private static String buildDirectory;

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

    long start = System.nanoTime();
    for (int i = 0; i < KEYS_COUNT; i++) {
      final int index = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          (atomicOperation) -> bTree.put(atomicOperation, index % 32000, index, index + 1));
    }
    long end = System.nanoTime();
    System.out.printf("%d us per insert%n", (end - start) / 1_000 / KEYS_COUNT);

    start = System.nanoTime();
    for (int i = 0; i < KEYS_COUNT; i++) {
      Assertions.assertThat(bTree.get(i % 32000, i)).isEqualTo(i + 1);
    }
    end = System.nanoTime();

    System.out.printf("%d us per get%n", (end - start) / 1_000 / KEYS_COUNT);

    //          Assert.assertEquals(0, (int) sbTree.firstKey());
    //          Assert.assertEquals(KEYS_COUNT - 1, (int) sbTree.lastKey());

    for (int i = KEYS_COUNT; i < KEYS_COUNT + 100; i++) {
      Assert.assertEquals(bTree.get(i % 32000, i), -1);
    }
  }
}
