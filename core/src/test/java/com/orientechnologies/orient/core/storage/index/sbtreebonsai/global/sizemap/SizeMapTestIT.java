package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.sizemap;

import static org.junit.Assert.*;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class SizeMapTestIT {

  public static final String DIR_NAME = "/sizeMapTest";
  public static final String DB_NAME = "sizeMapTest";
  private static OrientDB orientDB;
  private static OAtomicOperationsManager atomicOperationsManager;
  private static OAbstractPaginatedStorage storage;
  private static String buildDirectory;

  private SizeMap sizeMap;

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) {
      buildDirectory = "./target" + DIR_NAME;
    } else {
      buildDirectory += DIR_NAME;
    }

    OFileUtils.deleteRecursively(new File(buildDirectory));

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());

    if (orientDB.exists(DB_NAME)) {
      orientDB.drop(DB_NAME);
    }

    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

    ODatabaseSession databaseSession = orientDB.open(DB_NAME, "admin", "admin");
    storage = (OAbstractPaginatedStorage) ((ODatabaseInternal<?>) databaseSession).getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    databaseSession.close();
  }

  @AfterClass
  public static void afterClass() {
    orientDB.drop(DB_NAME);
    orientDB.close();

    OFileUtils.deleteRecursively(new File(buildDirectory));
  }

  @Before
  public void setUp() throws Exception {
    sizeMap = new SizeMap(storage, "sizeMap", ".sm");
    atomicOperationsManager.executeInsideAtomicOperation(
        (atomicOperation) -> sizeMap.create(atomicOperation));
  }

  @After
  public void tearDown() throws Exception {
    atomicOperationsManager.executeInsideAtomicOperation(
        (atomicOperation) -> sizeMap.delete(atomicOperation));
  }
}
