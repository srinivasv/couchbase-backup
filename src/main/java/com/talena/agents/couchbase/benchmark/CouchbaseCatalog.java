package com.talena.agents.couchbase.benchmark;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

public class CouchbaseCatalog {
  private String catalogFilePath;
  private Map<Short, CatalogData> catalog;

  public CouchbaseCatalog(String catalogFilePath) {
    this.catalogFilePath = catalogFilePath;

    catalog = new HashMap<Short, CatalogData>();
  }

  public void load() {
    try {
      File file = new File(catalogFilePath);
      FileInputStream fIn = null;
      DataInputStream dataIn = null;
  
      fIn = new FileInputStream(file);
      dataIn = new DataInputStream(fIn);
  
      short key;
      CatalogData value;

      System.out.println("Loading into catalog ...");
      while (dataIn.available() != 0) {
        value = new CatalogData();

        key = dataIn.readShort();
        value.setvBucketUuid(dataIn.readLong());
        value.setLastSeqNo(dataIn.readLong());

        catalog.put(key, value);
        System.out.println("Key = " + key + ", Value = " + value);
      }
      System.out.println("Done loading catalog.");

      dataIn.close();
      fIn.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public void store() {
    try {
      File file = new File(catalogFilePath);
      FileOutputStream fOut = null;
      DataOutputStream dataOut = null;
  
      fOut = new FileOutputStream(file);
      dataOut = new DataOutputStream(fOut);
  
      System.out.println("Saving catalog ...");
      for (Map.Entry<Short, CatalogData> i : catalog.entrySet()) {
        short key = i.getKey();
        CatalogData value = i.getValue();

        dataOut.writeShort(key);
        dataOut.writeLong(value.getvBucketUuid());
        dataOut.writeLong(value.getLastSeqNo());

        System.out.println("Key = " + key + ", Value = " + value);
      }
      System.out.println("Done saving catalog.");

      dataOut.close();
      fOut.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public Map<Short, CatalogData> getCatalog() {
    return catalog;
  }

  public void setCatalog(Map<Short, CatalogData> catalog) {
    this.catalog = catalog;
  }
}
