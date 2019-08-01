package com.uber.pegasus.catalog;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetastoreClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreClient.class);

  private final IMetaStoreClient hiveClient;

  public MetastoreClient(HiveConf hiveConf) {
    try {
      hiveClient = RetryingMetaStoreClient.getProxy(hiveConf,
          (tbl) -> null, HiveMetaStoreClient.class.getName());
    } catch (MetaException e) {
      LOG.warn("Error creating HMS client", e);
      throw new IllegalStateException(e);
    }
  }

  public IMetaStoreClient getClient() {
    return hiveClient;
  }

  @Override
  public void close() {
    if (hiveClient != null) {
      hiveClient.close();
    }
  }
}
