package com.uber.pegasus.membership;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.ACL;

public class ZooKeeperSession implements Closeable {
  private static final Logger LOG = LogManager.getLogger(ZooKeeperSession.class);

  private static final String ZK_CONNECTION_STRING_KEY =
      "pegasus.zookeeper.connectString";
  private static final String ZK_CONNECTION_TIMEOUT_MILLIS_KEY =
      "pegasus.zookeeper.connectTimeoutMillis";

  /** Root zookeeper directory */
  private static final String ZK_ZNODE_KEY = "pegasus.zookeeper.znode";
  private static final String ZK_ZNODE_DEFAULT = "/pegasus";

  /** Znode directory locations for master and worker. */
  private static final String MASTER_MEMBERSHIP_ZNODE = "master";
  private static final String WORKER_MEMBERSHIP_ZNODE = "worker";

  /** ZK connection string (host/port of quorum) */
  private final String zkConnectString;

  /** ZK connection timeout */
  private final int zkConnectTimeoutMs;

  /** ZK root node */
  private String rootNode;

  /** ACLs used to create new Znodes */
  private List<ACL> newNodeAcl;

  /** Default ACL provider for new directories */
  private final ACLProvider defaultAclProvider = new ACLProvider() {
    @Override
    public List<ACL> getDefaultAcl() {
      return newNodeAcl;
    }
    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  /** Master membership. Only maintained for master nodes (i.e., iff masterPort > 0). */
  private final Set<String> masters = new HashSet<>();

  /** Worker membership. Only maintained for master nodes (i.e., iff masterPort > 0). */
  private final Set<String> workers = new HashSet<>();

  /** Current ZK session */
  private volatile CuratorFramework zkSession;

  /** List of callbacks to be invoked when new session is (re)created. */
  private final List<NewSessionCb> newSessionCbs = new ArrayList<>();


  /** Identifier for the service */
  private final String id;

  /** Ports for master and worker. Set iff > 0. */
  private final int masterPort;
  private final int workerPort;


  public ZooKeeperSession(Configuration conf, String id, int masterPort, int workerPort) {
    this.id = id;
    this.masterPort = masterPort;
    this.workerPort = workerPort;

    this.zkConnectString = conf.get(ZK_CONNECTION_STRING_KEY);
    if (zkConnectString == null || zkConnectString.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "ZooKeeper connection string has to be specified through " + ZK_CONNECTION_STRING_KEY);
    }
    this.zkConnectTimeoutMs = conf.getInt(ZK_CONNECTION_TIMEOUT_MILLIS_KEY,
        CuratorFrameworkFactory.builder().getConnectionTimeoutMs());
    LOG.info("Connecting to ZooKeeper at " + zkConnectString + " with timeout " +
        zkConnectTimeoutMs + "ms, and id: " + id);

    this.rootNode = conf.get(ZK_ZNODE_KEY, ZK_ZNODE_DEFAULT);
    LOG.info("ZooKeeper root: " + rootNode);

    initMembershipPaths();
  }

  public boolean runningMaster() {
    return masterPort > 0;
  }

  public boolean runningWorker() {
    return workerPort > 0;
  }

  @Override
  public void close() throws IOException {}

  /**
   * Callback that is invoked when a session (re)starts.
   */
  public interface NewSessionCb {
    void newSession() throws IOException;
  }

  public void addNewSessionCb(NewSessionCb cb) {
    synchronized (newSessionCbs) {
      newSessionCbs.add(cb);
    }
  }

  private CuratorFramework getSession() throws IOException {
    if (zkSession == null || zkSession.getState() == CuratorFrameworkState.STOPPED) {
      boolean recreatedSession = false;
      synchronized (this) {
        if (zkSession == null || zkSession.getState() == CuratorFrameworkState.STOPPED) {
          zkSession = CuratorFrameworkFactory.builder()
              .connectString(zkConnectString)
              .connectionTimeoutMs(zkConnectTimeoutMs)
              .aclProvider(defaultAclProvider)
              .retryPolicy(new ExponentialBackoffRetry(1000, 3))
              .build();
          zkSession.start();
          recreatedSession = true;
        }
      }
      if (recreatedSession) {
        if (runningMaster()) {

        }
      }
    }

    return zkSession;
  }

  private void initMembership(boolean isMaster) throws IOException {

  }

  private void ensurePath(String path, List<ACL> acls) throws IOException {
      CuratorFramework zk = getSession();
  }

  private void initMembershipPaths() {
    throw new UnsupportedOperationException();
  }
}
