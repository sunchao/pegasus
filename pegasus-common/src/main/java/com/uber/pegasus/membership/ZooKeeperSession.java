package com.uber.pegasus.membership;

import static org.apache.zookeeper.ZooDefs.Ids;
import static org.apache.zookeeper.ZooDefs.Perms;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperSession implements Membership, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSession.class);

  private static final String ZK_CONNECTION_STRING_KEY = "pegasus.zookeeper.connectString";
  private static final String ZK_CONNECTION_TIMEOUT_MILLIS_KEY =
      "pegasus.zookeeper.connectTimeoutMillis";

  /** Root zookeeper directory */
  private static final String ZK_ZNODE_KEY = "pegasus.zookeeper.znode";

  private static final String ZK_ZNODE_DEFAULT = "/pegasus";

  /** Znode directory locations for master. */
  private static final String MASTER_MEMBERSHIP_ZNODE = "master";

  /** Znode directory locations for worker. */
  private static final String WORKER_MEMBERSHIP_ZNODE = "worker";

  /** Watcher of worker membership znode */
  private PathChildrenCache workerMembership;

  /** ZK connection string (host/port of quorum) */
  private final String zkConnectString;

  /** ZK connection timeout */
  private final int zkConnectTimeoutMs;

  /** ZK root node */
  private String rootNode;

  /** ACLs used to create new Znodes */
  private List<ACL> newNodeAcl =
      Collections.singletonList(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE));

  /** Default ACL provider for new directories */
  private final ACLProvider defaultAclProvider =
      new ACLProvider() {
        @Override
        public List<ACL> getDefaultAcl() {
          return newNodeAcl;
        }

        @Override
        public List<ACL> getAclForPath(String path) {
          return getDefaultAcl();
        }
      };

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

  public ZooKeeperSession(Configuration conf, String id, int masterPort, int workerPort)
      throws IOException {
    this.id = id;
    this.masterPort = masterPort;
    this.workerPort = workerPort;

    this.zkConnectString = conf.get(ZK_CONNECTION_STRING_KEY);
    if (zkConnectString == null || zkConnectString.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "ZooKeeper connection string has to be specified through " + ZK_CONNECTION_STRING_KEY);
    }
    this.zkConnectTimeoutMs =
        conf.getInt(
            ZK_CONNECTION_TIMEOUT_MILLIS_KEY,
            CuratorFrameworkFactory.builder().getConnectionTimeoutMs());
    LOG.info(
        "Connecting to ZooKeeper at "
            + zkConnectString
            + " with timeout "
            + zkConnectTimeoutMs
            + "ms, and id: "
            + id);

    this.rootNode = conf.get(ZK_ZNODE_KEY, ZK_ZNODE_DEFAULT);
    LOG.info("ZooKeeper root: " + rootNode);

    initMembershipPaths();
  }

  @Override
  public Collection<InetSocketAddress> workers() {
    Set<InetSocketAddress> result = new HashSet<>();
    for (String address : ImmutableSet.copyOf(workers)) {
      try {
        URI uri = new URI("dummy", address, null, null, null);
        int port = uri.getPort() == -1 ? 0 : uri.getPort();
        result.add(new InetSocketAddress(uri.getHost(), port));
      } catch (URISyntaxException e) {
        LOG.warn("Error when parsing {}", address, e);
      }
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    if (workerMembership != null) {
      workerMembership.close();
      synchronized (workers) {
        workers.clear();
      }
    }
    if (zkSession != null) {
      zkSession.close();
    }
  }

  public boolean runningMaster() {
    return masterPort > 0;
  }

  public boolean runningWorker() {
    return workerPort > 0;
  }

  /** Callback that is invoked when a session (re)starts. */
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
          zkSession =
              CuratorFrameworkFactory.builder()
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
          LOG.info("Started to monitor cluster membership");
          monitorMembership();
        }
        initMembership();

        synchronized (newSessionCbs) {
          for (NewSessionCb cb : newSessionCbs) {
            cb.newSession();
          }
        }
      }
    }

    return zkSession;
  }

  private void initMembershipPaths() throws IOException {
    ensurePath(getRelativePath(MASTER_MEMBERSHIP_ZNODE), newNodeAcl);
    ensurePath(getRelativePath(WORKER_MEMBERSHIP_ZNODE), newNodeAcl);
  }

  /**
   * Initialize to monitor cluster worker membership. This is ONLY called by master node.
   *
   * @throws IOException if failed to watch worker membership from ZK
   */
  private void monitorMembership() throws IOException {
    synchronized (zkSession) {
      if (workerMembership != null) {
        workerMembership.close();
      }
      workerMembership =
          new PathChildrenCache(zkSession, getRelativePath(WORKER_MEMBERSHIP_ZNODE), true);
      try {
        Preconditions.checkState(workers.isEmpty(), "worker set is not empty");
        workerMembership.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        // Initialize the worker set
        workers.clear();
        for (ChildData cd : workerMembership.getCurrentData()) {
          workers.add(getWorkerId(cd));
        }
      } catch (Exception e) {
        throw new IOException("Could not watch worker membership", e);
      }
    }

    workerMembership
        .getListenable()
        .addListener(
            (CuratorFramework zk, PathChildrenCacheEvent e) -> {
              LOG.debug("Received ZK event {}", e);
              synchronized (zkSession) {
                // This session is no longer the active one, ignore the update.
                if (zk != zkSession) {
                  return;
                }
                switch (e.getType()) {
                  case CHILD_ADDED:
                    workers.add(getWorkerId(e.getData()));
                    break;
                  case CHILD_REMOVED:
                    workers.remove(getWorkerId(e.getData()));
                    break;
                  case INITIALIZED:
                  default:
                    // TODO: should we handle other types of event such as CHILD_UPDATED?
                    workers.clear();
                    for (ChildData cd : workerMembership.getCurrentData()) {
                      workers.add(getWorkerId(cd));
                    }
                    break;
                }
              }
            });
  }

  private String getWorkerId(ChildData cd) {
    String path = cd.getPath();
    return path.substring(path.lastIndexOf('/') + 1);
  }

  /**
   * Create a leaf ephemeral path in ZK for the current node.
   *
   * @throws IOException if the creation failed
   */
  private void initMembership() throws IOException {
    CuratorFramework zk = getSession();
    String path =
        getRelativePath(
            runningMaster() ? MASTER_MEMBERSHIP_ZNODE : WORKER_MEMBERSHIP_ZNODE + "/" + id);
    zkDelete(path);
    try {
      zk.create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .withACL(newNodeAcl)
          .forPath(path);
    } catch (Exception e) {
      throw new IOException("Could not create znode at " + path, e);
    }
  }

  private String getRelativePath(String path) {
    return rootNode + '/' + path;
  }

  /**
   * Ensure that a persistent path already exist in ZK.
   *
   * @param path the path to ensure
   * @param acl ACLs used to create the path, in case it doesn't exist
   * @throws IOException when error occurs while creating the path
   */
  private void ensurePath(String path, List<ACL> acl) throws IOException {
    try {
      CuratorFramework zk = getSession();
      String node =
          zk.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .withACL(acl)
              .forPath(path);
      LOG.info("Path {} is successfully created", node);
    } catch (KeeperException.NodeExistsException e) {
      LOG.debug("Path already exists: {}", path);
    } catch (Exception e) {
      throw new IOException("Error creating path " + path, e);
    }
  }

  private void zkDelete(String path) throws IOException {
    CuratorFramework zk = getSession();
    try {
      zk.delete().forPath(path);
      LOG.info("Path {} has been successfully deleted", path);
    } catch (KeeperException.NoNodeException e) {
      // already deleted
    } catch (Exception e) {
      throw new IOException("Error deleting " + path, e);
    }
  }
}
