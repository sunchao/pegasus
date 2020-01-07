package com.uber.pegasus;

import com.uber.pegasus.membership.ZooKeeperSession;
import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.PegasusWorkerGrpc;
import io.grpc.stub.StreamObserver;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PegasusWorkerService extends PegasusWorkerGrpc.PegasusWorkerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(PegasusWorkerService.class);

  private static final String CONF_ROOT = "conf";

  private final Configuration conf;
  private final ZooKeeperSession zkSession;

  public PegasusWorkerService(int port) {
    conf = new Configuration();
    addResource(conf, "core-site.xml");
    addResource(conf, "hdfs-site.xml");

    try {
      InetAddress host = InetAddress.getLocalHost();
      zkSession = new ZooKeeperSession(conf, host.getHostName() + ":" + port, -1, 15000);
    } catch (IOException e) {
      LOG.error("Could not initialize ZK session", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execTask(
      Pegasus.ExecTaskRequest req, StreamObserver<Pegasus.ExecTaskResponse> responseObserver) {
    throw new UnsupportedOperationException("execTask() is not yet implemented");
  }

  @Override
  public void fetch(
      Pegasus.FetchRequest req, StreamObserver<Pegasus.FetchResponse> responseObserver) {
    throw new UnsupportedOperationException("fetch() is not yet implemented");
  }

  private void addResource(Configuration conf, String fileName) {
    String confDir = System.getenv("HADOOP_CONF_DIR");
    if (confDir == null) {
      confDir = CONF_ROOT;
    }
    String resource = String.join("/", confDir, fileName);
    LOG.info("Loading configuration file {}", fileName);
    try {
      conf.addResource(new FileInputStream(resource));
    } catch (FileNotFoundException fnfe) {
      throw new RuntimeException("Cannot locate file " + fileName);
    }
  }
}
