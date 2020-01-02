package com.uber.pegasus;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PegasusWorkerServer implements AutoCloseable {
  private static final Logger LOG = LogManager.getFormatterLogger(PegasusWorkerServer.class);

  private final Server server;

  public PegasusWorkerServer(int port) {
    // TODO: many more configurations on this!
    this.server = NettyServerBuilder.forPort(port).addService(new PegasusWorkerService()).build();
  }

  public PegasusWorkerServer(InetSocketAddress ss) {
    this.server = NettyServerBuilder.forAddress(ss).addService(new PegasusWorkerService()).build();
  }

  public void start() throws IOException {
    server.start();
    LOG.info("Server started, listening on " + server.getPort());
  }

  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  @Override
  public void close() {
    if (server != null) {
      server.shutdown();
    }
  }
}
