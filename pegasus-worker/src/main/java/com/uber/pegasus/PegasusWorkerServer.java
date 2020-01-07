package com.uber.pegasus;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PegasusWorkerServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PegasusWorkerServer.class);

  private final Server server;

  public PegasusWorkerServer() {
    this(new InetSocketAddress(0).getPort());
  }

  public PegasusWorkerServer(int port) {
    // TODO: many more configurations on this!
    this.server =
        NettyServerBuilder.forPort(port).addService(new PegasusWorkerService(port)).build();
  }

  public void start() throws IOException {
    server.start();
    LOG.info("Server started, listening on {}", server.getPort());
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
