package com.uber.pegasus;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PegasusWorkerServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PegasusWorkerServer.class);

  private final Server server;

  public PegasusWorkerServer() {
    this(getFreeSocketPort());
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

  /** Get a random socket port that is currently available */
  private static int getFreeSocketPort() {
    int port = 0;
    try {
      ServerSocket s = new ServerSocket(0);
      port = s.getLocalPort();
      s.close();
      return port;
    } catch (IOException e) {
      // Could not get a free port. Return default port 0.
    }
    return port;
  }
}
