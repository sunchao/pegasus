package com.uber.pegasus;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class PegasusWorkerServer implements AutoCloseable {
  private static final Logger LOG =
      LogManager.getFormatterLogger(PegasusWorkerServer.class);

  private final int port;
  private final Server server;

  public PegasusWorkerServer(int port) {
    this.port = port;

    // TODO: many more configurations on this!
    this.server = NettyServerBuilder.forPort(port)
        .addService(new PegasusWorkerService())
        .build();
  }

  public void start() throws IOException {
    server.start();
    LOG.info("Server started, listening on {}", port);
  }

  @Override
  public void close() {
    if (server != null) {
      server.shutdown();
    }
  }
}
