package com.uber.pegasus;

import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.PegasusMasterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class PegasusMasterClient {
  private static final Logger LOG =
      LogManager.getFormatterLogger(PegasusMasterClient.class);

  private final ManagedChannel channel;
  private final PegasusMasterServiceGrpc.PegasusMasterServiceBlockingStub blockingStub;
  private final PegasusMasterServiceGrpc.PegasusMasterServiceStub asyncStub;

  public PegasusMasterClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
  }

  public PegasusMasterClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = PegasusMasterServiceGrpc.newBlockingStub(channel);
    asyncStub = PegasusMasterServiceGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  public void printVersion() {
    Pegasus.GetVersionResponse resp =
        blockingStub.getVersion(Pegasus.Empty.newBuilder().build());
    LOG.info("Version = " + resp.getVersion());
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: PegasusMasterClient <host> <port>");
      System.exit(1);
    }

    PegasusMasterClient client =
        new PegasusMasterClient(args[0], Integer.parseInt(args[1]));
    client.printVersion();
    client.shutdown();
    LOG.info("Done");
  }
}
