package com.uber.pegasus;

import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.Pegasus.StructuredRequest;
import com.uber.pegasus.proto.PegasusMasterServiceGrpc;
import com.uber.pegasus.proto.PegasusWorkerGrpc;
import com.uber.pegasus.proto.internal.Internal;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PegasusMasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(PegasusMasterClient.class);

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

  public void plan(String databaseName, String tableName, List<String> columns) {
    List<StructuredRequest.ColumnHandle> columnHandles = new ArrayList<>();
    for (String column : columns) {
      columnHandles.add(StructuredRequest.ColumnHandle.newBuilder().setName(column).build());
    }
    StructuredRequest request =
        StructuredRequest.newBuilder()
            .setDatabaseName(databaseName)
            .setTableName(tableName)
            .addAllColumns(columnHandles)
            .build();
    Pegasus.PlanRequest planRequest =
        Pegasus.PlanRequest.newBuilder()
            .setReqType(Pegasus.RequestType.STRUCTURE)
            .setStructuredReq(request)
            .build();
    try {
      Pegasus.PlanResponse planResponse = blockingStub.plan(planRequest);
      for (Pegasus.Task task : planResponse.getTasksList()) {
        List<Internal.NetworkAddress> addresses = task.getLocalHostsList();
        if (addresses.isEmpty()) {
          throw new RuntimeException("No address found for task {}" + task);
        }
        Internal.NetworkAddress addr = task.getLocalHostsList().get(0);

        // connect to worker service and exec the task
        PegasusWorkerGrpc.PegasusWorkerBlockingStub workerStub =
            PegasusWorkerGrpc.newBlockingStub(
                ManagedChannelBuilder.forAddress(addr.getHostname(), addr.getPort())
                    .usePlaintext()
                    .build());
        Pegasus.ExecTaskResponse execResponse =
            workerStub.execTask(
                Pegasus.ExecTaskRequest.newBuilder()
                    .setTask(task.getTask())
                    .setFetchSize(1024)
                    .setLimit(-1)
                    .build());

        // debugging: print out schema

      }
    } catch (Exception e) {
      Status status = Status.fromThrowable(e);
      status.asException().printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: PegasusMasterClient <host> <port>");
      System.exit(1);
    }

    PegasusMasterClient client = new PegasusMasterClient(args[0], Integer.parseInt(args[1]));
    client.plan("foo", "bar", new ArrayList<>());
    client.shutdown();
  }
}
