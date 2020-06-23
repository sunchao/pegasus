package com.uber.pegasus.example;

import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.Pegasus.StructuredRequest;
import com.uber.pegasus.proto.PegasusMasterServiceGrpc;
import com.uber.pegasus.proto.PegasusWorkerGrpc;
import com.uber.pegasus.proto.internal.Internal;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

public class TableCat {
  private final ManagedChannel channel;
  private final PegasusMasterServiceGrpc.PegasusMasterServiceBlockingStub blockingStub;
  private final PegasusMasterServiceGrpc.PegasusMasterServiceStub asyncStub;

  private static final int FETCH_INTERVAL_SEC = 3;

  public TableCat(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
  }

  public TableCat(ManagedChannelBuilder<?> channelBuilder) {
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

        boolean done = false;
        while (!done) {
          Pegasus.FetchRequest fetchReq =
              Pegasus.FetchRequest.newBuilder().setHandle(execResponse.getHandle()).build();
          Iterator<Pegasus.FetchResponse> results = workerStub.fetch(fetchReq);

          BufferAllocator allocator = new RootAllocator();
          while (results.hasNext()) {
            Pegasus.FetchResponse current = results.next();

            ArrowReader reader =
                new ArrowStreamReader(
                    new ByteArrayInputStream(current.getBody().toByteArray()), allocator);
            print(reader, current.getNumRecords());

            if (current.getDone()) {
              done = true;
              break;
            }
          }
          TimeUnit.SECONDS.sleep(FETCH_INTERVAL_SEC);
        }
      }
    } catch (Exception e) {
      Status status = Status.fromThrowable(e);
      status.asException().printStackTrace();
    }
  }

  private void print(ArrowReader reader, int numRow) throws IOException {
    VectorSchemaRoot recordBatch = reader.getVectorSchemaRoot();
    // Arrow flatbuffer doesn't set rowCount, which seems like a bug
    recordBatch.setRowCount(numRow);
    Schema schema = recordBatch.getSchema();

    // First print out a header line for column schemas
    String header =
        schema
            .getFields()
            .stream()
            .map(f -> Types.getMinorTypeForArrowType(f.getType()).name())
            .reduce((s1, s2) -> s1 + "\t" + s2)
            .get();
    System.out.println(header);

    while (reader.loadNextBatch()) {
      // TODO: we should initialize these before the fetch command
      List<FieldVector> fieldData = recordBatch.getFieldVectors();
      int size = fieldData.size();
      BitVector[] bitVectors = new BitVector[size];
      IntVector[] intVectors = new IntVector[size];
      BigIntVector[] bigIntVectors = new BigIntVector[size];
      Float4Vector[] float4Vectors = new Float4Vector[size];
      Float8Vector[] float8Vectors = new Float8Vector[size];
      VarBinaryVector[] varBinaryVectors = new VarBinaryVector[size];

      for (int i = 0; i < fieldData.size(); i++) {
        FieldVector fv = fieldData.get(i);
        switch (fv.getMinorType()) {
          case BIT:
            bitVectors[i] = (BitVector) fv;
            break;
          case INT:
            intVectors[i] = (IntVector) fv;
            break;
          case BIGINT:
            bigIntVectors[i] = (BigIntVector) fv;
            break;
          case FLOAT4:
            float4Vectors[i] = (Float4Vector) fv;
            break;
          case FLOAT8:
            float8Vectors[i] = (Float8Vector) fv;
            break;
          case VARBINARY:
            varBinaryVectors[i] = (VarBinaryVector) fv;
            break;
        }
      }

      StringBuilder line = new StringBuilder();
      for (int i = 0; i < recordBatch.getRowCount(); i++) {
        if (i > 0) line.append("\n");
        for (int j = 0; j < size; j++) {
          if (j > 0) line.append("\t");
          switch (fieldData.get(j).getMinorType()) {
            case BIT:
              line.append(bitVectors[j].get(i) == 0 ? "false" : "true");
              break;
            case INT:
              line.append(intVectors[j].get(i));
              break;
            case BIGINT:
              line.append(bigIntVectors[j].get(i));
              break;
            case FLOAT4:
              line.append(float4Vectors[j].get(i));
              break;
            case FLOAT8:
              line.append(float8Vectors[j].get(i));
              break;
            case VARBINARY:
              line.append(new String(varBinaryVectors[j].get(i), StandardCharsets.UTF_8));
              break;
          }
        }

        System.out.println(line);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: PegasusCat <host> <port> <db> <tbl>");
      System.exit(1);
    }

    TableCat client = new TableCat(args[0], Integer.parseInt(args[1]));
    client.plan(args[2], args[3], new ArrayList<>());
    client.shutdown();
  }
}
