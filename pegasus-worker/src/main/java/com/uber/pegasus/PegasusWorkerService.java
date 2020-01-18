package com.uber.pegasus;

import com.google.protobuf.InvalidProtocolBufferException;
import com.uber.pegasus.membership.ZooKeeperSession;
import com.uber.pegasus.parquet.ParquetReader;
import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.PegasusWorkerGrpc;
import com.uber.pegasus.proto.internal.Internal;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PegasusWorkerService extends PegasusWorkerGrpc.PegasusWorkerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(PegasusWorkerService.class);
  private static final String CONF_ROOT = "conf";
  private static final int NUM_SCANNER_THREADS = 5;

  private final Configuration conf;
  private final ConcurrentHashMap<String, RecordBatches> readyBatches;
  private final ExecutorService executorService;

  public PegasusWorkerService(int port) {
    conf = new Configuration();
    readyBatches = new ConcurrentHashMap<>();
    executorService = Executors.newFixedThreadPool(NUM_SCANNER_THREADS);
    addResource(conf, "core-site.xml");
    addResource(conf, "hdfs-site.xml");

    try {
      InetAddress host = InetAddress.getLocalHost();
      new ZooKeeperSession(conf, host.getHostName() + ":" + port, -1, 15000);
    } catch (IOException e) {
      LOG.error("Could not initialize ZK session", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execTask(
      Pegasus.ExecTaskRequest req, StreamObserver<Pegasus.ExecTaskResponse> responseObserver) {
    try {
      UUID uuid = UUID.randomUUID();
      Internal.TaskInfo taskInfo = Internal.TaskInfo.parseFrom(req.getTask());
      Internal.ScanRange scanRange = taskInfo.getScanRanges(0);
      Internal.HdfsSplit split = scanRange.getHdfsSplit();
      executorService.execute(
          new ScannerThread(uuid, split.getFileName(), (int) split.getLength()));

      Pegasus.ExecTaskResponse response = Pegasus.ExecTaskResponse.newBuilder()
          .setHandle(uuid.toString()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (InvalidProtocolBufferException e) {
      responseObserver.onError(
          Status.INTERNAL.withDescription("Invalid task bytes").asRuntimeException());
    }
  }

  @Override
  public void fetch(
      Pegasus.FetchRequest req, StreamObserver<Pegasus.FetchResponse> responseObserver) {
    try {
      String uuid = req.getHandle();
      if (readyBatches.contains(uuid)) {
        Pegasus.FetchResponse response = Pegasus.FetchResponse.newBuilder()
      }
    }
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

  private class ScannerThread implements Runnable {
    private final UUID uuid;
    private final String fileName;
    private final int length;

    ScannerThread(UUID uuid, String fileName, int length) {
      this.uuid = uuid;
      this.fileName = fileName;
      this.length = length;
    }

    @Override
    public void run() {
      try {
        FileSystem fs = FileSystem.get(conf);
        ParquetReader parquetReader = new ParquetReader(fs, new Path(fileName), length);

        RecordBatches batches = new RecordBatches();
        while (parquetReader.hasNext()) {
          List<ValueVector> batch = parquetReader.readNext();
          batches.values.add(batch);
        }

        readyBatches.put(uuid.toString(), batches);

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class RecordBatches {
    List<List<ValueVector>> values;
  }
}
