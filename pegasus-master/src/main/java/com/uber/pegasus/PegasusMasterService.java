package com.uber.pegasus;

import com.uber.pegasus.catalog.MetastoreClient;
import com.uber.pegasus.membership.ZooKeeperSession;
import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.PegasusMasterServiceGrpc;
import com.uber.pegasus.proto.internal.Internal;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PegasusMasterService extends PegasusMasterServiceGrpc.PegasusMasterServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(PegasusMasterService.class);
  private static final String VERSION_NO = "1";
  private static final String CONF_ROOT = "conf";

  private final MetastoreClient metastoreClient;
  private final FileSystem fs;
  private final ZooKeeperSession zkSession;

  private final Configuration hadoopConf;
  private final HiveConf hiveConf;

  public PegasusMasterService() {
    hadoopConf = new Configuration();
    addResource(hadoopConf, "hdfs-site.xml");
    addResource(hadoopConf, "core-site.xml");

    hiveConf = new HiveConf();
    metastoreClient = new MetastoreClient(hiveConf);
    try {
      fs = FileSystem.get(hadoopConf);
    } catch (IOException e) {
      LOG.error("Could not initialize file system", e);
      throw new RuntimeException(e);
    }
    try {
      InetAddress host = InetAddress.getLocalHost();
      zkSession = new ZooKeeperSession(hadoopConf, host.getHostName(), 14000, -1);
    } catch (IOException e) {
      LOG.error("Could not initialize ZK session", e);
      throw new RuntimeException(e);
    }

    LOG.info("Started master service");
  }

  @Override
  public void getVersion(
      Pegasus.Empty request, StreamObserver<Pegasus.GetVersionResponse> responseObserver) {
    Pegasus.GetVersionResponse response =
        Pegasus.GetVersionResponse.newBuilder().setVersion(VERSION_NO).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void plan(Pegasus.PlanRequest req, StreamObserver<Pegasus.PlanResponse> responseObserver) {
    try {
      Table tableInfo = getTableInfo(req);
      List<Pegasus.Task> tasks = generateTasks(tableInfo);

      Pegasus.PlanResponse response = Pegasus.PlanResponse.newBuilder().addAllTasks(tasks).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (TException e) {
      LOG.error("Error fetching table information", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error fetch table information: " + e.getMessage())
              .asRuntimeException());
    } catch (IOException e) {
      LOG.error("Error generating tasks", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error generating tasks: " + e.getMessage())
              .asRuntimeException());
    }
  }

  @Override
  public void getSchema(
      Pegasus.PlanRequest req, StreamObserver<Pegasus.GetSchemaResponse> responseObserver) {
    throw new UnsupportedOperationException("getSchema() is not yet implemented");
  }

  private Table getTableInfo(Pegasus.PlanRequest req) throws TException {
    Pegasus.RequestType reqType = req.getReqType();
    switch (reqType) {
      case STRUCTURE:
        Pegasus.StructuredRequest structuredReq = req.getStructuredReq();
        return metastoreClient
            .getClient()
            .getTable(structuredReq.getDatabaseName(), structuredReq.getTableName());
      default:
        throw new UnsupportedOperationException("Request type: " + reqType + " not supported");
    }
  }

  /**
   * Given a table to be queried, generate a list of scan tasks to be dispatched to worker nodes.
   */
  private List<Pegasus.Task> generateTasks(Table table) throws TException, IOException {
    List<Path> listLocs = new ArrayList<>();

    if (table.getPartitionKeys().isEmpty()) {
      // table is not partitioned - list the table location and get files
      listLocs.add(new Path(table.getSd().getLocation()));
    } else {
      List<Partition> partitions =
          metastoreClient
              .getClient()
              .listPartitions(table.getDbName(), table.getTableName(), (short) -1);
      for (Partition part : partitions) {
        listLocs.add(new Path(part.getSd().getLocation()));
      }
    }

    List<LocatedFileStatus> allFiles = new ArrayList<>();
    for (Path loc : listLocs) {
      // TODO: check partition format
      try {
        // TODO: instead of listing and adding all files in a one step, we should make this
        //   concurrent and hand out tasks on the fly, utilizing the streaming API from gRpc.
        RemoteIterator<LocatedFileStatus> locatedFiles = fs.listLocatedStatus(loc);
        while (locatedFiles.hasNext()) {
          allFiles.add(locatedFiles.next());
        }
      } catch (FileNotFoundException e) {
        // TODO: should this be a warning instead? in Hive it is possible that a partition exist
        //   but the corresponding directory has already been removed.
        LOG.error("Directory not found: {}", loc, e);
        throw e;
      } catch (IOException e) {
        LOG.error("Error when listing directory: {}", loc, e);
        throw e;
      }
    }

    // TODO: lots of optimizations to do. For now, we just generate one task per file without
    //   locality.
    List<Pegasus.Task> result = new ArrayList<>();
    LOG.info("There are {} files to process", allFiles.size());
    for (LocatedFileStatus f : allFiles) {
      Internal.HdfsSplit split =
          Internal.HdfsSplit.newBuilder()
              .setFileName(f.getPath().toUri().toString())
              .setOffset(0)
              .setLength(f.getLen())
              .setFileLength(f.getLen())
              .build();

      Internal.ScanRange scanRange = Internal.ScanRange.newBuilder().setHdfsSplit(split).build();
      Internal.TaskInfo taskInfo = Internal.TaskInfo.newBuilder().addScanRanges(scanRange).build();
      Pegasus.Task task =
          Pegasus.Task.newBuilder()
              .setTask(taskInfo.toByteString())
              .addAllLocalHosts(
                  zkSession
                      .workers()
                      .stream()
                      .map(
                          addr ->
                              Internal.NetworkAddress.newBuilder()
                                  .setHostname(addr.getHostName())
                                  .setPort(addr.getPort())
                                  .build())
                      .collect(Collectors.toList()))
              .build();

      result.add(task);
    }

    LOG.info("There are {} tasks generated", result.size());
    return result;
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
