package com.uber.pegasus;

import com.uber.pegasus.catalog.MetastoreClient;
import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.PegasusMasterServiceGrpc;
import com.uber.pegasus.proto.internal.Internal;
import io.grpc.stub.StreamObserver;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

public class PegasusMasterService extends PegasusMasterServiceGrpc.PegasusMasterServiceImplBase {
  private static final Logger LOG = LogManager.getFormatterLogger(PegasusMasterService.class);
  private static final String VERSION_NO = "1";
  private static final String CONF_ROOT = "conf";

  private final MetastoreClient metastoreClient;
  private final FileSystem fs;

  private final Configuration hadoopConf;
  private final HiveConf hiveConf;

  public PegasusMasterService() {
    hadoopConf = new Configuration();
    // addResource(hadoopConf, "core-site.xml");
    // addResource(hadoopConf, "hdfs-site.xml");

    hiveConf = new HiveConf();
    // addResource(hiveConf, "hive-site.xml");

    metastoreClient = new MetastoreClient(hiveConf);
    try {
      fs = FileSystem.get(hadoopConf);
    } catch (IOException ioe) {
      LOG.fatal("Could not initialize file system", ioe);
      throw new RuntimeException(ioe);
    }
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

      for (Pegasus.Task task : tasks) {
        responseObserver.onNext(Pegasus.PlanResponse.newBuilder().addTasks(task).build());
      }
      responseObserver.onCompleted();
    } catch (TException e) {
      LOG.error("Error fetching table information", e);
    } catch (IOException e) {
      LOG.error("Error generating tasks", e);
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
    List<Partition> partitions =
        metastoreClient
            .getClient()
            .listPartitions(table.getDbName(), table.getTableName(), (short) -1);

    List<LocatedFileStatus> allFiles = new ArrayList<>();
    for (Partition part : partitions) {
      // TODO: check partition format
      Path partLoc = new Path(part.getSd().getLocation());
      try {
        // TODO: instead of listing and adding all files in a one step, we should make this
        //   concurrent and hand out tasks on the fly, utilizing the streaming API from gRpc.
        RemoteIterator<LocatedFileStatus> locatedFiles = fs.listLocatedStatus(partLoc);
        while (locatedFiles.hasNext()) {
          allFiles.add(locatedFiles.next());
        }
      } catch (FileNotFoundException e) {
        // TODO: should this be a warning instead? in Hive it is possible that a partition exist
        //   but the corresponding directory has already been removed.
        LOG.error("Directory not found: {}", partLoc, e);
        throw e;
      } catch (IOException e) {
        LOG.error("Error when listing directory: {}", partLoc, e);
        throw e;
      }
    }

    // TODO: lots of optimizations to do. For now, we just generate one task per file without
    //   locality.
    List<Pegasus.Task> result = new ArrayList<>();
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

      Pegasus.Task task = Pegasus.Task.newBuilder().setTask(taskInfo.toByteString()).build();
      result.add(task);
    }

    return result;
  }

  private void addResource(Configuration conf, String fileName) {
    String resource = String.join("/", CONF_ROOT, fileName);
    LOG.info("Loading configuration file {}", fileName);
    try {
      conf.addResource(new FileInputStream(resource));
    } catch (FileNotFoundException fnfe) {
      throw new RuntimeException("Cannot locate file " + fileName);
    }
  }
}
