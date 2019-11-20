package com.uber.pegasus;

import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.PegasusMasterServiceGrpc;
import io.grpc.stub.StreamObserver;

public class PegasusMasterService extends PegasusMasterServiceGrpc.PegasusMasterServiceImplBase {
  private static final String VERSION_NO = "1";

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
    throw new UnsupportedOperationException("plan() is not yet implemented");
  }

  @Override
  public void getSchema(
      Pegasus.PlanRequest req, StreamObserver<Pegasus.GetSchemaResponse> responseObserver) {
    throw new UnsupportedOperationException("getSchema() is not yet implemented");
  }
}
