package com.uber.pegasus;

import com.uber.pegasus.proto.Pegasus;
import com.uber.pegasus.proto.PegasusWorkerGrpc;
import io.grpc.stub.StreamObserver;

public class PegasusWorkerService extends PegasusWorkerGrpc.PegasusWorkerImplBase {

  @Override
  public void execTask(Pegasus.ExecTaskRequest req,
      StreamObserver<Pegasus.ExecTaskResponse> responseObserver) {
    throw new UnsupportedOperationException("execTask() is not yet implemented");
  }

  @Override
  public void fetch(Pegasus.FetchRequest req,
      StreamObserver<Pegasus.FetchResponse> responseObserver) {
    throw new UnsupportedOperationException("fetch() is not yet implemented");
  }
}
