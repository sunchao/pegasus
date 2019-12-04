package com.uber.pegasus;

public class PegasusWorker {
  public static void main(String[] args) throws Exception {
    if (args.length != 0 && args.length != 1) {
      System.err.println("Usage: PegasusWorker [port]");
      System.exit(1);
    }

    PegasusWorkerServer worker;
    worker = new PegasusWorkerServer();

    worker.start();
    worker.awaitTermination();
  }
}
