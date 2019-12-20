package com.uber.pegasus;

import java.net.InetSocketAddress;

public class PegasusWorker {
  public static void main(String[] args) throws Exception {
    if (args.length != 0 && args.length != 1) {
      System.err.println("Usage: PegasusWorker [port]");
      System.exit(1);
    }

    PegasusWorkerServer worker;
    if (args.length == 0) {
      worker = new PegasusWorkerServer(new InetSocketAddress(0));
    } else {
      worker = new PegasusWorkerServer(Integer.parseInt(args[0]));
    }
    worker.start();
    worker.awaitTermination();
  }
}
