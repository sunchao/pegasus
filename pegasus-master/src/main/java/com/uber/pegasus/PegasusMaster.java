package com.uber.pegasus;

import java.net.InetSocketAddress;

public class PegasusMaster {
  public static void main(String[] args) throws Exception {
    if (args.length != 0 && args.length != 1) {
      System.err.println("Usage: PegasusMaster [port]");
      System.exit(1);
    }

    PegasusMasterServer master;
    if (args.length == 0) {
      master = new PegasusMasterServer(new InetSocketAddress(0));
    } else {
      master = new PegasusMasterServer(Integer.parseInt(args[0]));
    }
    master.start();
    master.awaitTermination();
  }
}
