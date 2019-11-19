package com.uber.pegasus;

import java.net.InetSocketAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PegasusMaster {
  private static final Logger LOG = LogManager.getFormatterLogger(PegasusMaster.class);

  public static void main(String[] args) throws Exception {
    PegasusMasterServer master = new PegasusMasterServer(new InetSocketAddress(0));
    master.start();
    master.awaitTermination();
  }
}
