package com.uber.pegasus.membership;

import java.net.InetSocketAddress;
import java.util.Collection;

public interface Membership {
  /** Return all the current available worker addresses */
  Collection<InetSocketAddress> workers();
}
