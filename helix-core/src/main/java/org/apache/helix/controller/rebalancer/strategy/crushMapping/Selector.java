package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import org.apache.helix.controller.rebalancer.topology.Node;
import java.util.List;

interface Selector {
  Node select(long input, long round);
}
