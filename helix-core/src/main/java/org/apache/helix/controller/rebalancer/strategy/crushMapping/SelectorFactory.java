package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import org.apache.helix.controller.rebalancer.topology.Node;


class SelectorFactory {

  static Selector createSelector(Node node, boolean useStraw2) {
    if (useStraw2) {
      return new Straw2Selector(node);
    }
    return new StrawSelector(node);
  }
}
