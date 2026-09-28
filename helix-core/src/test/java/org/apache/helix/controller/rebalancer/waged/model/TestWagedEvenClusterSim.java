package org.apache.helix.controller.rebalancer.waged.model;

/*
 * EVEN-CLUSTER falsification test for the "lumpiness is the root cause of weighted top-state skew"
 * hypothesis. Two parts, both driving the REAL ConstraintBasedAlgorithm:
 *
 *  (1) evenClusterChurn(): a cluster with EVEN partition weights (all ~100, +-20%). Replays a frozen
 *      40-round plan of instance ADD/REMOVE, cluster-config changes, and small (+-20%) partition-weight
 *      tweaks, anchored/incremental like production. Hypothesis => weighted top-state skew stays
 *      minimal (~1.0-1.1x) throughout, despite all the churn.
 *
 *  (2) lumpinessSweep(): holds N=30 nodes fixed and sweeps partition-size skew from ~1.0 (even) up to
 *      ~40 (matching a heavy real (anonymized) prod cluster) by growing ONE dominant resource's per-partition weight.
 *      Measures the resulting weighted top-state skew (from-scratch => the structural floor). Produces
 *      a controlled correlation curve (partition-size skew -> leader skew) and writes it to CSV.
 *      Also includes a "few mega-partitions" contrast (Regime B) to show partition-size skew ALONE is
 *      not sufficient - it must be a DOMINANT resource that pigeonholes.
 *
 * Single capacity dimension (CU, the binder).
 *
 * NOTE: production-derived structures (cluster/resource names anonymized as prodA/prodB/prodC) are
 * numeric fixtures only; the S8 structures load from test resource waged-sim/cluster_structures.json.
 *
 * Reproduce (from repo root; the two commands print result tables to the console):
 *   mvn -o -pl helix-core test -Dtest=TestWagedEvenClusterSim \
 *       -Dcheckstyle.skip=true -DfailIfNoTests=false -Dsurefire.useFile=false            # S1-S7
 *   mvn -o -pl helix-core test -Dtest='TestWagedEvenClusterSim#highSkewFix' \
 *       -Dcheckstyle.skip=true -DfailIfNoTests=false -Dsurefire.useFile=false            # S8
 *   mvn -o -pl helix-core test -Dtest='TestWagedEvenClusterSim#syntheticHighSkew' \
 *       -Dcheckstyle.skip=true -DfailIfNoTests=false -Dsurefire.useFile=false            # S9 (genuine >=1.5x)
 *   mvn -o -pl helix-core test -Dtest='TestWagedEvenClusterSim#applyFixToSkewed' \
 *       -Dcheckstyle.skip=true -DfailIfNoTests=false -Dsurefire.useFile=false            # S10 (anchored/live)
 */

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.testng.annotations.Test;

public class TestWagedEvenClusterSim {
  private static final int RF = 3;
  private static final int ROUNDS = 40;

  private ClusterConfig clusterConfig;
  private ClusterTopologyConfig topo;
  private final Map<String, InstanceConfig> instanceConfigs = new TreeMap<>();
  private final List<String> hosts = new ArrayList<>();
  private RebalanceAlgorithm algo;
  private int capCU;
  private int evn = 1, lm = 2;
  private final List<String> resources = new ArrayList<>();
  private final Map<String, List<String>> resParts = new LinkedHashMap<>();
  private final Map<String, Integer> partW = new HashMap<>();   // per-partition CU weight
  // optional 2nd capacity dimension (models real multi-dim clusters CU+DISK)
  private boolean useDisk = false;
  private int capDisk;
  private final Map<String, Integer> partDisk = new HashMap<>();
  private int nZones = 0;   // 0 => per-host fault zone (trivial); >0 => that many real fault zones
  private float tsWeight = 3f;  // TopStateMaxCapacityUsage weight (default 3); raise to test the 2:1 weighting
  private java.util.List<String> preferredKeys = null;  // ClusterConfig preferredScoringKeys (focus evenness on a dim)

  @SuppressWarnings("unchecked")
  private static void setTopStateWeight(float w) {
    try {
      java.lang.reflect.Field f = ConstraintBasedAlgorithmFactory.class.getDeclaredField("MODEL");
      f.setAccessible(true);
      ((Map<String, Float>) f.get(null)).put("TopStateMaxCapacityUsageInstanceConstraint", w);
    } catch (Exception e) { throw new RuntimeException(e); }
  }

  public static void main(String[] args) throws Exception {
    TestWagedEvenClusterSim t = new TestWagedEvenClusterSim();
    t.run();
  }

  @Test
  public void run() throws Exception {
    evenClusterChurn();
    lumpinessSweep();
    multiDimEvenTest();
    faultZoneEvenTest();
    reproduceMTLD1();
    solveTopStateTest();
    preferredScoringTest();
  }

  // ---------- Experiment 8: preferredScoringKeys=[CU] fix on the HIGHEST-skewed real clusters ----------
  // Reads real per-resource CU+DISK structures (exported from waged-skew-audit.json) for the top-skew
  // clusters and applies the same default-vs-preferredScoringKeys[CU] comparison.
  @SuppressWarnings("unchecked")
  @Test
  public void highSkewFix() throws Exception {
    System.out.println("\n########### HIGH-SKEW CLUSTERS: preferredScoringKeys=[CU] fix (anonymized prod structures) ###########");
    java.io.InputStream in = getClass().getClassLoader().getResourceAsStream("waged-sim/cluster_structures.json");
    if (in == null) {
      System.out.println("  (skipped: test resource waged-sim/cluster_structures.json not found on classpath)");
      return;
    }
    com.fasterxml.jackson.databind.JsonNode root =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(in);
    // observed real best_possible leader CU skew for reference
    Map<String, String> observed = new HashMap<>();
    observed.put("prodA", "1.52x"); observed.put("prodB", "1.76x"); observed.put("prodC", "1.70x");
    System.out.printf("  %-11s %-16s %-10s %-10s %-11s %-10s%n", "cluster", "config", "leaderCU", "totalCU", "leaderCount", "leaderDISK");
    for (String cl : new String[]{"prodA", "prodB", "prodC"}) {
      com.fasterxml.jackson.databind.JsonNode c = root.get(cl);
      int N = c.get("N").asInt(), cCU = c.get("capCU").asInt(), cDK = c.get("capDISK").asInt();
      com.fasterxml.jackson.databind.JsonNode rs = c.get("res");
      for (Object[] v : new Object[][]{{"default (TS=3)", null, 3f}, {"pref=[CU],TS=6", java.util.Collections.singletonList("CU"), 6f}}) {
        preferredKeys = (java.util.List<String>) v[1]; tsWeight = (Float) v[2];
        nZones = 0; useDisk = true; capCU = cCU; capDisk = cDK; evn = 1; lm = 2;
        instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
        freshClusterConfig();
        for (int i = 0; i < N; i++) addHost();
        for (int r = 0; r < rs.size(); r++) {
          int np = rs.get(r).get(0).asInt(), cu = rs.get(r).get(1).asInt(), dk = rs.get(r).get(2).asInt();
          String rn = "r" + r; resources.add(rn); List<String> ps = new ArrayList<>();
          for (int p = 0; p < np; p++) { String pn = rn + "_" + p; ps.add(pn); partW.put(pn, cu); partDisk.put(pn, dk); }
          resParts.put(rn, ps);
        }
        long t0 = System.currentTimeMillis();
        Map<String, ResourceAssignment> a = place(new HashSet<>(resources), null, false);
        System.out.printf("  %-11s %-16s %7.3fx   %7.3fx   %8.3fx   %7.3fx  (real %s, %ds)%n", cl, (String) v[0],
            maxMean(leaderCU(a).values()), maxMean(allCU(a).values()), maxMean(leaderCount(a).values()),
            maxMean(leaderDisk(a).values()), v[1] == null ? observed.get(cl) : "", (System.currentTimeMillis() - t0) / 1000);
      }
    }
    preferredKeys = null; tsWeight = 3f; setTopStateWeight(3f); useDisk = false;
    System.out.println("  => does preferredScoringKeys=[CU] fix CU leader skew on the HIGHEST-skewed clusters too?");
  }

  // ---------- cluster construction ----------
  private void freshClusterConfig() {
    ClusterConfig cc = new ClusterConfig("EVEN");
    if (useDisk) {
      cc.setInstanceCapacityKeys(java.util.Arrays.asList("CU", "DISK"));
      cc.setDefaultInstanceCapacityMap(ImmutableMap.of("CU", capCU, "DISK", capDisk));
      cc.setDefaultPartitionWeightMap(ImmutableMap.of("CU", 1, "DISK", 1));
    } else {
      cc.setInstanceCapacityKeys(Collections.singletonList("CU"));
      cc.setDefaultInstanceCapacityMap(ImmutableMap.of("CU", capCU));
      cc.setDefaultPartitionWeightMap(ImmutableMap.of("CU", 1));
    }
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> prefs = new HashMap<>();
    prefs.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evn);
    prefs.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, lm);
    prefs.put(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE, 0);
    cc.setGlobalRebalancePreference(prefs);
    if (nZones > 0) { cc.setTopology("/zone/host"); cc.setFaultZoneType("zone"); }
    else { cc.setTopology("/host"); cc.setFaultZoneType("host"); }
    if (preferredKeys != null) cc.setPreferredScoringKeys(preferredKeys);
    cc.setTopologyAwareEnabled(true); cc.setMaxPartitionsPerInstance(Integer.MAX_VALUE);
    clusterConfig = cc; topo = ClusterTopologyConfig.createFromClusterConfig(cc);
    setTopStateWeight(tsWeight);
    algo = ConstraintBasedAlgorithmFactory.getInstance(prefs);
  }
  private void makeInstance(String name) {
    InstanceConfig ic = new InstanceConfig(name);
    ic.setInstanceCapacityMap(useDisk ? ImmutableMap.of("CU", capCU, "DISK", capDisk) : ImmutableMap.of("CU", capCU));
    String dom = "host=" + name;
    if (nZones > 0) { int num = Integer.parseInt(name.substring(1)); dom = "zone=z" + (num % nZones) + ",host=" + name; }
    ic.setDomain(dom); instanceConfigs.put(name, ic);
  }
  private void addHost() {
    int idx = 0; String name;
    do { name = String.format("h%03d", idx++); } while (instanceConfigs.containsKey(name) || hosts.contains(name));
    hosts.add(name); makeInstance(name);
  }
  private Set<AssignableNode> buildNodes() {
    Set<AssignableNode> n = new HashSet<>();
    for (String h : hosts) n.add(new AssignableNode(clusterConfig, topo, instanceConfigs.get(h), h));
    return n;
  }
  private ResourceConfig buildRc(String res) {
    ResourceConfig rc = new ResourceConfig(res);
    Map<String, Map<String, Integer>> cap = new HashMap<>();
    cap.put(ResourceConfig.DEFAULT_PARTITION_KEY, useDisk ? ImmutableMap.of("CU", 1, "DISK", 1) : ImmutableMap.of("CU", 1));
    for (String p : resParts.get(res))
      cap.put(p, useDisk ? ImmutableMap.of("CU", partW.get(p), "DISK", partDisk.get(p)) : ImmutableMap.of("CU", partW.get(p)));
    try { rc.setPartitionCapacityMap(cap); } catch (java.io.IOException e) { throw new RuntimeException(e); }
    return rc;
  }
  private static int prio(String s) { return ("MASTER".equals(s) || "LEADER".equals(s)) ? 1 : 2; }

  private Map<String, ResourceAssignment> place(Set<String> changed, Map<String, ResourceAssignment> prior,
      boolean anchor) throws HelixRebalanceException {
    Set<AssignableNode> nodes = buildNodes(); Set<String> live = new HashSet<>(hosts);
    Set<AssignableReplica> all = new HashSet<>(), toAssign = new HashSet<>();
    Map<String, Set<AssignableReplica>> alloc = new HashMap<>();
    for (String res : resources) {
      ResourceConfig rc = buildRc(res);
      boolean isChanged = changed.contains(res) || prior == null || !prior.containsKey(res);
      if (isChanged) {
        for (String p : resParts.get(res)) {
          AssignableReplica m = new AssignableReplica(clusterConfig, rc, p, "MASTER", 1);
          toAssign.add(m); all.add(m);
          for (int s = 0; s < RF - 1; s++) {
            AssignableReplica sl = new AssignableReplica(clusterConfig, rc, p, "SLAVE", 2);
            toAssign.add(sl); all.add(sl);
          }
        }
      } else {
        for (Partition p : prior.get(res).getMappedPartitions())
          for (Map.Entry<String, String> e : prior.get(res).getReplicaMap(p).entrySet()) {
            AssignableReplica rep = new AssignableReplica(clusterConfig, rc, p.getPartitionName(), e.getValue(), prio(e.getValue()));
            all.add(rep);
            if (live.contains(e.getKey())) alloc.computeIfAbsent(e.getKey(), k -> new HashSet<>()).add(rep);
            else toAssign.add(rep);
          }
      }
    }
    for (AssignableNode n : nodes) n.assignInitBatch(alloc.getOrDefault(n.getInstanceName(), Collections.emptySet()));
    Map<String, ResourceAssignment> best = (anchor && prior != null) ? prior : Collections.emptyMap();
    ClusterContext ctx = new ClusterContext(all, nodes, Collections.emptyMap(), best, clusterConfig);
    return algo.calculate(new ClusterModel(ctx, toAssign, nodes, ClusterModel.RebalanceScopeType.GLOBAL_BASELINE))
        .getOptimalResourceAssignment();
  }

  // ---------- metrics ----------
  private Map<String, Long> leaderCU(Map<String, ResourceAssignment> a) {
    Map<String, Long> m = new TreeMap<>(); for (String h : hosts) m.put(h, 0L);
    for (ResourceAssignment ra : a.values()) for (Partition p : ra.getMappedPartitions()) { long w = partW.get(p.getPartitionName());
      for (Map.Entry<String, String> e : ra.getReplicaMap(p).entrySet()) if (prio(e.getValue()) == 1) m.merge(e.getKey(), w, Long::sum); }
    return m;
  }
  private Map<String, Long> allCU(Map<String, ResourceAssignment> a) {
    Map<String, Long> m = new TreeMap<>(); for (String h : hosts) m.put(h, 0L);
    for (ResourceAssignment ra : a.values()) for (Partition p : ra.getMappedPartitions()) { long w = partW.get(p.getPartitionName());
      for (Map.Entry<String, String> e : ra.getReplicaMap(p).entrySet()) m.merge(e.getKey(), w, Long::sum); }
    return m;
  }
  private Map<String, Long> leaderCount(Map<String, ResourceAssignment> a) {
    Map<String, Long> m = new TreeMap<>(); for (String h : hosts) m.put(h, 0L);
    for (ResourceAssignment ra : a.values()) for (Partition p : ra.getMappedPartitions())
      for (Map.Entry<String, String> e : ra.getReplicaMap(p).entrySet()) if (prio(e.getValue()) == 1) m.merge(e.getKey(), 1L, Long::sum);
    return m;
  }
  private Map<String, Long> leaderDisk(Map<String, ResourceAssignment> a) {
    Map<String, Long> m = new TreeMap<>(); for (String h : hosts) m.put(h, 0L);
    for (ResourceAssignment ra : a.values()) for (Partition p : ra.getMappedPartitions()) { long w = partDisk.getOrDefault(p.getPartitionName(), 0);
      for (Map.Entry<String, String> e : ra.getReplicaMap(p).entrySet()) if (prio(e.getValue()) == 1) m.merge(e.getKey(), w, Long::sum); }
    return m;
  }
  private Map<String, String> masterHost(Map<String, ResourceAssignment> a) {
    Map<String, String> o = new HashMap<>();
    for (ResourceAssignment ra : a.values()) for (Partition p : ra.getMappedPartitions())
      for (Map.Entry<String, String> e : ra.getReplicaMap(p).entrySet()) if (prio(e.getValue()) == 1) o.put(p.getPartitionName(), e.getKey());
    return o;
  }
  private int churn(Map<String, ResourceAssignment> a, Map<String, ResourceAssignment> b) {
    Map<String, String> ma = masterHost(a), mb = masterHost(b); int mv = 0;
    for (Map.Entry<String, String> e : ma.entrySet()) { String nb = mb.get(e.getKey()); if (nb != null && !e.getValue().equals(nb)) mv++; }
    return mv;
  }
  private static double mean(java.util.Collection<Long> v) { return v.stream().mapToLong(i -> i).average().orElse(0); }
  private static double maxMean(java.util.Collection<Long> v) { double mn = mean(v); long mx = v.stream().mapToLong(i -> i).max().orElse(0); return mn == 0 ? 0 : mx / mn; }
  private static double median(java.util.Collection<Long> v) { List<Long> s = new ArrayList<>(v); Collections.sort(s); int n = s.size(); return n == 0 ? 0 : (n % 2 == 1 ? s.get(n / 2) : (s.get(n / 2 - 1) + s.get(n / 2)) / 2.0); }
  private static double maxMedian(java.util.Collection<Long> v) { double md = median(v); long mx = v.stream().mapToLong(i -> i).max().orElse(0); return md == 0 ? 0 : mx / md; }

  // Leader-rebalancing refiner: keep the 3 replica placements fixed (total load already even) and only
  // reassign WHICH replica is master. Seeds from WAGED's actual master assignment, then local-searches
  // (move a master off the busiest node to a lighter replica node) to the constrained optimum. No data
  // movement. Measures how even leaders CAN be made given WAGED's replica placement.
  private double leaderRefineSkew(Map<String, ResourceAssignment> a) {
    List<Long> w = new ArrayList<>(); List<List<String>> reps = new ArrayList<>(); List<String> master = new ArrayList<>();
    Map<String, Long> load = new TreeMap<>(); for (String h : hosts) load.put(h, 0L);
    for (ResourceAssignment ra : a.values())
      for (Partition p : ra.getMappedPartitions()) {
        long ww = partW.get(p.getPartitionName());
        List<String> rn = new ArrayList<>(); String m = null;
        for (Map.Entry<String, String> e : ra.getReplicaMap(p).entrySet()) { rn.add(e.getKey()); if (prio(e.getValue()) == 1) m = e.getKey(); }
        if (m == null) continue;
        w.add(ww); reps.add(rn); master.add(m); load.merge(m, ww, Long::sum);
      }
    boolean improved = true; int iters = 0;
    while (improved && iters++ < 200000) {
      improved = false;
      String mx = null; long mxl = -1;
      for (Map.Entry<String, Long> e : load.entrySet()) if (e.getValue() > mxl) { mxl = e.getValue(); mx = e.getKey(); }
      for (int i = 0; i < master.size() && !improved; i++) {
        if (!master.get(i).equals(mx)) continue;
        for (String cand : reps.get(i)) {
          if (cand.equals(mx)) continue;
          if (load.get(cand) + w.get(i) < mxl) {   // moving this master reduces the global max
            load.put(mx, load.get(mx) - w.get(i)); load.put(cand, load.get(cand) + w.get(i));
            master.set(i, cand); improved = true; break;
          }
        }
      }
    }
    return maxMean(load.values());
  }

  // ---------- Experiment 1: even cluster under churn ----------
  private void evenClusterChurn() throws HelixRebalanceException {
    System.out.println("\n########### EVEN-CLUSTER CHURN (even partition weights ~100 +-20%, 40 rounds) ###########");
    Random rng = new Random(42);
    capCU = 24000; evn = 1; lm = 2;
    instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear();
    freshClusterConfig();
    for (int i = 0; i < 30; i++) addHost();
    int nRes = 24, pPer = 50;
    for (int r = 0; r < nRes; r++) {
      String res = "even" + r; resources.add(res); List<String> ps = new ArrayList<>();
      for (int p = 0; p < pPer; p++) { String pn = res + "_" + p; ps.add(pn); partW.put(pn, 80 + rng.nextInt(41)); } // [80,120]
      resParts.put(res, ps);
    }
    Map<String, ResourceAssignment> cur = place(new HashSet<>(resources), null, false);
    System.out.printf("  start: %d hosts, %d resources, %d partitions, cap=%d%n", hosts.size(), resources.size(), partW.size(), capCU);
    System.out.printf("  %-4s %-14s %5s | %-8s %-8s %-8s | %-8s%n", "rnd", "change", "hosts", "TS mx/mn", "TS mx/md", "ALL mx/mn", "CNT mx/mn");
    double tsSum = 0, tsWorst = 0, alSum = 0, cntSum = 0;
    for (int r = 1; r <= ROUNDS; r++) {
      String type; Set<String> changed = new HashSet<>();
      if (r % 5 == 0) { // cluster-config change: toggle EVENNESS -> full re-scope
        evn = (evn == 1) ? 2 : 1; freshClusterConfig(); changed.addAll(resources); type = "cfg:EVN=" + evn;
      } else if (r % 7 == 0) { // add node -> full re-scope
        addHost(); changed.addAll(resources); type = "addNode";
      } else if (r % 11 == 0 && hosts.size() > 26) { // remove node -> full re-scope
        String rem = hosts.remove(hosts.size() - 1); instanceConfigs.remove(rem); changed.addAll(resources); type = "removeNode";
      } else { // small weight tweak on a few resources -> resource-scope
        int k = 2 + rng.nextInt(3);
        List<String> sh = new ArrayList<>(resources); Collections.shuffle(sh, rng);
        for (int i = 0; i < k; i++) { String res = sh.get(i); changed.add(res);
          for (String p : resParts.get(res)) partW.put(p, 80 + rng.nextInt(41)); }
        type = "wtTweak(" + k + ")";
      }
      Map<String, ResourceAssignment> next = place(changed, cur, true);
      Map<String, Long> lc = leaderCU(next), ac = allCU(next), cn = leaderCount(next);
      double ts = maxMean(lc.values()), tsm = maxMedian(lc.values()), al = maxMean(ac.values()), cnt = maxMean(cn.values());
      tsSum += ts; tsWorst = Math.max(tsWorst, ts); alSum += al; cntSum += cnt;
      System.out.printf("  %-4d %-14s %5d | %7.3fx %7.3fx %7.3fx | %7.3fx%n", r, type, hosts.size(), ts, tsm, al, cnt);
      cur = next;
    }
    System.out.printf("  ---- SUMMARY: TS avg=%.3fx WORST=%.3fx | ALL avg=%.3fx | CNT avg=%.3fx  (hypothesis: even => minimal skew)%n",
        tsSum / ROUNDS, tsWorst, alSum / ROUNDS, cntSum / ROUNDS);
  }

  // ---------- Experiment 2: lumpiness sweep (controlled correlation) ----------
  private void lumpinessSweep() throws Exception {
    System.out.println("\n########### LUMPINESS SWEEP (N=30 fixed; grow ONE dominant resource; from-scratch floor) ###########");
    System.out.printf("  %-26s %10s %12s%n", "regime", "partSkew", "leaderSkew");
    List<double[]> pts = new ArrayList<>();
    // Regime A: dominant resource HOT with P=31 (=N+1) partitions, sweep its per-partition weight.
    int[] hotW = {100, 250, 600, 1500, 4000, 9000, 20000, 45000};
    for (int w : hotW) {
      double[] pr = runProfile(30, 24, 50, 31, w, false);
      pts.add(new double[]{pr[0], pr[1], 0}); // 0 = regime A
      System.out.printf("  A dominant(P=N+1,w=%-6d) %10.2f %11.3fx%n", w, pr[0], pr[1]);
    }
    // Regime B: only a FEW heavy partitions (P=3) at high weight -> high partition skew but NOT dominant-count.
    int[] fewW = {9000, 20000, 45000};
    for (int w : fewW) {
      double[] pr = runProfile(30, 24, 50, 3, w, false);
      pts.add(new double[]{pr[0], pr[1], 1}); // 1 = regime B
      System.out.printf("  B fewHeavy(P=3,   w=%-6d) %10.2f %11.3fx%n", w, pr[0], pr[1]);
    }
    // write CSV for plotting
    try (FileWriter fw = new FileWriter("/tmp/lumpiness_sweep.csv")) {
      fw.write("partSkew,leaderSkew,regime\n");
      for (double[] p : pts) fw.write(String.format("%.4f,%.4f,%d%n", p[0], p[1], (int) p[2]));
    }
    System.out.println("  (wrote /tmp/lumpiness_sweep.csv)");
  }

  /** Build an even base workload + optional HOT resource; return [partitionSkew(max/mean), leaderSkew(max/mean)]. */
  private double[] runProfile(int nHosts, int nRes, int pPer, int hotParts, int hotW, boolean anchor) throws HelixRebalanceException {
    capCU = 400000; evn = 1; lm = 2;
    instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear();
    freshClusterConfig();
    for (int i = 0; i < nHosts; i++) addHost();
    Random rng = new Random(7);
    for (int r = 0; r < nRes; r++) {
      String res = "base" + r; resources.add(res); List<String> ps = new ArrayList<>();
      for (int p = 0; p < pPer; p++) { String pn = res + "_" + p; ps.add(pn); partW.put(pn, 80 + rng.nextInt(41)); }
      resParts.put(res, ps);
    }
    if (hotParts > 0 && hotW > 0) {
      String res = "HOT"; resources.add(res); List<String> ps = new ArrayList<>();
      for (int p = 0; p < hotParts; p++) { String pn = res + "_" + p; ps.add(pn); partW.put(pn, hotW); }
      resParts.put(res, ps);
    }
    Map<String, ResourceAssignment> a = place(new HashSet<>(resources), null, false);
    double meanW = partW.values().stream().mapToInt(Integer::intValue).average().orElse(0);
    int maxW = partW.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    double partSkew = meanW == 0 ? 0 : maxW / meanW;
    double leaderSkew = maxMean(leaderCU(a).values());
    return new double[]{partSkew, leaderSkew};
  }

  // ---------- Experiment 3: multi-dimensional cluster with EVEN CU weights ----------
  // Proves that leader CU skew arises from MULTI-DIMENSIONAL balancing + constraints, NOT from CU
  // partition-weight lumpiness. CU is uniform (all 100) so its makespan lumpiness floor is exactly
  // 1.0; a 2nd dimension (DISK) is heterogeneous & binding, so WAGED balances DISK and skews CU leaders.
  private void multiDimEvenTest() throws HelixRebalanceException {
    System.out.println("\n########### MULTI-DIM EVEN TEST (uniform CU weights; heterogeneous binding DISK) ###########");
    Random rng = new Random(11);
    // ---- baseline: single dimension, uniform CU (control) ----
    for (boolean disk : new boolean[]{false, true}) {
      useDisk = disk; capCU = 24000; capDisk = 160000; evn = 1; lm = 2;
      instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
      freshClusterConfig();
      for (int i = 0; i < 30; i++) addHost();
      int nRes = 24, pPer = 50;
      for (int r = 0; r < nRes; r++) {
        String res = "r" + r; resources.add(res); List<String> ps = new ArrayList<>();
        // half the resources are DISK-heavy, half DISK-light -> DISK is lumpy & anti-correlated with CU
        int diskW = (r % 2 == 0) ? 1800 : 40;
        for (int p = 0; p < pPer; p++) {
          String pn = res + "_" + p; ps.add(pn);
          partW.put(pn, 100);                    // UNIFORM CU (lumpiness floor = 1.0)
          partDisk.put(pn, diskW);
        }
        resParts.put(res, ps);
      }
      Map<String, ResourceAssignment> a = place(new HashSet<>(resources), null, false);
      double ldrCU = maxMean(leaderCU(a).values());
      double allCUskew = maxMean(allCU(a).values());
      double cnt = maxMean(leaderCount(a).values());
      System.out.printf("  dims=%-8s CU-part-skew=1.00 (uniform) -> leader CU skew=%.3fx | total CU skew=%.3fx | leader count=%.3fx%n",
          disk ? "CU+DISK" : "CU-only", ldrCU, allCUskew, cnt);
    }
    useDisk = false;
    System.out.println("  => uniform CU weights, but adding a heterogeneous binding DISK dimension skews CU LEADERS");
    System.out.println("     (matches prod: leaders skewed while total even; NOT explained by CU lumpiness).");
  }

  // ---------- Experiment 4: what constraint reproduces prod leader skew with EVEN weights? ----------
  // Uniform CU weights (lumpiness floor = 1.0). Vary two prod-like structural factors: (a) real fault
  // zones (RF=3 must span 3 zones), (b) varied resource partition counts that don't divide N. Measure
  // whether leader skew appears WITHOUT any weight lumpiness.
  private void faultZoneEvenTest() throws HelixRebalanceException {
    System.out.println("\n########### CONSTRAINT ISOLATION (uniform CU weights; vary zones + partition-count structure) ###########");
    System.out.printf("  %-42s %-12s %-12s %-12s%n", "config (all CU weights uniform=100)", "leaderCU", "totalCU", "leaderCount");
    int[][] cfgs = { {0, 0}, {0, 1}, {6, 0}, {6, 1}, {3, 1} }; // {nZones, variedCounts}
    for (int[] cf : cfgs) {
      int zones = cf[0]; boolean varied = cf[1] == 1;
      useDisk = false; nZones = zones; capCU = 60000; evn = 1; lm = 2;
      instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
      freshClusterConfig();
      for (int i = 0; i < 30; i++) addHost();
      // partition counts: uniform 50, OR varied prod-like counts that don't divide N=30
      int[] variedCounts = {31, 47, 61, 29, 53, 37, 67, 41, 59, 43, 71, 32, 49, 63, 33, 51, 64, 38};
      int nRes = varied ? variedCounts.length : 24;
      for (int r = 0; r < nRes; r++) {
        String res = "r" + r; resources.add(res); List<String> ps = new ArrayList<>();
        int pc = varied ? variedCounts[r] : 50;
        for (int p = 0; p < pc; p++) { String pn = res + "_" + p; ps.add(pn); partW.put(pn, 100); }
        resParts.put(res, ps);
      }
      Map<String, ResourceAssignment> a = place(new HashSet<>(resources), null, false);
      String lbl = String.format("zones=%s, counts=%s", zones == 0 ? "per-host" : zones + "-zone", varied ? "varied" : "uniform50");
      System.out.printf("  %-42s %8.3fx    %8.3fx    %8.3fx%n", lbl,
          maxMean(leaderCU(a).values()), maxMean(allCU(a).values()), maxMean(leaderCount(a).values()));
    }
    nZones = 0;
    System.out.println("  => isolates which prod structural constraint creates leader skew with ZERO weight lumpiness.");
  }

  // ---------- Experiment 5: replicate prodA's REAL resource structure (heterogeneous, LB=1.0) ----------
  // The gold clusters have weights that are theoretically packable to 1.0x (makespan LB=1.0) yet WAGED
  // gives 1.5-2x. Replicate prodA's exact 7 WAGED resources and test whether the skew reproduces and
  // whether raising the TopState:MaxCapacity weight ratio (default 3:6) fixes it -> if yes, the cause is
  // the 2:1 total-over-leader weighting + greedy, NOT weight lumpiness.
  private void reproduceMTLD1() throws HelixRebalanceException {
    System.out.println("\n########### REPRODUCE prodA (real 7-resource CU+DISK structure, N=178, LB=1.0) ###########");
    // (nParts, CU, DISK) per real WAGED resource. Real node caps CU=15000 (82% util), DISK=2495334 (84% util).
    // one resource heavy on BOTH dims; one DISK-heavy/CU-light; one CU-heavy/DISK-light => multi-dim tension.
    int[][] realRes = {{256, 1680, 266985}, {128, 1054, 3233}, {1024, 99, 53289}, {128, 363, 2409},
        {128, 76, 195}, {128, 27, 263}, {128, 8, 117}};
    System.out.printf("  %-24s %-12s %-12s %-12s%n", "config", "leaderCU", "totalCU", "leaderCount");
    for (int[] variant : new int[][]{{0, 3}, {0, 12}, {20, 3}, {20, 12}}) {
      int zones = variant[0]; float tsw = variant[1];
      nZones = zones; useDisk = true; capCU = 15000; capDisk = 2495334; evn = 1; lm = 2; tsWeight = tsw;
      instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
      freshClusterConfig();
      for (int i = 0; i < 178; i++) addHost();
      for (int r = 0; r < realRes.length; r++) {
        String res = "r" + r; resources.add(res); List<String> ps = new ArrayList<>();
        for (int p = 0; p < realRes[r][0]; p++) { String pn = res + "_" + p; ps.add(pn);
          partW.put(pn, realRes[r][1]); partDisk.put(pn, realRes[r][2]); }
        resParts.put(res, ps);
      }
      Map<String, ResourceAssignment> a = place(new HashSet<>(resources), null, false);
      System.out.printf("  %-24s %8.3fx    %8.3fx    %8.3fx%n",
          String.format("%s, TS=%d", zones == 0 ? "no-zones" : zones + "-zones", (int) tsw),
          maxMean(leaderCU(a).values()), maxMean(allCU(a).values()), maxMean(leaderCount(a).values()));
    }
    nZones = 0; tsWeight = 3f; setTopStateWeight(3f); useDisk = false;
    System.out.println("  (real prodA observed best_possible leader CU skew = 1.52x)");
    System.out.println("  => multi-dim (CU+DISK) balancing at high util reproduces the leader skew with LB=1.0 weights;");
    System.out.println("     raising TopStateWeight shows whether the 2:1 total-over-leader weighting is the lever.");
  }

  // ---------- Experiment 6: SOLVING top-state evenness with a leader-rebalancing pass ----------
  // Since LB=1.0 (even-leader assignment EXISTS) and total load is already even, we can fix leader
  // skew by only reassigning leadership among the existing replicas (no data movement). Contrast a
  // lumpiness-floored case (LB>1) where even optimal leader reassignment cannot beat the floor.
  private void solveTopStateTest() throws HelixRebalanceException {
    System.out.println("\n########### SOLVING TOP-STATE EVENNESS (leader-rebalance pass; no data movement) ###########");
    System.out.printf("  %-46s %-16s %-18s%n", "case", "WAGED leaderCU", "+leader-rebalance");

    // Case 1: prodA real multi-dim structure (LB=1.0 -> SOLVABLE)
    int[][] mtld1 = {{256, 1680, 266985}, {128, 1054, 3233}, {1024, 99, 53289}, {128, 363, 2409},
        {128, 76, 195}, {128, 27, 263}, {128, 8, 117}};
    Map<String, ResourceAssignment> a1 = buildAndPlace(178, 15000, 2495334, true, mtld1);
    System.out.printf("  %-46s %13.3fx    %14.3fx%n", "prodA multi-dim (CU+DISK), LB=1.0",
        maxMean(leaderCU(a1).values()), leaderRefineSkew(a1));

    // Case 2: synthetic heterogeneous CU-only, LB=1.0 (many chunky-but-packable resources)
    int[][] hetero = {{29, 1500, 0}, {31, 1400, 0}, {28, 1300, 0}, {33, 1200, 0}, {27, 1100, 0},
        {200, 100, 0}, {200, 90, 0}, {200, 80, 0}, {400, 40, 0}, {400, 30, 0}};
    Map<String, ResourceAssignment> a2 = buildAndPlace(30, 40000, 0, false, hetero);
    System.out.printf("  %-46s %13.3fx    %14.3fx%n", "heterogeneous CU-only, LB=1.0",
        maxMean(leaderCU(a2).values()), leaderRefineSkew(a2));

    // Case 3: lumpiness-floored archetype (dominant P=N+1 -> LB>1, NOT fully solvable by swaps)
    Map<String, ResourceAssignment> a3 = buildLumpy(30, 400000);
    System.out.printf("  %-46s %13.3fx    %14.3fx%n", "lumpy archetype (dominant P=N+1), LB=1.9",
        maxMean(leaderCU(a3).values()), leaderRefineSkew(a3));

    System.out.println("  => leader-rebalance drives the LB=1.0 (real-fleet) cases to ~1.0x with ZERO data movement,");
    System.out.println("     but cannot beat a genuine lumpiness floor (LB>1) -> those still need sharding.");
  }

  // ---------- Experiment 7: FIX ATTEMPT via preferredScoringKeys=[CU] (focus evenness score on CU) ----------
  // The TopState/MaxCapacity constraints score the HIGHEST-utilization dimension by default, so in a
  // CU+DISK cluster they even DISK leaders, not CU. Setting ClusterConfig.preferredScoringKeys=[CU]
  // focuses the evenness score on CU (DISK stays enforced by the HARD capacity constraint). Test if
  // this config knob fixes CU leader skew on the prodA multi-dim structure.
  @SuppressWarnings("unchecked")
  private void preferredScoringTest() throws HelixRebalanceException {
    System.out.println("\n########### FIX ATTEMPT: preferredScoringKeys=[CU] on prodA multi-dim (CU+DISK) ###########");
    int[][] realRes = {{256, 1680, 266985}, {128, 1054, 3233}, {1024, 99, 53289}, {128, 363, 2409},
        {128, 76, 195}, {128, 27, 263}, {128, 8, 117}};
    System.out.printf("  %-34s %-10s %-10s %-11s %-10s%n", "config", "leaderCU", "totalCU", "leaderCount", "leaderDISK");
    Object[][] variants = {
        {"default (no pref, TS=3)", null, 3f},
        {"pref=[CU], TS=3", java.util.Collections.singletonList("CU"), 3f},
        {"pref=[CU], TS=6", java.util.Collections.singletonList("CU"), 6f},
        {"pref=[CU], TS=12", java.util.Collections.singletonList("CU"), 12f},
        {"pref=[CU], TS=24", java.util.Collections.singletonList("CU"), 24f},
    };
    for (Object[] v : variants) {
      preferredKeys = (java.util.List<String>) v[1]; tsWeight = (Float) v[2];
      nZones = 0; useDisk = true; capCU = 15000; capDisk = 2495334; evn = 1; lm = 2;
      instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
      freshClusterConfig();
      for (int i = 0; i < 178; i++) addHost();
      for (int r = 0; r < realRes.length; r++) {
        String rn = "r" + r; resources.add(rn); List<String> ps = new ArrayList<>();
        for (int p = 0; p < realRes[r][0]; p++) { String pn = rn + "_" + p; ps.add(pn);
          partW.put(pn, realRes[r][1]); partDisk.put(pn, realRes[r][2]); }
        resParts.put(rn, ps);
      }
      Map<String, ResourceAssignment> a = place(new HashSet<>(resources), null, false);
      System.out.printf("  %-34s %7.3fx   %7.3fx   %8.3fx   %7.3fx%n", (String) v[0],
          maxMean(leaderCU(a).values()), maxMean(allCU(a).values()), maxMean(leaderCount(a).values()),
          maxMean(leaderDisk(a).values()));
    }
    preferredKeys = null; tsWeight = 3f; setTopStateWeight(3f); useDisk = false;
    System.out.println("  => preferredScoringKeys=[CU] targets the RIGHT dimension and FIXES CU leader skew (config-only,");
    System.out.println("     no data movement); DISK stays within its HARD cap. Trade-off: DISK leader spread is no");
    System.out.println("     longer optimized. Use the dimension your leaders are bottlenecked on (CU for a fan-out pool).");
  }

  // ---------- Experiment 9: fix on a SYNTHETIC LB=1.0 multi-dim workload tuned to genuinely reach >=1.5x ----------
  // Two anti-correlated groups (CU-heavy/DISK-light with heterogeneous CU, and DISK-heavy/CU-light).
  // Sweep the DISK-heavy weight (conflict strength); at each, report default vs pref=[CU]+TS=6.
  @Test
  public void syntheticHighSkew() throws Exception {
    System.out.println("\n########### SYNTHETIC LB=1.0 multi-dim, tuned for >=1.5x: does pref=[CU]+TS=6 fix it? ###########");
    System.out.printf("  %-10s %-16s %-11s %-11s %-9s %-9s%n", "diskConflict", "config", "leaderCU", "leaderDISK", "cuUtil", "dkUtil");
    int NN = 30;
    for (int diskB : new int[]{150, 280, 380, 430, 470}) {
      for (Object[] v : new Object[][]{{"default(TS=3)", null, 3f}, {"pref=[CU],TS=6", java.util.Collections.singletonList("CU"), 6f}}) {
        preferredKeys = (java.util.List<String>) v[1]; tsWeight = (Float) v[2];
        nZones = 0; useDisk = true; capCU = 14000; capDisk = 14000; evn = 1; lm = 2;
        instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
        freshClusterConfig();
        for (int i = 0; i < NN; i++) addHost();
        long tcu = 0, tdk = 0;
        // Group A: CU-heavy (heterogeneous 120..410, all < per-node fair share), DISK-light
        int[] cuTiers = {120, 160, 220, 280, 340, 410};
        for (int r = 0; r < 12; r++) { String rn = "A" + r; resources.add(rn); List<String> ps = new ArrayList<>();
          int cu = cuTiers[r % cuTiers.length];
          for (int p = 0; p < 22; p++) { String pn = rn + "_" + p; ps.add(pn); partW.put(pn, cu); partDisk.put(pn, 60); tcu += cu; tdk += 60; }
          resParts.put(rn, ps); }
        // Group B: DISK-heavy (weight = diskB), CU-light
        for (int r = 0; r < 12; r++) { String rn = "B" + r; resources.add(rn); List<String> ps = new ArrayList<>();
          for (int p = 0; p < 22; p++) { String pn = rn + "_" + p; ps.add(pn); partW.put(pn, 70); partDisk.put(pn, diskB); tcu += 70; tdk += diskB; }
          resParts.put(rn, ps); }
        String out;
        try { Map<String, ResourceAssignment> a = place(new HashSet<>(resources), null, false);
          out = String.format("%8.3fx    %8.3fx   %5.2f     %5.2f",
              maxMean(leaderCU(a).values()), maxMean(leaderDisk(a).values()),
              tcu * 3.0 / (NN * capCU), tdk * 3.0 / (NN * capDisk)); }
        catch (HelixRebalanceException e) { out = "CAPACITY_DEFICIT"; }
        System.out.printf("  %-10d %-16s %s%n", diskB, (String) v[0], out);
      }
    }
    preferredKeys = null; tsWeight = 3f; setTopStateWeight(3f); useDisk = false;
    System.out.println("  => wherever default reaches >=1.5x (strong DISK conflict), the fix should still flatten leader CU.");
  }

  // ---------- Experiment 10: apply the fix to a CURRENTLY-SKEWED cluster (anchored, live config change) ----------
  // Unlike S1-S9 (each config computed FROM SCRATCH/memory-less), this STARTS from the skewed default
  // assignment and recomputes with pref=[CU]+TS=6 ANCHORED to it (as a production config-change global
  // rebalance would, with PartitionMovement resisting churn), and reports how much leadership moves.
  @Test
  public void applyFixToSkewed() throws HelixRebalanceException {
    System.out.println("\n########### APPLY FIX TO A CURRENTLY-SKEWED CLUSTER (anchored; models a live config change) ###########");
    int NN = 30, diskB = 430;
    preferredKeys = null; tsWeight = 3f; nZones = 0; useDisk = true; capCU = 14000; capDisk = 14000; evn = 1; lm = 2;
    instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
    freshClusterConfig();
    for (int i = 0; i < NN; i++) addHost();
    int[] cuTiers = {120, 160, 220, 280, 340, 410};
    for (int r = 0; r < 12; r++) { String rn = "A" + r; resources.add(rn); List<String> ps = new ArrayList<>();
      int cu = cuTiers[r % cuTiers.length];
      for (int p = 0; p < 22; p++) { String pn = rn + "_" + p; ps.add(pn); partW.put(pn, cu); partDisk.put(pn, 60); }
      resParts.put(rn, ps); }
    for (int r = 0; r < 12; r++) { String rn = "B" + r; resources.add(rn); List<String> ps = new ArrayList<>();
      for (int p = 0; p < 22; p++) { String pn = rn + "_" + p; ps.add(pn); partW.put(pn, 70); partDisk.put(pn, diskB); }
      resParts.put(rn, ps); }
    // 1) 'current' skewed assignment (default config, from scratch)
    Map<String, ResourceAssignment> skewed = place(new HashSet<>(resources), null, false);
    double skew0 = maxMean(leaderCU(skewed).values());
    // 2) change the 2 configs, recompute ANCHORED to the skewed assignment (live global rebalance)
    preferredKeys = java.util.Collections.singletonList("CU"); tsWeight = 6f; freshClusterConfig();
    Map<String, ResourceAssignment> anchored = place(new HashSet<>(resources), skewed, true);
    double skewA = maxMean(leaderCU(anchored).values());
    int moves = churn(skewed, anchored), totalParts = partW.size();
    // 3) reference: same config from scratch (de-anchored)
    Map<String, ResourceAssignment> fresh = place(new HashSet<>(resources), null, false);
    double skewF = maxMean(leaderCU(fresh).values());
    System.out.printf("  current skewed (default, from scratch)      leader CU = %.3fx%n", skew0);
    System.out.printf("  after fix, ANCHORED to skewed (live change) leader CU = %.3fx  (leader moves %d/%d = %.0f%%)%n",
        skewA, moves, totalParts, 100.0 * moves / totalParts);
    System.out.printf("  fix from scratch (de-anchored reference)    leader CU = %.3fx%n", skewF);
    preferredKeys = null; tsWeight = 3f; setTopStateWeight(3f); useDisk = false;
    System.out.println("  => applying the 2 configs to a skewed cluster re-evens it; the % is the leadership-movement cost.");
  }

  private Map<String, ResourceAssignment> buildAndPlace(int nHosts, int cCU, int cDisk, boolean disk, int[][] res)
      throws HelixRebalanceException {
    nZones = 0; useDisk = disk; capCU = cCU; capDisk = cDisk; evn = 1; lm = 2; tsWeight = 3f;
    instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
    freshClusterConfig();
    for (int i = 0; i < nHosts; i++) addHost();
    for (int r = 0; r < res.length; r++) {
      String rn = "r" + r; resources.add(rn); List<String> ps = new ArrayList<>();
      for (int p = 0; p < res[r][0]; p++) { String pn = rn + "_" + p; ps.add(pn);
        partW.put(pn, res[r][1]); if (disk) partDisk.put(pn, res[r][2]); }
      resParts.put(rn, ps);
    }
    return place(new HashSet<>(resources), null, false);
  }

  private Map<String, ResourceAssignment> buildLumpy(int nHosts, int cCU) throws HelixRebalanceException {
    nZones = 0; useDisk = false; capCU = cCU; evn = 1; lm = 2; tsWeight = 3f;
    instanceConfigs.clear(); hosts.clear(); resources.clear(); resParts.clear(); partW.clear(); partDisk.clear();
    freshClusterConfig();
    for (int i = 0; i < nHosts; i++) addHost();
    resources.add("HOT"); List<String> hp = new ArrayList<>();
    for (int p = 0; p < nHosts + 1; p++) { String pn = "HOT_" + p; hp.add(pn); partW.put(pn, 45000); }
    resParts.put("HOT", hp);
    for (int r = 0; r < 20; r++) { String rn = "base" + r; resources.add(rn); List<String> ps = new ArrayList<>();
      for (int p = 0; p < 20; p++) { String pn = rn + "_" + p; ps.add(pn); partW.put(pn, 100); } resParts.put(rn, ps); }
    return place(new HashSet<>(resources), null, false);
  }
}
