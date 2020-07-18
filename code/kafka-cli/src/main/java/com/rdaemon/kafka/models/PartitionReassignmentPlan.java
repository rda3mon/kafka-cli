package com.rdaemon.kafka.models;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartitionReassignmentPlan {
  Map<ReplicaType, Set<String>> partitionAddition = new HashMap<>();
  Map<ReplicaType, Set<String>> partitionRemoval = new HashMap<>();

  public Map<ReplicaType, Set<String>> getPartitionAddition() {
    return partitionAddition;
  }

  public Map<ReplicaType, Set<String>> getPartitionRemoval() {
    return partitionRemoval;
  }
}
