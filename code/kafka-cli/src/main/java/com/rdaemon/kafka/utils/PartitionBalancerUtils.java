package com.rdaemon.kafka.utils;

import com.google.gson.Gson;
import com.rdaemon.kafka.models.OutputPartition;
import com.rdaemon.kafka.models.Partition;
import com.rdaemon.kafka.models.PartitionReassignmentPlan;
import com.rdaemon.kafka.models.ReplicaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import kafka.cluster.Broker;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionBalancerUtils {

  private static Logger logger = LoggerFactory.getLogger(PartitionBalancerUtils.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  public static void displayPartitionBalanceByTopic(Map<Integer, Map<ReplicaType, Set<String>>> nodeMap, Set<String> topics) {
    Map<String, Map<Integer, Map<ReplicaType, Integer>>> counter = new HashMap<>();
    for (Map.Entry<Integer, Map<ReplicaType, Set<String>>> entry : nodeMap.entrySet()) {
      Integer brokerId = entry.getKey();
      for (ReplicaType replicaType : ReplicaType.values()) {
        Set<String> partitions = entry.getValue().get(replicaType);

        for (String partition : partitions) {
          String[] split = partition.split(":");
          String topicName = split[0];

          if (!counter.containsKey(topicName)) {
            counter.put(topicName, new HashMap<>());
          }

          if (!counter.get(topicName).containsKey(brokerId)) {
            counter.get(topicName).put(brokerId, new HashMap<>());
          }

          if (!counter.get(topicName).get(brokerId).containsKey(replicaType)) {
            for (ReplicaType type : ReplicaType.values()) {
              counter.get(topicName).get(brokerId).put(type, 0);
            }
          }

          int currentValue = counter.get(topicName).get(brokerId).get(replicaType);
          counter.get(topicName).get(brokerId).put(replicaType, currentValue + 1);
        }
      }
    }

    for (String topicName : counter.keySet()) {
      for (Integer brokerId : counter.get(topicName).keySet()) {
        System.out.println("Host: " + brokerId + ", topic: " + topicName + ", Leader: " + counter.get(topicName).get(brokerId).get(ReplicaType.LEADER)
            + ", replicaCount: " + counter.get(topicName).get(brokerId).get(ReplicaType.REPLICA));
      }
      System.out.println("-------------------------------");
    }
  }

  public static void displayPartitionBalance(Map<Integer, Map<ReplicaType, Set<String>>> nodeMap) {
    for (Map.Entry<Integer, Map<ReplicaType, Set<String>>> entry : nodeMap.entrySet()) {
      Integer host = entry.getKey();
      Integer leaderCount = entry.getValue().get(ReplicaType.LEADER).size();
      Integer replicaCount = entry.getValue().get(ReplicaType.REPLICA).size();

      System.out.println("Host: " + host + ", Leader: " + leaderCount + ", replicaCount: " + replicaCount);
    }
  }

  public static void applyPartitionReassignmentPlan(Map<Integer, Map<ReplicaType, Set<String>>> nodeMap,
      Map<Integer, PartitionReassignmentPlan> partitionReassignmentPlanMap) {
    for (Integer brokerId : partitionReassignmentPlanMap.keySet()) {
      Map<ReplicaType, Set<String>> brokerNodeMap = nodeMap.get(brokerId);
      PartitionReassignmentPlan reassignmentPlan = partitionReassignmentPlanMap.get(brokerId);

      for (ReplicaType replicaType : reassignmentPlan.getPartitionRemoval().keySet()) {
        Set<String> partitionsToRemove = reassignmentPlan.getPartitionRemoval().get(replicaType);
        brokerNodeMap.get(replicaType).removeAll(partitionsToRemove);
      }

      for (ReplicaType replicaType : reassignmentPlan.getPartitionAddition().keySet()) {
        Set<String> partitionsToAdd = reassignmentPlan.getPartitionAddition().get(replicaType);
        brokerNodeMap.get(replicaType).addAll(partitionsToAdd);
      }

    }
  }

  public static void resolveUnderReplicatedAssignments(Map<Integer, Map<ReplicaType, Set<String>>> nodeMap,
      Map<Integer, PartitionReassignmentPlan> partitionReassignmentPlanMap, Map<String, Integer> topicReplicationFactor) {

    List<Integer> brokers = new ArrayList<>(nodeMap.keySet());
    Map<String, Set<Integer>> partitionReplicaCount = new HashMap<>();
    for (Integer broker : nodeMap.keySet()) {
      for (ReplicaType replicaType : nodeMap.get(broker).keySet()) {
        Set<String> partitions = nodeMap.get(broker).get(replicaType);
        for (String partition : partitions) {
          partitionReplicaCount.putIfAbsent(partition, new HashSet<>());
          partitionReplicaCount.get(partition).add(broker);
        }
      }
    }

    for (String partition : partitionReplicaCount.keySet()) {
      String[] splits = partition.split(":");
      String topic = splits[0];
      Integer replicationFactor = topicReplicationFactor.get(topic);
      Set<Integer> replicas = partitionReplicaCount.get(partition);

      while (true) {
        if (replicas.size() < replicationFactor) {
          Collections.shuffle(brokers);
          if (!replicas.contains(brokers.get(0))) {
            replicas.add(brokers.get(0));
            nodeMap.get(brokers.get(0)).get(ReplicaType.REPLICA).add(partition);
            partitionReassignmentPlanMap.get(brokers.get(0)).getPartitionAddition().get(ReplicaType.REPLICA).add(partition);
          }
        } else {
          break;
        }
      }
    }
  }

  // TODO: Fix DRY violation
  public static void printUnderReplicatedPartitionsPostAssignment(Map<Integer, Map<ReplicaType, Set<String>>> nodeMap,
      Map<String, Integer> topicReplicationFactor) {
    List<Integer> brokers = new ArrayList<>(nodeMap.keySet());
    Map<String, Set<Integer>> partitionReplicaCount = new HashMap<>();
    for (Integer broker : nodeMap.keySet()) {
      for (ReplicaType replicaType : nodeMap.get(broker).keySet()) {
        Set<String> partitions = nodeMap.get(broker).get(replicaType);
        for (String partition : partitions) {
          partitionReplicaCount.putIfAbsent(partition, new HashSet<>());
          partitionReplicaCount.get(partition).add(broker);
        }
      }
    }

    for (String partition : partitionReplicaCount.keySet()) {
      String[] splits = partition.split(":");
      String topic = splits[0];
      Integer replicationFactor = topicReplicationFactor.get(topic);
      Set<Integer> replicas = partitionReplicaCount.get(partition);

      if (replicas.size() < replicationFactor) {
        System.out.println("Under replicated: " + partition + ", Replication factor: " + replicationFactor + ", replicas: " + replicas);
      }

    }
  }

  //TODO: Replicas and Leader should not be on same broker
  public static Map<Integer, PartitionReassignmentPlan> generateReassignmentPlan(Map<String, Map<Integer, Partition>> topicsMap,
      Map<String, Integer> topicReplicationFactor, List<Broker> allBrokers) {
    Map<Integer, PartitionReassignmentPlan> partitionReassignmentPlans = new HashMap<>();

    for (String topicName : topicsMap.keySet()) {
      Map<Integer, Partition> partitionMap = topicsMap.get(topicName);
      Map<String, Map<Integer, Partition>> tempTopicsMap = new HashMap<>();
      tempTopicsMap.put(topicName, partitionMap);
      Map<Integer, Map<ReplicaType, Set<String>>> balanceNodeMap = KafkaParserUtils.fetchNodeMapFromMetaInformation(tempTopicsMap, allBrokers);

      Map<ReplicaType, Set<String>> freePool = new HashMap<>();
      for (ReplicaType replicaType : ReplicaType.values()) {
        freePool.put(replicaType, new HashSet<>());
      }

      Map<ReplicaType, Integer> minAssignmentPerBroker = new HashMap<>();
      minAssignmentPerBroker.put(ReplicaType.LEADER, partitionMap.size() / allBrokers.size());
      minAssignmentPerBroker.put(ReplicaType.REPLICA, (partitionMap.size() * (topicReplicationFactor.get(topicName) - 1)) / allBrokers.size());

      Map<ReplicaType, Integer> excessAssignmentPending = new HashMap<>();
      excessAssignmentPending.put(ReplicaType.LEADER, partitionMap.size() % allBrokers.size());
      excessAssignmentPending.put(ReplicaType.REPLICA, (partitionMap.size() * (topicReplicationFactor.get(topicName) - 1)) % allBrokers.size());

      for (Integer brokerId : balanceNodeMap.keySet()) {
        Integer brokerWeight = 1; //TODO: Make it configurable
        if (!partitionReassignmentPlans.containsKey(brokerId)) {
          partitionReassignmentPlans.put(brokerId, new PartitionReassignmentPlan());
          for (ReplicaType replicaType : ReplicaType.values()) {
            partitionReassignmentPlans.get(brokerId).getPartitionAddition().put(replicaType, new HashSet<>());
            partitionReassignmentPlans.get(brokerId).getPartitionRemoval().put(replicaType, new HashSet<>());
          }
        }

        for (ReplicaType replicaType : ReplicaType.values()) {
          Set<String> assignment = balanceNodeMap.get(brokerId).get(replicaType);
          while (assignment.size() > minAssignmentPerBroker.get(replicaType)) {
            if (assignment.size() == (minAssignmentPerBroker.get(replicaType) + 1) && excessAssignmentPending.get(replicaType) > 0) {
              excessAssignmentPending.put(replicaType, (excessAssignmentPending.get(replicaType) - 1));
              break;
            }
            String element = assignment.iterator().next();
            assignment.remove(element);
            freePool.get(replicaType).add(element);
            partitionReassignmentPlans.get(brokerId).getPartitionRemoval().get(replicaType).add(element);
          }
        }
      }

      for (ReplicaType replicaType : ReplicaType.values()) {
        Set<String> pool = freePool.get(replicaType);
        boolean imbalanceAssignment = false;

        while (pool.size() > 0) {
          for (Integer brokerId : balanceNodeMap.keySet()) {
            Set<String> assignment = balanceNodeMap.get(brokerId).get(replicaType);

            int attempts = (pool.size() * 2);
            while ((assignment.size() < (minAssignmentPerBroker.get(replicaType) + 1) || imbalanceAssignment) && pool.size() > 0) {
              if ((assignment.size() == (minAssignmentPerBroker.get(replicaType)) && excessAssignmentPending.get(replicaType) == 0)
                  && !imbalanceAssignment) {
                break;
              } else if (assignment.size() == (minAssignmentPerBroker.get(replicaType) + 1)) {
                excessAssignmentPending.put(replicaType, (excessAssignmentPending.get(replicaType) - 1));
              }

              //TODO: Highly inefficient. Think of better algorithm
              String element = new ArrayList<String>(pool).get(new Random().nextInt(pool.size()));
              boolean alreadyFoundInBroker = false;
              for (ReplicaType replicaType1 : ReplicaType.values()) {
                if (balanceNodeMap.get(brokerId).get(replicaType1).contains(element)) {
                  alreadyFoundInBroker = true;
                  attempts -= 1;
                  break;
                }
              }
              if (!alreadyFoundInBroker) {
                pool.remove(element);
                assignment.add(element);
                partitionReassignmentPlans.get(brokerId).getPartitionAddition().get(replicaType).add(element);
              }
              if (attempts < 0) {
                break;
              }
            }
          }
          imbalanceAssignment = true;
        }
      }
    }
    return partitionReassignmentPlans;
  }

  public static void displayReassignmentPlan(Map<Integer, Map<ReplicaType, Set<String>>> nodeMap,
      Map<Integer, PartitionReassignmentPlan> partitionReassignmentPlanMap, int maxOutputReassignment) {

    Set<String> allPartitions = new HashSet<>();
    Map<String, Deque<Integer>> aggregatedOutput = new HashMap<>();
    for (Integer broker : partitionReassignmentPlanMap.keySet()) {
      PartitionReassignmentPlan plan = partitionReassignmentPlanMap.get(broker);

      for (ReplicaType replicaType : plan.getPartitionAddition().keySet()) {
        allPartitions.addAll(plan.getPartitionAddition().get(replicaType));
      }
    }

    for (Integer broker : nodeMap.keySet()) {
      for (ReplicaType replicaType : nodeMap.get(broker).keySet()) {
        Set<String> partitions = nodeMap.get(broker).get(replicaType);

        for (String partition : partitions) {
          if (allPartitions.contains(partition)) {
            aggregatedOutput.putIfAbsent(partition, new LinkedList<>());

            if (replicaType.equals(ReplicaType.LEADER)) {
              aggregatedOutput.get(partition).addFirst(broker);
            } else {
              aggregatedOutput.get(partition).addLast(broker);
            }
          }
        }
      }
    }

    System.out.println("\nFinal Output\n");
    ArrayList<OutputPartition> outputPartitions = new ArrayList<>();
    int count = 0;
    for (String key : aggregatedOutput.keySet()) {
      count += 1;
      String[] splits = key.split(":");
      String topic = splits[0];
      String partition = splits[1];
      outputPartitions.add(new OutputPartition(topic, Integer.valueOf(partition), new ArrayList<>(aggregatedOutput.get(key))));
      if (count % maxOutputReassignment == 0) {
        printOutput(outputPartitions);
        outputPartitions.clear();
      }
    }

    if (outputPartitions.size() > 0) {
      printOutput(outputPartitions);
    }
  }

  private static void printOutput(ArrayList<OutputPartition> outputPartitions) {
    System.out.println(" ----------------------------------------------- ");
    Map<String, Object> finalOutput = new HashMap<>();
    finalOutput.put("version", 1);
    finalOutput.put("partitions", outputPartitions);
    try {
      System.out.println(mapper.writeValueAsString(finalOutput));
    } catch (IOException e) {
      logger.error("Failed to parse output with error: " + e.getMessage(), e);
    }
  }
}
