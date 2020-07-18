package com.rdaemon.kafka.utils;

import com.rdaemon.kafka.models.Partition;
import com.rdaemon.kafka.models.PartitionBroker;
import com.rdaemon.kafka.models.ReplicaType;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;

import java.util.*;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

public class KafkaParserUtils {

  /*
   * return -- {"1": {"leader": ["test3:51", "test3:31"], "replicas": ["test1:94", "test1:91"] } }
   */
  public static Map<Integer, Map<ReplicaType, Set<String>>> fetchNodeMapFromMetaInformation(Map<String, Map<Integer, Partition>> topicsMap,
      List<Broker> allBrokers) {
    Map<Integer, Map<ReplicaType, Set<String>>> nodeMap = new HashMap<>();

    for (Broker broker : allBrokers) {
      if (!nodeMap.containsKey(broker.id())) {
        Map<ReplicaType, Set<String>> map = new HashMap<>();

        map.put(ReplicaType.LEADER, new HashSet());
        map.put(ReplicaType.REPLICA, new HashSet());
        nodeMap.put(broker.id(), map);
      }
    }

    for (String topicName : topicsMap.keySet()) {
      Map<Integer, Partition> partitionMap = topicsMap.get(topicName);
      for (Integer partitionNum : partitionMap.keySet()) {
        Partition partition = partitionMap.get(partitionNum);
        boolean head = true;
        for (PartitionBroker broker : partition.getReplicas()) {
          ReplicaType replicaType = head ? ReplicaType.LEADER : ReplicaType.REPLICA;
          nodeMap.get(broker.getBrokerId()).get(replicaType).add(topicName + ":" + partition.getPartitionId());
          head = false;
        }
      }
    }
    return nodeMap;
  }

  /*
   * return -- {"test": {"0": {"isr": [{"brokerId": 5, "host": "kafka5"}, {"brokerId": 3, "host": "kafka3"} ], "partitionId": 0, "replicas": [{"brokerId": 3, "host": "kafka3"}, {"brokerId": 5, "host": "kafka5"} ] } } }
   */
  public static Map<String, Map<Integer, Partition>> fetchTopicMapFromTopicMetadata(Set<TopicMetadata> topicMetadataSet) {
    Map<String, Map<Integer, Partition>> partitionMap = new HashMap<>();

    for (TopicMetadata metadata : topicMetadataSet) {
      List<PartitionMetadata> partitionMetadataList = scala.collection.JavaConversions.seqAsJavaList(metadata.partitionsMetadata());

      partitionMap.put(metadata.topic(), new HashMap<>());
      for (PartitionMetadata partitionMetadata : partitionMetadataList) {
        Broker leader = partitionMetadata.leader().get();
        List<Broker> replicas = scala.collection.JavaConversions.seqAsJavaList(partitionMetadata.replicas());
        List<Broker> isr = scala.collection.JavaConversions.seqAsJavaList(partitionMetadata.isr());

        List<PartitionBroker> tempReplicas = new ArrayList<>();
        ;
        for (Broker broker : replicas) {
          tempReplicas.add(new PartitionBroker.Builder().setBrokerId(broker.id()).setHost(broker.host()).build());
        }

        List<PartitionBroker> tempIsr = new ArrayList<>();
        ;
        for (Broker broker : isr) {
          tempIsr.add(new PartitionBroker.Builder().setBrokerId(broker.id()).setHost(broker.host()).build());
        }

        partitionMap.get(metadata.topic()).put(partitionMetadata.partitionId(),
            new Partition.Builder().setPartitionId(partitionMetadata.partitionId()).setReplicas(tempReplicas).setIsr(tempIsr).build());
      }
    }

    return partitionMap;
  }

  /*
   * return -- {"test2":2,"test3":2,"test":4,"test1":4}
   */
  public static Map<String, Integer> fetchTopicReplicationFactorMapFromTopicMap(Map<String, Map<Integer, Partition>> topicsMap) {
    Map<String, Integer> topicReplicationFactor = new HashMap<>();

    for (String topicName : topicsMap.keySet()) {
      Map<Integer, Partition> partitionMap = topicsMap.get(topicName);

      // Logic is to get max number of replicas for any partitions * number of partitions.
      // This logic is used as there is no way to fetch replication factor with current set of api's
      Optional<Partition> maxPartition = partitionMap.values().stream()
          .max((partition1, partition2) -> Integer.compare(partition1.getReplicas().size(), partition2.getReplicas().size()));

      if (maxPartition.isPresent()) {
        topicReplicationFactor.put(topicName, maxPartition.get().getReplicas().size());
      } else {
        topicReplicationFactor.put(topicName, 0);
      }
    }
    return topicReplicationFactor;
  }
}
