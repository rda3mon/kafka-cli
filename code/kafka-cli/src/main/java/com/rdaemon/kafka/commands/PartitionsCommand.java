package com.rdaemon.kafka.commands;

import com.rdaemon.kafka.models.PartitionReassignmentPlan;
import com.rdaemon.kafka.models.ReplicaType;
import com.rdaemon.kafka.utils.PartitionBalancerUtils;
import com.rdaemon.kafka.utils.ZookeeperAPIUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.rdaemon.kafka.models.Partition;
import com.rdaemon.kafka.utils.KafkaParserUtils;
import joptsimple.internal.Strings;
import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

public class PartitionsCommand implements Command {
  private ObjectMapper objectMapper = new ObjectMapper();

  private ZkClient zkClient;

  private Logger logger = LoggerFactory.getLogger(PartitionsCommand.class);

  public PartitionsCommand() {
  }

  public PartitionsCommand(ZkClient zkClient) {
    this.zkClient = zkClient;
  }

  @Override public void list(List<String> inputTopics) {
    Set<String> topics = new HashSet<>();
    if (inputTopics.size() == 0) {
      topics = new HashSet<>(scala.collection.JavaConversions.seqAsJavaList(ZkUtils.getAllTopics(zkClient)));
    } else {
      topics.addAll(inputTopics);
    }

    Map<String, Map<Integer, Partition>> topicsMap = ZookeeperAPIUtils.fetchTopicInformationFromZookeeper(zkClient, topics);
    ;
    List<Broker> allBrokers = ZookeeperAPIUtils.fetchBrokersFromZookeeper(zkClient);
    Map<Integer, Map<ReplicaType, Set<String>>> nodeMap = KafkaParserUtils.fetchNodeMapFromMetaInformation(topicsMap, allBrokers);

    //TODO: To be deprecated
    Set<TopicMetadata> topicMetadataSet =
        scala.collection.JavaConversions.setAsJavaSet(AdminUtils.fetchTopicMetadataFromZk(JavaConversions.asScalaSet(topics), zkClient));
    Map<String, Map<Integer, Partition>> partitionMap = KafkaParserUtils.fetchTopicMapFromTopicMetadata(topicMetadataSet);

    for (String topic : partitionMap.keySet()) {
      Map<Integer, Partition> partitions = partitionMap.get(topic);

      System.out.println("Topic: " + topic);
      for (Integer partitionId : partitions.keySet()) {
        Partition partition = partitions.get(partitionId);

        System.out.println("Partition: " + partitionId + ", Replicas: " + partition.getReplicas() + ", ISR: " + partition.getIsr());
      }
      System.out.println("----------------------------");
    }

  }

  public void ViewBalancer(List<String> inputTopics) {
    Set<String> topics = new HashSet<>();
    if (inputTopics.size() <= 0) {
      topics = new HashSet<>(scala.collection.JavaConversions.seqAsJavaList(ZkUtils.getAllTopics(zkClient)));
    } else {
      topics.addAll(inputTopics);
    }

    Set<TopicMetadata> topicMetadataSet =
        scala.collection.JavaConversions.setAsJavaSet(AdminUtils.fetchTopicMetadataFromZk(JavaConversions.asScalaSet(topics), zkClient));

    Map<String, Map<Integer, Partition>> topicsMap = ZookeeperAPIUtils.fetchTopicInformationFromZookeeper(zkClient, topics);
    ;
    List<Broker> allBrokers = ZookeeperAPIUtils.fetchBrokersFromZookeeper(zkClient);
    Map<Integer, Map<ReplicaType, Set<String>>> nodeMap = KafkaParserUtils.fetchNodeMapFromMetaInformation(topicsMap, allBrokers);

    //        PartitionBalancerUtils.displayPartitionBalance(nodeMap);
    PartitionBalancerUtils.displayPartitionBalanceByTopic(nodeMap, topics);
  }

  public String generateBalancer(List<String> topic, int maxOutputReassignment) {
    Set<String> topics = new HashSet<>();

    if (topic.isEmpty()) {
      topics = ZookeeperAPIUtils.fetchTopicsFromZookeeper(zkClient);
    }
    List<Broker> allBrokers = ZookeeperAPIUtils.fetchBrokersFromZookeeper(zkClient);
    Map<String, Map<Integer, Partition>> topicsMap = ZookeeperAPIUtils.fetchTopicInformationFromZookeeper(zkClient, topics);

    Map<String, Integer> topicReplicationFactor = KafkaParserUtils.fetchTopicReplicationFactorMapFromTopicMap(topicsMap);
    Map<Integer, Map<ReplicaType, Set<String>>> nodeMap = KafkaParserUtils.fetchNodeMapFromMetaInformation(topicsMap, allBrokers);

    Map<Integer, PartitionReassignmentPlan> partitionReassignmentPlan =
        PartitionBalancerUtils.generateReassignmentPlan(topicsMap, topicReplicationFactor, allBrokers);

    PartitionBalancerUtils.applyPartitionReassignmentPlan(nodeMap, partitionReassignmentPlan);

    PartitionBalancerUtils.resolveUnderReplicatedAssignments(nodeMap, partitionReassignmentPlan, topicReplicationFactor);

    PartitionBalancerUtils.printUnderReplicatedPartitionsPostAssignment(nodeMap, topicReplicationFactor);

    PartitionBalancerUtils.displayPartitionBalanceByTopic(nodeMap, topics);
    PartitionBalancerUtils.displayPartitionBalance(nodeMap);

    PartitionBalancerUtils.displayReassignmentPlan(nodeMap, partitionReassignmentPlan, maxOutputReassignment);

    return null;
  }
}
