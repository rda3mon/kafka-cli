package com.rdaemon.kafka.utils;

import com.rdaemon.kafka.models.Partition;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;

public class ZookeeperAPIUtils {

  public static Set<String> fetchTopicsFromZookeeper(ZkClient zkClient) {
    return new HashSet<>(scala.collection.JavaConversions.seqAsJavaList(ZkUtils.getAllTopics(zkClient)));
  }

  /*
   * return -- {"test": {"0": {"isr": [{"brokerId": 5, "host": "kafka5"}, {"brokerId": 3, "host": "kafka3"} ],
   * "partitionId": 0, "replicas": [{"brokerId": 3, "host": "kafka3"}, {"brokerId": 5, "host": "kafka5"} ] } } }
   */
  public static Map<String, Map<Integer, Partition>> fetchTopicInformationFromZookeeper(ZkClient zkClient, Set<String> topics) {
    Set<TopicMetadata> topicMetadataSet =
        scala.collection.JavaConversions.setAsJavaSet(AdminUtils.fetchTopicMetadataFromZk(JavaConversions.asScalaSet(topics), zkClient));
    return KafkaParserUtils.fetchTopicMapFromTopicMetadata(topicMetadataSet);
  }

  public static List<Broker> fetchBrokersFromZookeeper(ZkClient zkClient) {
    return scala.collection.JavaConversions.seqAsJavaList(ZkUtils.getAllBrokersInCluster(zkClient));
  }
}
