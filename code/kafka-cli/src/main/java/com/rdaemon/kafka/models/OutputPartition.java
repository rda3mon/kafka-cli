package com.rdaemon.kafka.models;

import java.util.ArrayList;

public class OutputPartition {
  private final String topic;

  private final Integer partition;

  private final ArrayList<Integer> replicas;

  public OutputPartition(String topic, Integer partition, ArrayList<Integer> replicas) {
    this.topic = topic;
    this.partition = partition;
    this.replicas = replicas;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public ArrayList<Integer> getReplicas() {
    return replicas;
  }
}
