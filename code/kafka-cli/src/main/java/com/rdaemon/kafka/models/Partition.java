package com.rdaemon.kafka.models;

import java.util.List;
import java.util.Set;

public class Partition {
  private int partitionId;
  private List<PartitionBroker> replicas;
  private List<PartitionBroker> isr;

  private Partition(int partitionId, List<PartitionBroker> replicas, List<PartitionBroker> isr) {
    this.partitionId = partitionId;
    this.replicas = replicas;
    this.isr = isr;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public List<PartitionBroker> getReplicas() {
    return replicas;
  }

  public void setReplicas(List<PartitionBroker> replicas) {
    this.replicas = replicas;
  }

  public List<PartitionBroker> getIsr() {
    return isr;
  }

  public void setIsr(List<PartitionBroker> isr) {
    this.isr = isr;
  }

  public static class Builder {
    private int partitionId;
    private List<PartitionBroker> replicas;
    private List<PartitionBroker> isr;

    public Builder() {
    }

    public Builder setPartitionId(int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    public Builder setReplicas(List<PartitionBroker> replicas) {
      this.replicas = replicas;
      return this;
    }

    public Builder setIsr(List<PartitionBroker> isr) {
      this.isr = isr;
      return this;
    }

    public Partition build() {
      return new Partition(this.partitionId, this.replicas, this.isr);
    }
  }
}
