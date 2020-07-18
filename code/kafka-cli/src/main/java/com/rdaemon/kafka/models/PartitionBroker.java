package com.rdaemon.kafka.models;

public class PartitionBroker {
  private int brokerId;
  private String host;

  private PartitionBroker(int brokerId, String host) {
    this.brokerId = brokerId;
    this.host = host;
  }

  public int getBrokerId() {
    return brokerId;
  }

  public PartitionBroker setBrokerId(int brokerId) {
    this.brokerId = brokerId;
    return this;
  }

  public String getHost() {
    return host;
  }

  public PartitionBroker setHost(String host) {
    this.host = host;
    return this;
  }

  public PartitionBroker build() {
    return this;
  }

  public static class Builder {
    private int brokerId;
    private String host;

    public Builder setBrokerId(int brokerId) {
      this.brokerId = brokerId;
      return this;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public PartitionBroker build() {
      return new PartitionBroker(this.brokerId, this.host);
    }
  }

  @Override public String toString() {
    return String.valueOf(brokerId);
  }
}
