package com.rdaemon.kafka.commands;

import java.util.HashSet;
import java.util.List;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

public class TopicsCommand implements Command {
  private ZkClient zkClient;

  public TopicsCommand() {
  }

  public TopicsCommand(ZkClient zkClient) {
    this.zkClient = zkClient;
  }

  @Override public void list(List<String> topic) {
    System.out.println(new HashSet<>(scala.collection.JavaConversions.seqAsJavaList(ZkUtils.getAllTopics(zkClient))));
  }
}
