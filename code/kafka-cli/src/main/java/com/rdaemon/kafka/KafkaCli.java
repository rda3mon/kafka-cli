package com.rdaemon.kafka;

import com.rdaemon.kafka.commands.PartitionsCommand;
import com.rdaemon.kafka.commands.TopicsCommand;
import com.rdaemon.kafka.models.ZKConfig;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(name = "kafkacli", mixinStandardHelpOptions = true, version = "1.0.0") public class KafkaCli implements Runnable {

  @Option(names = { "-v",
      "--verbose" }, required = false, type = Boolean.class, description = "Verbose mode. Helpful for troubleshooting. Multiple -v options increase the verbosity.")
  private boolean[] verbose = new boolean[0];

  @Option(names = { "-zk", "--zookeeper" }, required = true, type = String.class, description = "Zookeeper with port. Example: 127.0.0.1:2181")
  private String zookeeper;

  @Option(names = { "-l", "--list" }, required = false, type = Boolean.class, description = "Flag to list selected topic/partition/broker")
  private boolean list;

  @Option(names = { "-lb", "--list-balancer" }, required = false, type = Boolean.class, description = "List partition balancer") private boolean
      listBalancer;

  @Option(names = { "-gb", "--generate-balancer" }, required = false, type = Boolean.class, description = "Generate partition balancer")
  private boolean generateBalancer;

  @Option(names = { "-boc", "--balancer-output-count" }, required = false, type = Integer.class, defaultValue = "20", description = "Generate "
      + "partition balancer") private int maxOutputReassignment;

  @Option(names = { "-ts", "--topics" }, required = false, arity = "1..*", description = "List of topics as input") String[] topics = new String[0];

  @CommandLine.ArgGroup(exclusive = true, multiplicity = "1") Entity entityType;

  static class Entity {
    @Option(names = { "-t", "--topic" }, required = true, type = Boolean.class) boolean topic;

    @Option(names = { "-p", "--partition" }, type = Boolean.class, required = true) boolean partition;

    @Option(names = { "-b", "--broker" }, type = Boolean.class, required = true) boolean broker;
  }

  public void run() {
    ZkClient zkClient = new ZkClient(zookeeper, ZKConfig.getSessionTimeoutMs(), ZKConfig.getConnectionTimeoutMs(), ZKStringSerializer$.MODULE$);
    List<String> topicsAsList = Arrays.stream(topics).collect(Collectors.toList());

    if (entityType.partition) {
      PartitionsCommand partitionsCommand = new PartitionsCommand(zkClient);
      if (list) {
        partitionsCommand.list(topicsAsList);
      } else if (listBalancer) {
        partitionsCommand.ViewBalancer(topicsAsList);
      } else if (generateBalancer) {
        partitionsCommand.generateBalancer(topicsAsList, maxOutputReassignment);
      }
    } else if (entityType.broker) {

    } else if (entityType.topic) {
      if (list) {
        TopicsCommand topicsCommand = new TopicsCommand(zkClient);
        topicsCommand.list(topicsAsList);
      }
    } else {
      System.out.println("Invalid command. Please try --help");
    }
  }

  public static void main(String[] args) {
    // By implementing Runnable or Callable, parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.

    int exitCode = new CommandLine(new KafkaCli()).execute(args);
    System.exit(exitCode);
  }
}
