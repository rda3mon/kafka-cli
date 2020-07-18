# kafka-cli
Kafka tool for managing complex kafka clusters

### Build the package

#### Clone the repository

```
git clone https://github.com/rda3mon/kafka-cli
```

#### Build the package

```
cd kafka-cli && mvn clean package
```

#### Help command

```
java -cp kafka-cli/target/kafka-cli-jar-with-dependencies.jar com.rdaemon.kafka.KafkaCli --help
Usage: kafkacli (-t | -p | -b) [-hlVv] [-gb] [-lb] -zk=<zookeeper>
                [-ts=<topics>...]...
  -b, --broker
      -gb, --generate-balancer
                             Generate partition balancer
  -h, --help                 Show this help message and exit.
  -l, --list                 Flag to list selected topic/partition/broker
      -lb, --list-balancer   List partition balancer
  -p, --partition
  -t, --topic
      -ts, --topics=<topics>...
                             List of topics as input
  -v, --verbose              Verbose mode. Helpful for troubleshooting.
                               Multiple -v options increase the verbosity.
  -V, --version              Print version information and exit.
      -zk, --zookeeper=<zookeeper>
                             Zookeeper with port. Example: 127.0.0.1:2181
```

### TODO

* Limit the number of movement with stage wise movement
* Display per topic movement
