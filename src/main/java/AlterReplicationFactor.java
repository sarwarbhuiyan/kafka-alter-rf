import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.server.quota.ClientQuotaEntity.ConfigEntityType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "kafka-alter-rf",
    description = "A simply utility to alter the replication factor of a topic", version = "0.0.1",
    mixinStandardHelpOptions = true)
public class AlterReplicationFactor implements Runnable {

  @Option(required = false, names = {"-b", "--bootstrap-server"},
      description = "List of Kafka Bootstrap servers", defaultValue = "localhost:9092",
      showDefaultValue = Visibility.ALWAYS)
  private String bootstrapServers = "localhost:9092";

  @Option(required = false, names = {"-c", "--command-config"},
      description = "Config file containing properties like security credentials, etc",
      defaultValue = "", showDefaultValue = Visibility.ALWAYS)
  private String commandConfigFile = "";


  @Option(required = true, names = {"-t", "--topic"},
      description = "Topic to alter replication factor on")
  private String topic = null;

  @ArgGroup(exclusive = true, multiplicity = "0..1")
  Exclusive replicationFactorOrPlacement;

  static class Exclusive {
    @Option(required = false, names = {"-r", "--replication-factor"},
        description = "New replication factor")
    Integer replicationFactor = null;

//    @Option(names = {"-rp", "--replica-placement"}, required = false)
//    String replicaPlacement;

  }

  @Option(required = false, names = {"-e", "--execute"}, description = "Execute the plan")
  private boolean execute = false;

  @Option(required = false, names = {"-fr", "--force"},
      description = "Force reassignment even if the replication factor is met")
  private boolean forceReassignment = false;

  @Option(required = false, names = {"-f", "--file"},
      description = "File to export reassignment json to")
  private String file = "";

  @Option(required = false, names = {"-pr", "--preferred-rack"},
      description = "Preferred rack for leaders")
  private String preferredRack = null;

  @Spec
  CommandSpec spec;



  /**
   * Takes a number of lists and interleaves them by choosing one from each list
   * 
   * @param <T>
   * @param lists
   * @return
   */
  static <T> List<T> interleave(List<List<T>> lists) {
    int maxSize = lists.stream().mapToInt(List::size).max().orElse(0);
    return IntStream.range(0, maxSize).boxed()
        .flatMap(i -> lists.stream().filter(l -> i < l.size()).map(l -> l.get(i)))
        .collect(Collectors.toList());
  }

  /**
   * Takes a list and returns "take" items back but starting from position
   * 
   * @param <T>
   * @param input
   * @param position
   * @return
   */
  static <T> List<T> rotation(List<T> input, int position, int take) {
    List<T> output = new ArrayList<>();
    for (int i = position; i < take + position; i++) {
      output.add(input.get(i % input.size()));
    }
    // System.out.println("Rotation"+output);
    return output;
  }

  /**
   * Putting in a strategy interface so that we could have more clever reassignments (e.g. least
   * amount of steps)
   * 
   * @author sarwar
   *
   */
  public interface ReassignmentStrategy {
    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments() throws Exception;
  }

  /**
   * This can be moved to another class as just one strategy to builder out the reassignments list
   * 
   * @author sarwar
   *
   */
  public static class RoundRobinAcrossRacksStrategy implements ReassignmentStrategy {
    protected final String topic;
    protected final List<TopicPartitionInfo> currentPartitions;
    // private final Queue<List<Integer>> permutations;
    protected List<Integer> rackAlternatingNodes;
    protected int replicationFactor;
    protected Random rand;

    public RoundRobinAcrossRacksStrategy(String topic, Collection<Node> brokers,
        List<TopicPartitionInfo> currentPartitions, int replicationFactor) {
      this.currentPartitions = currentPartitions;
      this.topic = topic;
      this.replicationFactor = replicationFactor;
      this.rand = new Random();

      List<List<Node>> splitByRackNodes = brokers.stream()
          .collect(Collectors
              .groupingBy(n -> (n.rack() != null && n.rack().length() > 0) ? n.rack() : ""))
          .values().stream().collect(Collectors.toList());
      // System.out.println("SplitByNodes: "+splitByRackNodes);

      this.rackAlternatingNodes =
          interleave(splitByRackNodes).stream().map(n -> n.id()).collect(Collectors.toList());

      // System.out.println("RackAlternatingNodes: "+rackAlternatingNodes);
    }

    @Override
    public Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments() {

      // rotate randomly
      List<Integer> randomlyRotatedNodes = rotation(rackAlternatingNodes,
          rand.nextInt(rackAlternatingNodes.size()), rackAlternatingNodes.size());
      // List<Integer> randomlyRotatedNodes = rackAlternatingNodes;

      return currentPartitions.stream()
          .collect(Collectors.toMap(tp -> new TopicPartition(topic, tp.partition()),
              tp -> Optional.of(new NewPartitionReassignment(
                  rotation(randomlyRotatedNodes, tp.partition(), replicationFactor)))));
    }


  }


  public static class PreferredRackRoundRobinAcrossArackStrategy
      extends RoundRobinAcrossRacksStrategy {

    protected String preferredRack = "";

    protected Map<String, List<Integer>> splitByRackNodes;

    protected List<Integer> preferredRackAlternatingNodes; // leaders chosen from this list always;

    protected List<Integer> remainingRackAlternativeNodes; // followers chosen from this list
                                                           // always;

    public PreferredRackRoundRobinAcrossArackStrategy(String topic, Collection<Node> brokers,
        List<TopicPartitionInfo> currentPartitions, int replicationFactor, String preferredRack) {
      super(topic, brokers, currentPartitions, replicationFactor);
      this.preferredRack = preferredRack;

      this.splitByRackNodes = brokers.stream()
          .collect(Collectors.groupingBy(
              n -> (n.rack() != null && n.rack().length() > 0) ? n.rack() : "",
              Collectors.mapping(n -> n.id(), Collectors.toList())));

      this.preferredRackAlternatingNodes = this.splitByRackNodes.get(preferredRack);

      this.remainingRackAlternativeNodes =
          this.splitByRackNodes.entrySet().stream().filter(i -> !i.getKey().equals(preferredRack))
              .flatMap(e -> e.getValue().stream().map(s -> s)).collect(Collectors.toList());
    }

    @Override
    public Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments() {
      // rotate randomly the leader
      List<Integer> randomlyRotatedLeaderNodes = rotation(preferredRackAlternatingNodes,
          rand.nextInt(preferredRackAlternatingNodes.size()), preferredRackAlternatingNodes.size());
      List<Integer> randomlyRotatedFollowerNodes = rotation(remainingRackAlternativeNodes,
          rand.nextInt(remainingRackAlternativeNodes.size()), remainingRackAlternativeNodes.size());

      return currentPartitions.stream()
          .collect(Collectors.toMap(tp -> new TopicPartition(topic, tp.partition()),
              tp -> Optional.of(new NewPartitionReassignment(Stream
                  .of(rotation(randomlyRotatedLeaderNodes, tp.partition(),
                      randomlyRotatedLeaderNodes.size()),
                      rotation(randomlyRotatedFollowerNodes, tp.partition(),
                          replicationFactor - randomlyRotatedLeaderNodes.size()))
                  .flatMap(Collection::stream).collect(Collectors.toList())

              ))));


    }
  }

  public class MRCPreferredRackReassignmentStrategy implements ReassignmentStrategy {

    protected String preferredRack = null;
    protected Map<String, List<Integer>> splitByRackNodes;
    protected List<Integer> preferredRackAlternatingNodes;
    protected Map<TopicPartition, Optional<NewPartitionReassignment>> currentAssignments;
    protected Random rand = null;

    public MRCPreferredRackReassignmentStrategy(String topic, Collection<Node> brokers,
        Map<TopicPartition, Optional<NewPartitionReassignment>> currentAssignments,
        String replicaPlacement, String preferredRack) {
      this.preferredRack = preferredRack;
      this.currentAssignments = currentAssignments;
      this.rand = new Random();
      this.splitByRackNodes = brokers.stream()
          .collect(Collectors.groupingBy(
              n -> (n.rack() != null && n.rack().length() > 0) ? n.rack() : "",
              Collectors.mapping(n -> n.id(), Collectors.toList())));

      this.preferredRackAlternatingNodes = this.splitByRackNodes.get(preferredRack);
    }

    @Override
    public Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments()
        throws Exception {
      Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
      for (TopicPartition tp : currentAssignments.keySet()) {
        Optional<NewPartitionReassignment> replicaSet = currentAssignments.get(tp);
        List<Integer> replicas =
            replicaSet.orElseThrow(() -> new Exception("no reassignment list found")).targetReplicas();
        List<Integer> observers = 
            replicaSet.orElseThrow(() -> new Exception("no reassignment list found")).targetObservers();
        
        List<Integer> subtractObserverBrokers = subtractObservers(preferredRackAlternatingNodes, observers);
        Integer randomLeader = rotation(subtractObserverBrokers,
            rand.nextInt(subtractObserverBrokers.size()), 1).get(0);

        List<Integer> swappedReplicas = swapReplica(randomLeader, replicas);
        reassignments.put(tp, Optional.of(NewPartitionReassignment.ofReplicasAndObservers(swappedReplicas, observers)));
      }
      return reassignments;
    }

    private List<Integer> subtractObservers(List<Integer> preferredRackAlternatingNodes,
        List<Integer> observers) {
      return preferredRackAlternatingNodes.stream().filter(e -> !observers.contains(e)).collect(Collectors.toList());
    }

    private List<Integer> swapReplica(Integer randomLeader, List<Integer> replicas) {
      if(randomLeader == replicas.get(0))
          return replicas;
      
      Integer[] asArray = new Integer[replicas.size()];
      replicas.toArray(asArray);
      for (int i = 1; i < asArray.length; i++) {
        if(asArray[i] == randomLeader) {
          Integer temp = asArray[0];
          asArray[0] = randomLeader;
          asArray[i] = temp;
          break;
        }
      }
      return Arrays.asList(asArray);
    }

  }

  @Override
  public void run() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", this.bootstrapServers);

    if (commandConfigFile.length() > 0) {
      Properties fileProps = new Properties();
      try {
        fileProps.load(new FileInputStream(commandConfigFile));
      } catch (Throwable e) {
        throw new CommandLine.ParameterException(spec.commandLine(),
            "Could not find or read specified file");
      }
      fileProps.forEach((key, value) -> properties.put(key, value)); // merge two props
    }



    try (Admin client = AdminClient.create(properties)) {
      List<TopicPartitionInfo> currentPartitions =
          client.describeTopics(Collections.singleton(topic)).topicNameValues().get(topic).get()
              .partitions();

      Collection<Node> brokers = client.describeCluster().nodes().get();

      DescribeConfigsResult topicConfigResources = client.describeConfigs(
          Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, topic)));
      final Map<ConfigResource, Config> topicConfigs =
          topicConfigResources.all().get(10, TimeUnit.SECONDS);

      ReassignmentStrategy reassignmentStrategy = null;

      Map<TopicPartition, Optional<NewPartitionReassignment>> currentAssignments = new HashMap<>();
      for (TopicPartitionInfo tpi : currentPartitions) {
        List<Integer> replicas =
            tpi.replicas().stream().map(r -> r.id()).collect(Collectors.toList());
        List<Integer> observers =
            tpi.observers().stream().map(r -> r.id()).collect(Collectors.toList());
        NewPartitionReassignment assignment =
            NewPartitionReassignment.ofReplicasAndObservers(replicas, observers);
        currentAssignments.put(new TopicPartition(topic, tpi.partition()), Optional.of(assignment));
      }
      
      Integer replicationFactor = replicationFactorOrPlacement!=null ? replicationFactorOrPlacement.replicationFactor : currentPartitions.get(0).replicas().size();
      
      
      System.out.println("Current Assignments:");
      System.out.println(formatReassignmentJson(currentAssignments));

      ConfigEntry confluentPlacementConstraint = topicConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, topic)).get("confluent.placement.constraints");
      if (confluentPlacementConstraint!=null) {

        //String replicaPlacementFileLocation = replicationFactorOrPlacement !=null ? replicationFactorOrPlacement.replicaPlacement : confluentPlacementConstraint.value();
        if (replicationFactorOrPlacement!=null && replicationFactorOrPlacement.replicationFactor != null) {
          throw new CommandLine.ParameterException(spec.commandLine(),
              "Cannot set replication factor as this topic has confluent.placement.constraints set");
        }

//        if (replicaPlacementFileLocation != null
//            && new File(replicaPlacementFileLocation).exists()) {
//
//        }

        if (preferredRack != null) {
          reassignmentStrategy = new MRCPreferredRackReassignmentStrategy(topic, brokers, currentAssignments, "", preferredRack);
        }

      } else {
        
        // We're dealing with plain replication factor
        
        if (replicationFactor != null && replicationFactor > brokers.size()) {
          throw new CommandLine.ParameterException(spec.commandLine(),
              "Replication factor cannot exceed the number of brokers present");
        }

        if (currentPartitions.get(0).replicas().size() == replicationFactor && !forceReassignment) {
          throw new CommandLine.ParameterException(spec.commandLine(),
              "Replication factor is already " + replicationFactor);
        }



        reassignmentStrategy =
            new RoundRobinAcrossRacksStrategy(topic, brokers, currentPartitions, replicationFactor);

        if (preferredRack != null)
          reassignmentStrategy = new PreferredRackRoundRobinAcrossArackStrategy(topic, brokers,
              currentPartitions, replicationFactor, preferredRack);
      }

      HashSet<TopicPartition> partitionSet = new HashSet<>();
      currentPartitions.stream().map(tpi -> new TopicPartition(topic, tpi.partition()))
          .collect(Collectors.toSet());


      System.out.println("Reassignments:");
      Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments =
          reassignmentStrategy.reassignments();
      for (TopicPartition tp : reassignments.keySet()) {
        System.out.println(reassignments.get(tp).get().targetReplicas());
      }

      // System.out.println(formatReassignmentJson(reassignments));

      // execute reassignment
      if (execute) {
        client.alterPartitionReassignments(reassignments).all().get();
        System.out
            .println("Replication factor for topic " + topic + " updated to " + replicationFactor);
      } else {
        System.out.println(
            "The above assignment is only a proposed assignment. Use --execute to execute the assignment");

        String output = formatReassignmentJson(reassignments);
        // if the user specified a file parameter
        if (file != null && !file.isEmpty()) {
          byte[] strToBytes = output.getBytes();

          try (FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(strToBytes);
            System.out.println("Written reassignment plan to " + file + " successfully");
          } catch (Exception e) {
            e.printStackTrace();
          }

        }
      }

    } catch (CommandLine.ParameterException p) {
      throw p;
    } catch (Exception e) {
      e.printStackTrace();
      throw new CommandLine.ParameterException(spec.commandLine(),
          "A fatal exception has occurred. ");

    }



  }


  private String formatReassignmentJson(
      Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {
    ObjectMapper mapper = new ObjectMapper();


    try {
      Map<String, Object> reassignmentsMap = new HashMap<String, Object>();
      reassignmentsMap.put("version", 1);

      // TreeSet<TopicPartition> sortedKeys = new TreeSet<>(reassignments.keySet());

      List<Map<String, Object>> partitions = reassignments.keySet().stream()
          .sorted((a, b) -> a.partition() - b.partition()).map(tp -> {
            List<Integer> replicas = reassignments.get(tp).get().targetReplicas();
            List<Integer> observers = reassignments.get(tp).get().targetObservers();
            return new HashMap<String, Object>() {
              {
                put("topic", tp.topic());
                put("log_dirs", replicas.stream().map(r -> "any").collect(Collectors.toList()));
                put("partition", tp.partition());
                put("replicas", replicas);
                if (observers != null && observers.size() > 0)
                  put("observers", observers);
              }
            };
          }).collect(Collectors.toList());


      reassignmentsMap.put("partitions", partitions);

      String reassignmentsMapJson =
          mapper.writerWithDefaultPrettyPrinter().writeValueAsString(reassignmentsMap);
      return reassignmentsMapJson;
    } catch (JsonProcessingException e) {
      System.out.println("Error occurred converting plan to JSON:\n\n");
      e.printStackTrace();
    }
    return null;

  }

  public static void main(String... args) {

    System.exit(new CommandLine(new AlterReplicationFactor()).execute(args));


    // Properties properties = new Properties();
    // properties.put("bootstrap.servers", "kafka4:9095");
    // String topic = "test";
    // int replicationFactor = 3;
    //
    // try (AdminClient client = AdminClient.create(properties)) {
    // List<TopicPartitionInfo> currentPartitions = client
    // .describeTopics(Collections.singleton(topic)).values().get(topic).get().partitions();
    //
    // System.out.println("Current Assignments:");
    // for (TopicPartitionInfo tpi : currentPartitions) {
    // System.out.println(tpi.replicas().stream().map(r -> r.id()).collect(Collectors.toList()));
    //
    // }
    //
    // Collection<Node> brokers = client.describeCluster().nodes().get();
    //
    // ReassignmentStrategy reassignmentStrategy = new RoundRobinAcrossRacksStrategy(topic, brokers,
    // currentPartitions, replicationFactor);
    //
    // System.out.println("Reassignments:");
    // Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments =
    // reassignmentStrategy.reassignments();
    //
    // //execute reassignment
    // client.alterPartitionReassignments(reassignments).all().get();

    //
    // } catch (Exception e) {
    // e.printStackTrace();
    // }

  }


}
