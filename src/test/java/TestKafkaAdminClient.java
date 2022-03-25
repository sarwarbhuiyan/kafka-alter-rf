import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestKafkaAdminClient {

    private AdminClient client = null;

    @Before
    public void setup() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        client = AdminClient.create(conf);

    }

    @After
    public void teardown() {
        client.close();
    }

    @Test
    public void testNames() throws InterruptedException, ExecutionException {
        ListTopicsResult ltr = client.listTopics();
        KafkaFuture<Set<String>> names = ltr.names();
        System.out.println(names.get());
    }

    //@Test
    public void testCreateTopic() {
        int partitions = 8;
        short replicationFactor = 2;
        try {
            KafkaFuture<Void> future = client
                    .createTopics(Collections.singleton(new NewTopic("tweet", partitions, replicationFactor)),
                            new CreateTopicsOptions().timeoutMs(10000))
                    .all();
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    //@Test
    public void testDeleteTopic() {
        KafkaFuture<Void> future = client.deleteTopics(Collections.singleton("tweet")).all();
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testChangeProperties() throws InterruptedException, ExecutionException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "tweet");

        // get the current topic configuration
        DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));

        Map<ConfigResource, org.apache.kafka.clients.admin.Config> config;
        config = describeConfigsResult.all().get();

        System.out.println(config);

        // create a new entry for updating the retention.ms value on the same topic
        ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "60000");
        Map<ConfigResource, Config> updateConfig = new HashMap<ConfigResource, Config>();
        updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));

        AlterConfigsResult alterConfigsResult = client.alterConfigs(updateConfig);
        alterConfigsResult.all();

        describeConfigsResult = client.describeConfigs(Collections.singleton(resource));

        config = describeConfigsResult.all().get();
    }

}
