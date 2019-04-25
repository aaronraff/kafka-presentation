import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {

    public static void main(String[] args) throws Exception {
        // Some configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-words");

        // Broker to connect to
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Default Serializers and Deserializers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Commit interval for KTable, defauly is 1 minute
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);

        final StreamsBuilder builder = new StreamsBuilder();

        // Get input from stream-counts-input topic
        KStream<String, String> source = builder.stream("streams-counts-input");

        // Group the records by the value and count them
        KTable<String, Long> counts = source.groupBy(new KeyValueMapper<String, String, String>() {
            public String apply(String key, String val) {
                return val;
            }
        }).count();

        // Write the records to streams-counts-output topic
        counts.toStream().to("streams-counts-output", Produced.with(Serdes.String(), Serdes.Long()));

        // Create the Processor topology
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // Handle Ctrl-C
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        // Start the stream processor and don't stop until interrupt
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }
}
