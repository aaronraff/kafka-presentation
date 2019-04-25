import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class AnomalyDetection {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-anomaly");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-anomaly-input");
        KTable<Windowed<String>, Long> purchases = source
            .map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                public KeyValue<String, String> apply(String key, String username) {
                    return new KeyValue<String, String>(username, username);
                }
        })
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
        .count()
        .filter(new Predicate<Windowed<String>, Long>() {
            public boolean test(Windowed<String> username, Long count) {
                return count >= 3;
            }
        });

        KStream<String, Long> purchasesOutput = purchases
                .toStream()
                .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>>() {
                    public KeyValue<String, Long> apply(Windowed<String> stringWindowed, Long aLong) {
                        return new KeyValue<String, Long>(stringWindowed.toString(), aLong);
                    }
                }).filter(new Predicate<String, Long>() {
                        public boolean test(String username, Long count) {
                            return count != null;
                    }
                });

        purchasesOutput.to("streams-anomaly-output", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }
}
