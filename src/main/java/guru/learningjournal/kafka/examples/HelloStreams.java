package guru.learningjournal.kafka.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.time.Duration;

public class HelloStreams {
    private static final Logger logger = LogManager.getLogger(HelloProducer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<Integer, Byte> kStream = streamsBuilder.stream(AppConfigs.topicName);
        //kStream.foreach((k, v) -> System.out.println("Key= " + k + " Value= " + v));
        //kStream.peek((k,v)-> System.out.println("Key= " + k + " Value= " + v));

        TimeWindows tumblingWindow = TimeWindows.of(Duration.ofSeconds(60));
        KTable<Windowed<Integer>, Long> transactionCount = kStream.groupByKey().windowedBy(tumblingWindow).count(Materialized.as("transaction-count"));
        //KTable<Windowed<Integer>, Long> responseTimeAvg = kStream.groupByKey().windowedBy(tumblingWindow).aggregate(null, null, null)   count(Materialized.as("response-time-avg"));
        transactionCount.toStream().print(Printed.<Windowed<Integer>, Long>toSysOut().withLabel("transaction-count"));
   
/* 
        protected static KTable<Long,Double>getRatingAverageTable ( KStream<Long,Byte>ratings,  String avgRatingsTopicName,   SpecificAvroSerde<CountAndSum>countAndSumSerde){
            // Grouping Ratings
            KGroupedStream<Long,Double>ratingsById=ratings.map((key,rating)->new KeyValue<>(rating.getMovieId(),rating.getRating())).groupByKey(with(Long(),Double()));
        
            final KTable<Long,CountAndSum>ratingCountAndSum=ratingsById.aggregate(()->new CountAndSum(0L,0.0),(key,value,aggregate)->{aggregate.setCount(aggregate.getCount()+1);aggregate.setSum(aggregate.getSum()+value);return aggregate;},Materialized.with(Long(),countAndSumSerde));
        
            final KTable<Long,Double>ratingAverage=ratingCountAndSum.mapValues(value->value.getSum()/value.getCount(),Materialized.as("average-ratings"));
        
            // persist the result in topic
            //ratingAverage.toStream().to(avgRatingsTopicName);return ratingAverage;
        }
*/

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        logger.info("Starting stream.");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down stream");
            streams.close();
        }));
    }
}
