package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.Random;
import java.time.LocalDateTime;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.producerApplicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        
        Random random = new Random();

        logger.info("Start sending messages...");
        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            LocalDateTime now = LocalDateTime.now();
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "{\"TransactionTime\"=\"" + now + "\",\"Merchant\"=\"MERCHANT" + random.nextInt(10) + "\",\"Div\"=\"" + random.nextInt(10) + "001\"" + ",\"Country\"=\"UK\",\"ResponseTime\"=" + String.format("%.2f",random.nextDouble()) + "}"));
        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
