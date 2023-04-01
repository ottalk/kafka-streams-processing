package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

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

        BufferedReader reader;

		try {
			reader = new BufferedReader(new FileReader("/Users/ottalk/Github/kafka-streams-processing/datetime-fix/SampleTransactions.txt"));
			String line = reader.readLine();

            int i=1;
            logger.info("Start sending messages...");
			while (line != null) {
				System.out.println(line);
                producer.send(new ProducerRecord<>(AppConfigs.topicName, i,line));
				// read next line
				line = reader.readLine();
                i++;
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();
    }
}
