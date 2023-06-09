package com.harvicom.kafkastreams.fiveminwindow.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.KeyValue;
import java.time.Duration;

@Component
public class FiveMinWindowEventStreamProcessor {

    @Autowired
    private StreamsBuilder streamsBuilder;

    @PostConstruct
    public void streamTopology() {

        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer,stringDeserializer);

        TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(stringSerializer);
        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>();
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);


        //Consumed<String, JsonNode> consumerOptions = Consumed.with(Serdes.String(), jsonSerde).withTimestampExtractor(new StringTimestampExtractor());
        //KStream<String, JsonNode> kStream = streamsBuilder.stream("streams-test-1", consumerOptions);
        KStream<String, JsonNode> kStream = streamsBuilder.stream("streams-test-1", Consumed.with(Serdes.String(), jsonSerde));


        //kStream.filter((key, value) -> value.startsWith("Message_")).mapValues((k, v) -> v.toUpperCase()).peek((k, v) -> System.out.println("Key : " + k + " Value : " + v)).to("streams-test-2", Produced.with(Serdes.String(), Serdes.String()));
        kStream.map(new KeyValueMapper<String,JsonNode,KeyValue<String,JsonNode>>() {
            @Override
            public KeyValue<String, JsonNode> apply(String key, JsonNode value) {
                ObjectNode node = (ObjectNode) value;
                //node.put("TRANSACTION_TIME",node.get("TRANSACTION_TIME").asText()+"TEST");
                String compositeKey=node.get("MERCHANT_NO").asText()+":"+node.get("DIV").asText()+":"+node.get("COUNTRY").asText();
                return new KeyValue<>(compositeKey, value);
            }
        }).to("streams-test-2", Produced.with(Serdes.String(),jsonSerde));

        //KGroupedStream<String, JsonNode> group = kStream.groupByKey(Grouped.with(Serdes.String(), jsonSerde));
/*
        TimeWindows tumblingWindow = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10));
     

        KTable<Windowed<String>, JsonNode> countAndSum = kStream.groupByKey(Grouped.with(Serdes.String(), jsonSerde)).windowedBy(tumblingWindow).aggregate(
                () -> {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    node.put("count", 0);
                    node.put("sum", 0);
                    node.put("max", 0);
                    return (JsonNode) node;
                },
                (key, value, aggregate) -> {
                    // if we want to clear the state when we get the "last" event of this sort
                    // we can "just" return null here

                    //((ObjectNode)aggregate).put("WINDOW_START", key.getBytes().toString());
                    ((ObjectNode)aggregate).put("MERCHANT_NO", value.get("MERCHANT_NO").asText());
                    ((ObjectNode)aggregate).put("DIV", value.get("DIV").asText());
                    ((ObjectNode)aggregate).put("COUNTRY", value.get("COUNTRY").asText());
                    ((ObjectNode)aggregate).put("count", aggregate.get("count").asLong() + 1);
                    ((ObjectNode)aggregate).put("sum", aggregate.get("sum").asDouble() + value.get("RESPONSE_TIME").asDouble());
                    ((ObjectNode)aggregate).put("max", Math.max(aggregate.get("max").asDouble(),value.get("RESPONSE_TIME").asDouble()));
                    return aggregate;
                },
                Materialized.with(windowedSerde,jsonSerde)));

                //Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store") .withValueSerde(Serdes.Long())

        
        KTable<Windowed<String>, JsonNode> average = countAndSum.mapValues((value -> {ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("MERCHANT_NO", value.get("MERCHANT_NO").asText());
            node.put("DIV", value.get("DIV").asText());
            node.put("COUNTRY", value.get("COUNTRY").asText());
            node.put("count",value.get("count").asLong());
            node.put("max",value.get("max").asDouble());
            node.put("avg",value.get("sum").asDouble() / (double)value.get("count").asDouble());
            return node;
        }));
                
        //average.to(Serdes.String(), jsonSerde, "streams-test-2");
        average.toStream().to("streams-test-3", Produced.with(windowedSerde, jsonSerde));
*/
    }
}
