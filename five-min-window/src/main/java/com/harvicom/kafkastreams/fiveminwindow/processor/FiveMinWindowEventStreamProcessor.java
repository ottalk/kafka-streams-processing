package com.harvicom.kafkastreams.fiveminwindow.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.KeyValue;

@Component
public class FiveMinWindowEventStreamProcessor {

    @Autowired
    private StreamsBuilder streamsBuilder;

    @PostConstruct
    public void streamTopology() {

        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        //Consumed<String, JsonNode> consumerOptions = Consumed.with(Serdes.String(), jsonSerde).withTimestampExtractor(new StringTimestampExtractor());
        //KStream<String, JsonNode> kStream = streamsBuilder.stream("streams-test-1", consumerOptions);
        KStream<String, JsonNode> kStream = streamsBuilder.stream("streams-test-1", Consumed.with(Serdes.String(), jsonSerde));


        //kStream.filter((key, value) -> value.startsWith("Message_")).mapValues((k, v) -> v.toUpperCase()).peek((k, v) -> System.out.println("Key : " + k + " Value : " + v)).to("streams-test-2", Produced.with(Serdes.String(), Serdes.String()));
        kStream.map(new KeyValueMapper<String,JsonNode,KeyValue<String,JsonNode>>() {
            @Override
            public KeyValue<String, JsonNode> apply(String key, JsonNode value) {
                ObjectNode node = (ObjectNode) value;
                node.put("TRANSACTION_TIME",node.get("TRANSACTION_TIME").asText()+"TEST");
                return new KeyValue<>(key, value);
            }
        }).to("streams-test-2", Produced.with(Serdes.String(),jsonSerde));


    }
}
