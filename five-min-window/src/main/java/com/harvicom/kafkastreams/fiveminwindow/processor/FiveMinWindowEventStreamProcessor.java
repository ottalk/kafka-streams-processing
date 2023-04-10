package com.harvicom.kafkastreams.fiveminwindow.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Grouped;
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
                //node.put("TRANSACTION_TIME",node.get("TRANSACTION_TIME").asText()+"TEST");
                String compositeKey=node.get("MERCHANT_NO").asText()+":"+node.get("DIV").asText()+":"+node.get("COUNTRY").asText();
                return new KeyValue<>(compositeKey, value);
            }
        }).to("streams-test-2", Produced.with(Serdes.String(),jsonSerde));

        //KGroupedStream<String, JsonNode> group = kStream.groupByKey(Grouped.with(Serdes.String(), jsonSerde));

        KTable<String, JsonNode> countAndSum = kStream.groupByKey(Grouped.with(Serdes.String(), jsonSerde)).aggregate(
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
                    /*
                    if (eventLastOfItskind) return null;
                    */
                    //((ObjectNode)aggregate).put("MERCHANT_NO", aggregate.get("MERCHANT_NO").asText());
                    //((ObjectNode)aggregate).put("DIV", aggregate.get("DIV").asText());
                    //((ObjectNode)aggregate).put("COUNTRY", aggregate.get("COUNTRY").asText());
                    ((ObjectNode)aggregate).put("count", aggregate.get("count").asLong() + 1);
                    ((ObjectNode)aggregate).put("sum", aggregate.get("sum").asDouble() + value.get("RESPONSE_TIME").asDouble());
                    ((ObjectNode)aggregate).put("max", Math.max(aggregate.get("max").asDouble(),value.get("RESPONSE_TIME").asDouble()));
                    return aggregate;
                },
                Materialized.with(Serdes.String(),jsonSerde));

        
        KTable<String, JsonNode> average = countAndSum.mapValues((value -> {ObjectNode node = JsonNodeFactory.instance.objectNode();
            //node.put("MERCHANT_NO", value.get("MERCHANT_NO").asText());
            //node.put("DIV", value.get("DIV").asText());
            //node.put("COUNTRY", value.get("COUNTRY").asText());
            node.put("count",value.get("count").asLong());
            node.put("max",value.get("max").asDouble());
            node.put("avg",value.get("sum").asDouble() / (double)value.get("count").asDouble());
            return node;
        }));
                
        //average.to(Serdes.String(), jsonSerde, "streams-test-2");
        average.toStream().to("streams-test-3", Produced.with(Serdes.String(), jsonSerde));

/*
 public void process(KStream<SensorKeyDTO, SensorDataDTO> stream) {

        buildAggregateMetricsBySensor(stream)
                .to(outputTopic, Produced.with(String(), new SensorAggregateMetricsSerde()));

    }

private KStream<String, SensorAggregateMetricsDTO> buildAggregateMetricsBySensor(KStream<SensorKeyDTO, SensorDataDTO> stream) {
        return stream
                .map((key, val) -> new KeyValue<>(val.getId(), val))
                .groupByKey(Grouped.with(String(), new SensorDataSerde()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(WINDOW_SIZE_IN_MINUTES)).grace(Duration.ofMillis(0)))
                .aggregate(SensorAggregateMetricsDTO::new,
                        (String k, SensorDataDTO v, SensorAggregateMetricsDTO va) -> aggregateData(v, va),
                        buildWindowPersistentStore())
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value));
    }


    private Materialized<String, SensorAggregateMetricsDTO, WindowStore<Bytes, byte[]>> buildWindowPersistentStore() {
        return Materialized
                .<String, SensorAggregateMetricsDTO, WindowStore<Bytes, byte[]>>as(WINDOW_STORE_NAME)
                .withKeySerde(String())
                .withValueSerde(new SensorAggregateMetricsSerde());
    }
 */



    }
}
