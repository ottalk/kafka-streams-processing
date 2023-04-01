package com.harvicom.kafkastreams.datetimefix.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class EventStreamProcessor {

    @Autowired
    private StreamsBuilder streamsBuilder;

    @PostConstruct
    public void streamTopology() {


        KStream<String, String> kStream = streamsBuilder.stream("streams-test-1", Consumed.with(Serdes.String(), Serdes.String()));
        //Consumed<String, String> consumerOptions = Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new StringTimestampExtractor());
        //KStream<String, String> kStream = streamsBuilder.stream("streams-test-1", consumerOptions);

        kStream.filter((key, value) -> value.startsWith("Message_")).mapValues((k, v) -> v.toUpperCase()).peek((k, v) -> System.out.println("Key : " + k + " Value : " + v)).to("streams-test-2", Produced.with(Serdes.String(), Serdes.String()));
    }
}
