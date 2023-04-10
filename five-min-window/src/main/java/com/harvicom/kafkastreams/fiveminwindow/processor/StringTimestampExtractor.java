package com.harvicom.kafkastreams.fiveminwindow.processor;

import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;

public class StringTimestampExtractor implements TimestampExtractor {
  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    //String json = (String) record.value();

    // create object mapper instance
    //ObjectMapper mapper = new ObjectMapper();

    // convert JSON string to `JsonNode`
    //JsonNode node = null;
    //try {
    //  node = mapper.readTree(json);
    //} catch (JsonProcessingException e) {
    //  e.printStackTrace();
    //}

    ObjectNode node = (ObjectNode) record.value();
    if (node != null && node.get("TRANASACTION_TIME").asText() != null) {
      String timestamp = node.get("TRANASACTION_TIME").asText();
      return Instant.parse(timestamp).toEpochMilli();
    }
    return partitionTime;
  }
}