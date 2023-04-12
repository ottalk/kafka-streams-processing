package com.harvicom.kafkastreams.fiveminwindow.processor;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LogAndContinueProductionExceptionHandler implements ProductionExceptionHandler {

    Logger logger = LoggerFactory.getLogger(LogAndContinueProductionExceptionHandler.class);

    @Override
    public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record,
                                                     final Exception exception) {
        logger.error("Kafka message marked as processed although it failed. Message: [{}], destination topic: [{}]",  new String(record.value()), record.topic(), exception);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
    }

}
