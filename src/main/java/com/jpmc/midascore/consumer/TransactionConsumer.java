package com.jpmc.midascore.consumer;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.service.IncentiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);
    private final IncentiveService incentiveService;

    public TransactionConsumer(IncentiveService incentiveService) {
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void consume(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        incentiveService.processIncentive(transaction);
    }
}
