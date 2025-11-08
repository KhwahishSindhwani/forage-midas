package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class WaldorfBalanceTest {
    static final Logger logger = LoggerFactory.getLogger(WaldorfBalanceTest.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private DatabaseConduit databaseConduit;

    @Test
    void verify_waldorf_balance_after_transactions() throws InterruptedException {
        // Populate initial users
        userPopulator.populate();
        
        // Send all transactions
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        
        // Wait for processing
        Thread.sleep(2000);

        // Query Waldorf's balance (ID 5)
        UserRecord waldorf = databaseConduit.findUser(5L);
        assertNotNull(waldorf, "Waldorf should exist in database");
        
        logger.info("Waldorf's final balance: {}", waldorf.getBalance());
        logger.info("Waldorf's name: {}", waldorf.getName());
        
        // Waldorf (ID 5) transactions:
        // Starting: 444.55
        // Receives from 9: +45.42 = 489.97
        // Receives from 6: +32.12 = 522.09
        // Sends to 9: -78.74 = 443.35
        // Receives from 9: +184.51 = 627.86
        // Transaction 4->5 (133.86) fails due to insufficient balance
        // Final: 627.86
        assertEquals(627.86f, waldorf.getBalance(), 0.01f, 
                "Waldorf's balance should be 627.86 after all transactions");
    }
}
