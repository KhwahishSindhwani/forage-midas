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

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class UserTenBalanceTest {
    static final Logger logger = LoggerFactory.getLogger(UserTenBalanceTest.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private DatabaseConduit databaseConduit;

    @Test
    void check_user_ten_receives_incentive() throws InterruptedException {
        userPopulator.populate();
        
        UserRecord user10 = databaseConduit.findById(10);
        float initialBalance = user10.getBalance();
        logger.info("User 10's initial balance: " + initialBalance);
        
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(3000);
        
        user10 = databaseConduit.findById(10);
        logger.info("----------------------------------------------------------");
        logger.info("User 10's final balance: " + user10.getBalance());
        logger.info("User 10's final incentive: " + user10.getIncentive());
        logger.info("Expected incentive: 3.5285 (1% of 352.85)");
        logger.info("----------------------------------------------------------");
        
        // Verify incentive was added
        float expectedIncentive = 3.5285f;
        assertEquals(expectedIncentive, user10.getIncentive(), 0.001);
        assertEquals(initialBalance + expectedIncentive, user10.getBalance(), 0.001);
    }
}
