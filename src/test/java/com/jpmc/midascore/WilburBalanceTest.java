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

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class WilburBalanceTest {
    static final Logger logger = LoggerFactory.getLogger(WilburBalanceTest.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private DatabaseConduit databaseConduit;

    @Test
    void check_wilbur_balance() throws InterruptedException {
        userPopulator.populate();
        
        // Print initial balance
        UserRecord wilbur = databaseConduit.findById(9);
        logger.info("Wilbur's initial balance: " + wilbur.getBalance());
        logger.info("Wilbur's initial incentive: " + wilbur.getIncentive());
        
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(3000);
        
        // Print final balance
        wilbur = databaseConduit.findById(9);
        logger.info("----------------------------------------------------------");
        logger.info("Wilbur's final balance: " + wilbur.getBalance());
        logger.info("Wilbur's final incentive: " + wilbur.getIncentive());
        logger.info("----------------------------------------------------------");
    }
}
