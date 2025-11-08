package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class IncentiveApiTest {
    static final Logger logger = LoggerFactory.getLogger(IncentiveApiTest.class);

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private UserPopulator userPopulator;

    @Test
    void test_incentive_endpoint() {
        userPopulator.populate();
        
        // Create a test transaction
        Transaction transaction = new Transaction(1, 2, 100.0f);
        
        // Create headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Transaction> request = new HttpEntity<>(transaction, headers);
        
        // Call the /incentive endpoint
        ResponseEntity<Incentive> response = restTemplate.postForEntity(
            "/incentive", 
            request, 
            Incentive.class
        );
        
        logger.info("----------------------------------------------------------");
        logger.info("Response status: " + response.getStatusCode());
        logger.info("Incentive amount: " + response.getBody().getAmount());
        logger.info("Expected incentive: 1.0 (1% of 100.0)");
        logger.info("----------------------------------------------------------");
        
        // Verify response
        assertNotNull(response.getBody());
        assertEquals(1.0f, response.getBody().getAmount(), 0.001);
    }
    
    @Test
    void test_invalid_transaction_returns_zero_incentive() {
        userPopulator.populate();
        
        // Create an invalid transaction (insufficient balance)
        Transaction transaction = new Transaction(4, 5, 10000.0f);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Transaction> request = new HttpEntity<>(transaction, headers);
        
        ResponseEntity<Incentive> response = restTemplate.postForEntity(
            "/incentive", 
            request, 
            Incentive.class
        );
        
        logger.info("----------------------------------------------------------");
        logger.info("Invalid transaction - Response status: " + response.getStatusCode());
        logger.info("Incentive amount: " + response.getBody().getAmount());
        logger.info("Expected incentive: 0.0 (invalid transaction)");
        logger.info("----------------------------------------------------------");
        
        assertNotNull(response.getBody());
        assertEquals(0.0f, response.getBody().getAmount(), 0.001);
    }
}
