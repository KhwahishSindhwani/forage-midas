package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {
    private final UserRepository userRepository;

    public TransactionConsumer(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void consume(Transaction transaction) {
        // Update sender balance
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender != null) {
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            userRepository.save(sender);
        }

        // Update recipient balance
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient != null) {
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());
            userRepository.save(recipient);
        }
    }
}
