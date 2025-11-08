package com.jpmc.midascore.service;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IncentiveService {
    private final UserRepository userRepository;

    public IncentiveService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public boolean isValidTransaction(Transaction transaction) {
        // Validate transaction amount is positive
        if (transaction.getAmount() <= 0) {
            return false;
        }

        // Validate sender exists
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender == null) {
            return false;
        }

        // Validate recipient exists
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient == null) {
            return false;
        }

        // Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            return false;
        }

        // Validate sender and recipient are different
        if (transaction.getSenderId() == transaction.getRecipientId()) {
            return false;
        }

        return true;
    }

    public float calculateIncentive(Transaction transaction) {
        // Simple incentive calculation: 1% of transaction amount
        return transaction.getAmount() * 0.01f;
    }

    @Transactional
    public Incentive processIncentive(Transaction transaction) {
        if (!isValidTransaction(transaction)) {
            return new Incentive(0.0f);
        }

        float incentiveAmount = calculateIncentive(transaction);

        // Get sender and recipient
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Process the transaction: deduct from sender, add to recipient
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Update recipient's incentive field
        recipient.setIncentive(recipient.getIncentive() + incentiveAmount);

        // Add incentive to recipient's balance (not deducted from sender)
        recipient.setBalance(recipient.getBalance() + incentiveAmount);

        // Save the updated users
        userRepository.save(sender);
        userRepository.save(recipient);

        return new Incentive(incentiveAmount);
    }
}
