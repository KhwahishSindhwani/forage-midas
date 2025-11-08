package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserBalanceChecker {
    @Autowired
    private DatabaseConduit databaseConduit;

    public void printUserBalance(long userId, String userName) {
        UserRecord user = databaseConduit.findById(userId);
        if (user != null) {
            System.out.println("User: " + userName + " (ID: " + userId + ")");
            System.out.println("Balance: " + user.getBalance());
            System.out.println("Incentive: " + user.getIncentive());
        } else {
            System.out.println("User with ID " + userId + " not found");
        }
    }
}
