package com.jpmc.midascore.controller;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.service.IncentiveService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IncentiveController {
    private final IncentiveService incentiveService;

    public IncentiveController(IncentiveService incentiveService) {
        this.incentiveService = incentiveService;
    }

    @PostMapping("/incentive")
    public ResponseEntity<Incentive> processIncentive(@RequestBody Transaction transaction) {
        Incentive incentive = incentiveService.processIncentive(transaction);
        return ResponseEntity.ok(incentive);
    }
}
