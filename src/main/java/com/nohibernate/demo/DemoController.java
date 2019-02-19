package com.nohibernate.demo;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Transactional
public class DemoController {

    private final JooqRepository jooqRepository;

    @PostMapping("/fail/{policyNumber}")
    public void createFail(@PathVariable("policyNumber") String policyNumber) {
        jooqRepository.createUnsafePolicy(policyNumber);
        jooqRepository.createUnsafePolicy(policyNumber);
    }

    @PostMapping("/assignments/{policyNumber}/{accountNumber}")
    public void create(@PathVariable("policyNumber") String policyNumber,
                       @PathVariable("accountNumber") String accountNumber) {
        jooqRepository.create(policyNumber, accountNumber);
        jooqRepository.createUnsafePolicy(policyNumber);
    }

    @GetMapping(value = "/accounts/{policyNumber}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> getAccountsForPolicy(@PathVariable("policyNumber") String policyNumber) {
        return jooqRepository.selectAccountsByPolicyNumber(policyNumber);
    }

    @GetMapping(value = "/assignments/{policyNumber}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public List<AssignmentDto> getAssignmentsForPolicy(@PathVariable("policyNumber") String policyNumber) {
        return jooqRepository.selectAssignmentsByPolicyNumber(policyNumber);
    }

    @DeleteMapping("/accounts/{accountNumber}")
    public void deleteAccount(@PathVariable("accountNumber") String accountNumber) {
        jooqRepository.deleteAccount(accountNumber);
    }

    @DeleteMapping("/policies/{policyNumber}")
    public void deletePolicy(@PathVariable("policyNumber") String policyNumber) {
        jooqRepository.deletePolicy(policyNumber);
    }

    @DeleteMapping("/assignments/{policyNumber}/{accountNumber}")
    public void deleteAssignment(@PathVariable("policyNumber") String policyNumber,
                                 @PathVariable("accountNumber") String accountNumber) {
        jooqRepository.deleteAssignment(policyNumber, accountNumber);
    }

    @DeleteMapping("/assignments/{id}")
    public void deleteAssignment(@PathVariable("id") Long id) {
        jooqRepository.deleteAssignment(id);
    }

    @DeleteMapping("/")
    public void deleteAll() {
        jooqRepository.clear();
    }
}
