package com.nohibernate.demo;

import com.nohibernate.demo.jooq.tables.Account;
import com.nohibernate.demo.jooq.tables.AccountToPolicy;
import com.nohibernate.demo.jooq.tables.Policy;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Transactional
public class JooqRepository {

    private final DSLContext queryBuilder;

    public void create(String policyNumber, String accountNumber) {
        com.nohibernate.demo.jooq.tables.records.PolicyRecord policy =
                queryBuilder.insertInto(Policy.POLICY)
                .columns(Policy.POLICY.POLICY_NUMBER)
                .values(policyNumber)
                .onConflict(Policy.POLICY.POLICY_NUMBER)
                .doUpdate()
                .set(Policy.POLICY.POLICY_NUMBER, Policy.POLICY.POLICY_NUMBER)
                .returning(Policy.POLICY.ID)
                .fetchOne();

        com.nohibernate.demo.jooq.tables.records.AccountRecord account =
                queryBuilder.insertInto(Account.ACCOUNT)
                .columns(Account.ACCOUNT.ACCOUNT_NUMBER)
                .values(accountNumber)
                .onConflict(Account.ACCOUNT.ACCOUNT_NUMBER)
                .doUpdate()
                .set(Account.ACCOUNT.ACCOUNT_NUMBER, Account.ACCOUNT.ACCOUNT_NUMBER)
                .returning(Account.ACCOUNT.ID)
                .fetchOne();

        queryBuilder.insertInto(AccountToPolicy.ACCOUNT_TO_POLICY)
                .set(AccountToPolicy.ACCOUNT_TO_POLICY.ACCOUNT_ID, account.getId())
                .set(AccountToPolicy.ACCOUNT_TO_POLICY.POLICY_ID, policy.getId())
                .onConflictDoNothing()
                .execute();
    }

    public List<String> selectAccountsByPolicyNumber(String policyNumber) {
        return queryBuilder.select(Account.ACCOUNT.ACCOUNT_NUMBER).from(Policy.POLICY)
                .join(AccountToPolicy.ACCOUNT_TO_POLICY)
                .on(Policy.POLICY.ID.eq(AccountToPolicy.ACCOUNT_TO_POLICY.POLICY_ID))
                .join(Account.ACCOUNT)
                .on(Account.ACCOUNT.ID.eq(AccountToPolicy.ACCOUNT_TO_POLICY.ACCOUNT_ID))
                .where(Policy.POLICY.POLICY_NUMBER.eq(policyNumber))
                .fetch(Account.ACCOUNT.ACCOUNT_NUMBER);
    }

    public List<AssignmentDto> selectAssignmentsByPolicyNumber(String policyNumber) {
        return queryBuilder.select(Account.ACCOUNT.ACCOUNT_NUMBER,
                Policy.POLICY.POLICY_NUMBER,
                AccountToPolicy.ACCOUNT_TO_POLICY.ID)
                .from(Policy.POLICY)
                .join(AccountToPolicy.ACCOUNT_TO_POLICY)
                .on(Policy.POLICY.ID.eq(AccountToPolicy.ACCOUNT_TO_POLICY.POLICY_ID))
                .join(Account.ACCOUNT)
                .on(Account.ACCOUNT.ID.eq(AccountToPolicy.ACCOUNT_TO_POLICY.ACCOUNT_ID))
                .where(Policy.POLICY.POLICY_NUMBER.eq(policyNumber))
                .fetch()

                .stream()
                .map(record -> AssignmentDto.builder()
                        .accountNumber(record.value1())
                        .policyNumber(record.value2())
                        .id(record.value3())
                        .build()
                ).collect(Collectors.toList());
    }

    public void deleteAccount(String accountNumber) {
        queryBuilder.delete(Account.ACCOUNT).where(Account.ACCOUNT.ACCOUNT_NUMBER.eq(accountNumber)).execute();
    }

    public void deletePolicy(String policyNumber) {
        queryBuilder.delete(Policy.POLICY).where(Policy.POLICY.POLICY_NUMBER.eq(policyNumber)).execute();
    }

    public void deleteAssignment(String policyNumber, String accountNumber) {
        Long accountID = queryBuilder.select(Account.ACCOUNT.ID)
                .from(Account.ACCOUNT)
                .where(Account.ACCOUNT.ACCOUNT_NUMBER.eq(accountNumber))
                .fetchOne(Account.ACCOUNT.ID);

        Long policyId = queryBuilder.select(Policy.POLICY.ID)
                .from(Policy.POLICY)
                .where(Policy.POLICY.POLICY_NUMBER.eq(policyNumber))
                .fetchOne(Policy.POLICY.ID);

        queryBuilder.delete(AccountToPolicy.ACCOUNT_TO_POLICY)
                .where(AccountToPolicy.ACCOUNT_TO_POLICY.POLICY_ID.eq(policyId))
                .and(AccountToPolicy.ACCOUNT_TO_POLICY.ACCOUNT_ID.eq(accountID))
                .execute();
    }

    public void deleteAssignment(Long id) {
        queryBuilder.delete(AccountToPolicy.ACCOUNT_TO_POLICY)
                .where(AccountToPolicy.ACCOUNT_TO_POLICY.ID.eq(id))
                .execute();
    }

    public void clear() {
        queryBuilder.delete(Account.ACCOUNT)
                .execute();
        queryBuilder.delete(Policy.POLICY)
                .execute();
    }
}
