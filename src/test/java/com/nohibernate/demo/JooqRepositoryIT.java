package com.nohibernate.demo;

import com.nohibernate.demo.jooq.tables.Account;
import com.nohibernate.demo.jooq.tables.AccountToPolicy;
import com.nohibernate.demo.jooq.tables.Policy;
import org.jooq.DSLContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class JooqRepositoryIT {

    @Autowired
    private JooqRepository jooqRepository;
    @Autowired
    private DSLContext dsl;

    @Test
    public void create_multipleEntries_doesNotProduceUnwantedDuplicates() {
        jooqRepository.create("1", "1");
        jooqRepository.create("1", "1");
        jooqRepository.create("1", "2");
        jooqRepository.create("2", "2");

        assertThat(jooqRepository.selectAssignmentsByPolicyNumber("1")).hasSize(2).containsOnly(
                AssignmentDto.builder().accountNumber("1").policyNumber("1").build(),
                AssignmentDto.builder().accountNumber("2").policyNumber("1").build()
        );

        assertThat(jooqRepository.selectAccountsByPolicyNumber("1")).hasSize(2).containsOnly("1", "2");

        assertThat(jooqRepository.selectAssignmentsByPolicyNumber("2")).hasSize(1).containsOnly(
                AssignmentDto.builder().accountNumber("2").policyNumber("2").build()
        );

        assertThat(jooqRepository.selectAccountsByPolicyNumber("2")).hasSize(1).containsOnly("2");

        assertThat(dsl.fetchCount(Account.ACCOUNT)).isEqualTo(2);
        assertThat(dsl.fetchCount(Policy.POLICY)).isEqualTo(2);
        assertThat(dsl.fetchCount(AccountToPolicy.ACCOUNT_TO_POLICY)).isEqualTo(3);
    }

    @Test
    public void delete_account_doesDeleteAssignmentButNotPolicy() {
        jooqRepository.create("1", "1");

        jooqRepository.deleteAccount("1");

        assertThat(dsl.fetchCount(Account.ACCOUNT)).isEqualTo(0);
        assertThat(dsl.fetchCount(Policy.POLICY)).isEqualTo(1);
        assertThat(dsl.fetchCount(AccountToPolicy.ACCOUNT_TO_POLICY)).isEqualTo(0);
    }

    @Test
    public void delete_policy_doesDeleteAssignmentButNotAccount() {
        jooqRepository.create("1", "1");

        jooqRepository.deletePolicy("1");

        assertThat(dsl.fetchCount(Account.ACCOUNT)).isEqualTo(1);
        assertThat(dsl.fetchCount(Policy.POLICY)).isEqualTo(0);
        assertThat(dsl.fetchCount(AccountToPolicy.ACCOUNT_TO_POLICY)).isEqualTo(0);
    }

    @Test
    public void delete_assignmentById_retainsPolicyAndAccount() {
        jooqRepository.create("1", "1");
        jooqRepository.deleteAssignment(jooqRepository.selectAssignmentsByPolicyNumber("1").get(0).getId());

        assertThat(dsl.fetchCount(Account.ACCOUNT)).isEqualTo(1);
        assertThat(dsl.fetchCount(Policy.POLICY)).isEqualTo(1);
        assertThat(dsl.fetchCount(AccountToPolicy.ACCOUNT_TO_POLICY)).isEqualTo(0);
    }

    @Test
    public void delete_assignmentByPolicyNumberAndAccount_retainsPolicyAndAccount() {
        jooqRepository.create("1", "1");
        jooqRepository.deleteAssignment("1", "1");

        assertThat(dsl.fetchCount(Account.ACCOUNT)).isEqualTo(1);
        assertThat(dsl.fetchCount(Policy.POLICY)).isEqualTo(1);
        assertThat(dsl.fetchCount(AccountToPolicy.ACCOUNT_TO_POLICY)).isEqualTo(0);
    }

    @Test
    public void clear_all_clearsDatabase() {
        jooqRepository.create("1", "1");
        jooqRepository.clear();

        assertThat(dsl.fetchCount(Account.ACCOUNT)).isEqualTo(0);
        assertThat(dsl.fetchCount(Policy.POLICY)).isEqualTo(0);
        assertThat(dsl.fetchCount(AccountToPolicy.ACCOUNT_TO_POLICY)).isEqualTo(0);
    }
}
