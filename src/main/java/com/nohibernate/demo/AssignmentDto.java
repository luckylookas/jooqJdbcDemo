package com.nohibernate.demo;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Builder
@EqualsAndHashCode(exclude = {"id"}) //for ease of testing
@Getter
public class AssignmentDto {
    private final String policyNumber;
    private final String accountNumber;
    private final Long id;
}
