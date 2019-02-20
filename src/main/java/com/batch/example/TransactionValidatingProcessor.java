package com.batch.example;

import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.util.StringUtils;

import com.batch.example.dto.Person;

public class TransactionValidatingProcessor extends ValidatingItemProcessor<Person> {
    public TransactionValidatingProcessor() {
        super(
            item -> {
                if (StringUtils.isEmpty(item.getFirstName())) {
                    throw new ValidationException("Customer has less than " +item.getFirstName());
                }
            }
        );
        setFilter(true);
    }
}