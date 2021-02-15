package com.michelin.ns4kafka.validation;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@NoArgsConstructor
@Getter
@Setter
public class ConnectValidator extends ResourceValidator{
    @Builder
    public ConnectValidator(Map<String, Validator> validationConstraints){
        super(validationConstraints);
    }
}
