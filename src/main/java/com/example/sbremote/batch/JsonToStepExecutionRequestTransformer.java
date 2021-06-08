package com.example.sbremote.batch;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.json.JsonToObjectTransformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.PostConstruct;

@Component
@Slf4j
public class JsonToStepExecutionRequestTransformer extends JsonToObjectTransformer {
    private ObjectMapper objectMapper;

    @PostConstruct
    public void construct() {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, false);
    }

    @Override
    protected Object doTransform(Message<?> message) throws Exception {
        StepExecutionRequest stepExecutionRequest = buildStepExecutionRequest(message);
        return this.getMessageBuilderFactory().withPayload(stepExecutionRequest).build();
    }

    private StepExecutionRequest buildStepExecutionRequest(Message<?> message) throws IOException {
        String s = message.getPayload().toString();
        log.info("transformer payload: {}", s);
//        return new ObjectMapper().readValue(s, StepExecutionRequest.class);
        try {
            LinkedHashMap map = objectMapper.readValue(s, LinkedHashMap.class);
            return new StepExecutionRequest((String) map.get("stepName"), Long.valueOf((Integer) map.get("jobExecutionId")),
                    Long.valueOf((Integer) map.get("stepExecutionId")));
        }catch (Exception ex) {
            log.error("Failed transformer", ex);
            throw new RuntimeException(ex);
        }
    }
}