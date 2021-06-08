package com.example.sbremote.batch;

import com.example.sbremote.batch.partitioner.LocalPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@StepScope
@Slf4j
public class CustomStepExecutionListener extends StepExecutionListenerSupport {

    @Autowired
    private LocalPartitioner localPartitioner;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        try {
            long startIdx = stepExecution.getExecutionContext().getLong("startIdx");
            long endIdx = stepExecution.getExecutionContext().getLong("endIdx");
            localPartitioner.min = startIdx;
            localPartitioner.max = endIdx;
            log.info("Worker Partition: startIdx: {}, endIdx: {}", startIdx, endIdx);

        } catch (Exception e) {
            log.error("system error, archive file failed" + e);
        }
        super.beforeStep(stepExecution);
    }
}
