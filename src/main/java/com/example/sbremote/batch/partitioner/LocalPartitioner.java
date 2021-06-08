package com.example.sbremote.batch.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@StepScope
@Slf4j
public class LocalPartitioner extends StepExecutionListenerSupport implements Partitioner
//		, StepExecutionListener
{

	public long min, max;

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> map = new HashMap<>(gridSize);
		AtomicInteger partitionNumber = new AtomicInteger(1);
		try {
			log.info("Remote Worker Partition- min: {}, max: {}", this.min, this.max);
			long targetSize = (max - min) / gridSize + 1;
			long start = min;
			long end =  (start + targetSize) - 1;
			while(start <= max) {
				if(end >= max) {
					end = max;
				}
				ExecutionContext context = new ExecutionContext();
				context.putLong("locStartIdx", start);
				context.putLong("locEndIdx", end);
				/**
				 * Key for Remote + Local partition is to have unique partition names for both
				 */
				map.put("LocalPartition" + partitionNumber.getAndIncrement(), context);

				start += targetSize;
				end += targetSize;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return map;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		this.min = stepExecution.getExecutionContext().getLong("startIdx");
		this.max = stepExecution.getExecutionContext().getLong("endIdx");
		super.beforeStep(stepExecution);
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		return super.afterStep(stepExecution);
	}
}
