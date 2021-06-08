package com.example.sbremote.batch.partitioner;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RemotePartitioner implements Partitioner {

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> map = new HashMap<>(gridSize);
		AtomicInteger partitionNumber = new AtomicInteger(1);
		try {
			long min = 1L, max= 100L;
			long targetSize = (max - min) / gridSize + 1;
			long start = 1L;
			long end =  (start + targetSize) - 1;
			while(start <= max) {
				if(end >= max) {
					end = max;
				}
				ExecutionContext context = new ExecutionContext();
				context.putLong("startIdx", start);
				context.putLong("endIdx", end);
				map.put("RemotePartition" + partitionNumber.getAndIncrement(), context);

				start += targetSize;
				end += targetSize;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return map;
	}
}
