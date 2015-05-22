package org.apache.flink.runtime.iterative.event;

import org.apache.flink.api.common.aggregators.Aggregator;

import java.util.Map;

/**
 * Created by nltran on 22/05/15.
 */
public class AggregatorEvent extends IterationEventWithAggregators{

	public AggregatorEvent(Map<String, Aggregator<?>> aggregators) {
		super(aggregators);
	}
}
