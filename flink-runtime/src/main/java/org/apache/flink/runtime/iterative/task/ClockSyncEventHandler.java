/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.iterative.task;

import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.iterative.event.WorkerClockEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.types.Value;

import com.gs.collections.api.bag.MutableBag;
import com.gs.collections.impl.bag.mutable.HashBag;

public class ClockSyncEventHandler implements EventListener<TaskEvent> {
	
	private final ClassLoader userCodeClassLoader;
	
	private final Map<String, Aggregator<?>> aggregators;

//	private final int numberOfEventsUntilEndOfSuperstep;

//	private int workerDoneEventCounter;
	
	private boolean endOfSuperstep;
	
	private int absp = 3;
	
	private int currentClock = 0;
	
	
	private final MutableBag<Integer> workersClocks;


	public ClockSyncEventHandler(Map<String, Aggregator<?>> aggregators, ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
		this.aggregators = aggregators;
		this.workersClocks = new HashBag<Integer>();
	}
	
	private void workerClock(int workerInt) {
		this.workersClocks.add(workerInt);
	}
	
	private int computeCurrentClock() {
		return workersClocks.occurrencesOf(workersClocks.min());
	}
	
	public int getCurrentClock(){
		return currentClock;
	}

	@Override
	public void onEvent(TaskEvent event) {
//		if (WorkerDoneEvent.class.equals(event.getClass())) {
//			onWorkerDoneEvent((WorkerDoneEvent) event);
//			return;
//		}
		
		if (WorkerClockEvent.class.equals(event.getClass())){
			onWorkerClockEvent((WorkerClockEvent)event);
			return;
		}
		
		throw new IllegalStateException("Unable to handle event " + event.getClass().getName());
	}
	
	private void onWorkerClockEvent(WorkerClockEvent workerClockEvent) {
		String[] aggNames = workerClockEvent.getAggregatorNames();
		Value[] aggregates = workerClockEvent.getAggregates(userCodeClassLoader);
		
		if (aggNames.length != aggregates.length) {
			throw new RuntimeException("Inconsistent WorkerDoneEvent received!");
		}
		
		int workerIndex = workerClockEvent.getWorkerIndex();
		int workerClock = workerClockEvent.getWorkerClock();
		
		int oldClock = currentClock;
		workerClock(workerIndex);
		currentClock = computeCurrentClock();

		if(workerClock > currentClock + absp) {
			for (int i = 0; i < aggNames.length; i++) {
				@SuppressWarnings("unchecked")
				Aggregator<Value> aggregator = (Aggregator<Value>) this.aggregators.get(aggNames[i]);
				aggregator.aggregate(aggregates[i]);
			}
		}

		if(oldClock != currentClock ) {
			this.endOfSuperstep = true;
			Thread.currentThread().interrupt();
		}

	}

//	private boolean allWorkersAtSameClock() {
//		return workersClocks.sizeDistinct() == 1;
//	}
//
//	private boolean allWorkersAtClock(int clock) {
//		for(Iterator i = workersClocks.iterator();i.h)
//	}
//	private void onWorkerDoneEvent(WorkerDoneEvent workerDoneEvent) {
//		if (this.endOfSuperstep) {
//			throw new RuntimeException("Encountered WorderDoneEvent when still in End-of-Superstep status.");
//		}
//		
//		workerDoneEventCounter++;
//
//		String[] aggNames = workerDoneEvent.getAggregatorNames();
//		Value[] aggregates = workerDoneEvent.getAggregates(userCodeClassLoader);
//
//		if (aggNames.length != aggregates.length) {
//			throw new RuntimeException("Inconsistent WorkerDoneEvent received!");
//		}
//		
//		for (int i = 0; i < aggNames.length; i++) {
//			@SuppressWarnings("unchecked")
//			Aggregator<Value> aggregator = (Aggregator<Value>) this.aggregators.get(aggNames[i]);
//			aggregator.aggregate(aggregates[i]);
//		}
//
//		if (workerDoneEventCounter % numberOfEventsUntilEndOfSuperstep == 0) {
//			endOfSuperstep = true;
//			Thread.currentThread().interrupt();
//		}
//	}
	
	public boolean isEndOfSuperstep() {
		return this.endOfSuperstep;
	}
	
	public void resetEndOfSuperstep() {
		this.endOfSuperstep = false;
	}
}
