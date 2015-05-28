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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.iterative.event.WorkerClockEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.types.Value;

import com.gs.collections.api.bag.MutableBag;
import com.gs.collections.impl.bag.mutable.HashBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSPClockSyncEventHandler implements EventListener<TaskEvent> {

	private static final Logger log = LoggerFactory.getLogger(ClockSyncEventHandler.class);

	private final ClassLoader userCodeClassLoader;

	private final Map<String, Aggregator<?>> aggregators;

//	private final int numberOfEventsUntilEndOfSuperstep;

//	private int workerDoneEventCounter;

	private boolean endOfSuperstep;

	private boolean clockUpdated;

	private boolean aggUpdated;

	private int absp = 3;

	private int currentClock = 0;

	private final int numberOfEventsUntilEndOfSuperstep;


	private final MutableBag<Integer> workersClocks;

	private Map<Integer, Integer> clocks;


	public SSPClockSyncEventHandler(int numberOfEventsUntilEndOfSuperstep, Map<String, Aggregator<?>> aggregators, ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
		this.aggregators = aggregators;
		this.numberOfEventsUntilEndOfSuperstep = numberOfEventsUntilEndOfSuperstep;
		this.workersClocks = new HashBag<Integer>();
		this.clocks = new HashMap<Integer, Integer>();
	}

	private void workerClock(int workerInt) {
		this.workersClocks.add(workerInt);
	}

	private void workerClock(int workerInt, int clock) {
		this.clocks.put(workerInt, clock);
	}

	private int computeCurrentClock() {
		return workersClocks.occurrencesOf(workersClocks.min());
	}

	public int getCurrentClock() {
		return currentClock;
	}

	@Override
	public void onEvent(TaskEvent event) {
//		if (WorkerDoneEvent.class.equals(event.getClass())) {
//			onWorkerDoneEvent((WorkerDoneEvent) event);
//			return;
//		}

		if (WorkerClockEvent.class.equals(event.getClass())) {
			onWorkerClockEvent((WorkerClockEvent) event);
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

		log.info("Worker " + workerIndex + " is at clock " + workerClock);

//		int oldClock = currentClock;
		workerClock(workerIndex, workerClock);


		// Clock currentClock + absp is reached when workers completed absp iterations
//		if(workerClock > currentClock + absp) {
		for (int i = 0; i < aggNames.length; i++) {
			@SuppressWarnings("unchecked")
			Aggregator<Value> aggregator = (Aggregator<Value>) this.aggregators.get(aggNames[i]);
			aggregator.aggregate(aggregates[i]);
			this.aggUpdated = true;
			Thread.currentThread().interrupt();
		}
//		}

		int oldClock = currentClock;
		if (clocks.size() % numberOfEventsUntilEndOfSuperstep == 0) { //means all the workers have initialized
			int newClockCandidate = getMinClock();
			if (newClockCandidate > currentClock) {
				currentClock = newClockCandidate;
				this.clockUpdated = true;
				Thread.currentThread().interrupt();
			}
		}
		this.endOfSuperstep = true;


//		if(allWorkersAtClock( currentClock + absp + 1) && howManyWorkersAtClock(currentClock + absp + 1) % numberOfEventsUntilEndOfSuperstep ==0) {
//			currentClock++;
//			this.endOfSuperstep = true;
//			Thread.currentThread().interrupt();
//		}
	}

	private int howManyWorkersAtClock(int clock) {
		int count = 0;
		for (Map.Entry<Integer, Integer> e : clocks.entrySet()) {
			if (e.getValue() == clock) {
				++count;
			}
		}
		return count;
	}

	public boolean allWorkersAtClock(int clock) {
		for (Map.Entry<Integer, Integer> e : clocks.entrySet()) {
			if (e.getValue() != clock) {
				return false;
			}
		}
		return true;
	}

	private int getMinClock() {
		int min = Integer.MAX_VALUE;
		for (Map.Entry<Integer, Integer> e : clocks.entrySet()) {
			int val = e.getValue();
			if (val < min) {
				min = val;
			}
		}
		return min;
	}

//	private boolean allWorkersAtClock(int clock) {
//		Iterator<Integer> i = workersClocks.iterator();
//		while(i.hasNext()){
//			int ii = i.next();
//			if(ii != clock){
//				return false;
//			}
//
//		}
//		return true;
//
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

	public boolean isClockUpdated() {
		return this.clockUpdated;
	}

	public void resetClockUpdated() {
		this.clockUpdated = false;
	}

	public boolean isAggUpdated() {
		return this.aggUpdated;
	}

	public void resetAggUpdated() {
		this.aggUpdated = false;
	}
}
