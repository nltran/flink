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


package org.apache.flink.runtime.iterative.concurrent;

import java.util.concurrent.CountDownLatch;

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.iterative.event.AllWorkersDoneEvent;
import org.apache.flink.runtime.iterative.event.ClockTaskEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.iterative.task.IterationHeadPactTask;
import org.apache.flink.types.Value;

/**
 * A resettable one-shot latch.
 * Replaces the SuperStepBarrier held by the {@link IterationHeadPactTask}
 */
public class ABSPClockHolder implements EventListener<TaskEvent> {
	
	private final ClassLoader userCodeClassLoader;

	private boolean terminationSignaled = false;

	private CountDownLatch latch;

	private String[] aggregatorNames;
	private Value[] aggregates;
	
	private int ownClock =0;
	private int currentClock=0;
	private int absp = 3;
	
	
	public ABSPClockHolder(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
	}
	
	public void clock(){
		this.ownClock++;
	}
	
	public int getClock(){
		return this.ownClock;
	}
	

	/** setup the barrier, has to be called at the beginning of each superstep */
	public void setup() {
//		if(ownClock > currentClock + absp )
			latch = new CountDownLatch(1);
	}

	/** wait on the barrier */
	public void waitForNextClock() throws InterruptedException {
		if(ownClock > currentClock + absp ) {
			setup();
			latch.await();
		}
	}

	public String[] getAggregatorNames() {
		return aggregatorNames;
	}
	
	public Value[] getAggregates() {
		return aggregates;
	}

	/** barrier will release the waiting thread if an event occurs
	 * @param event*/
	@Override
	public void onEvent(TaskEvent event) {
		if (event instanceof TerminationEvent) {
			terminationSignaled = true;
		}
		
		else if (event instanceof ClockTaskEvent) {
			ClockTaskEvent cte = (ClockTaskEvent) event;
			currentClock = cte.getClock();
			if (ownClock > currentClock + absp){
				return;
			}
		}
		
		else if (event instanceof AllWorkersDoneEvent) {
			AllWorkersDoneEvent wde = (AllWorkersDoneEvent) event;
			aggregatorNames = wde.getAggregatorNames();
			aggregates = wde.getAggregates(userCodeClassLoader);
		}
		else {
			throw new IllegalArgumentException("Unknown event type.");
		}

		latch.countDown();
	}

	public boolean terminationSignaled() {
		return terminationSignaled;
	}
}
