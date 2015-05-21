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
import org.apache.flink.runtime.iterative.event.ClockTaskEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.types.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resettable one-shot latch.
 * Replaces the SuperStepBarrier held by the {@link org.apache.flink.runtime.iterative.task.IterationHeadPactTask}
 */
public class SSPClockHolder implements EventListener<TaskEvent> {

	private static final Logger log = LoggerFactory.getLogger(SSPClockHolder.class);

	private final ClassLoader userCodeClassLoader;

	private boolean terminationSignaled = false;

	private CountDownLatch latch;

	private String[] aggregatorNames;
	private Value[] aggregates;

	private int ownClock =0;
	private int currentClock=0;
	private int absp = 3;
	private boolean justBeenReleased = false;

	public boolean isJustBeenReleased() {
		return justBeenReleased;
	}

	public void resetJustBeenReleased(){
		justBeenReleased = false;
	}

	public boolean isAtABSPLimit() {
		return (ownClock == currentClock+absp);
	}

	public SSPClockHolder(ClassLoader userCodeClassLoader) {
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
			log.info("Worker is ahead of absp bound (" + absp + ")  : current clock:" + currentClock +" ownclock: " + ownClock);
			setup();
			latch.await();
		}

		log.info("Worker can continue moving ahead (" + absp + ")  : current clock:" + currentClock +" ownclock: " + ownClock);

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
			aggregatorNames = cte.getAggregatorNames();
			aggregates = cte.getAggregates(userCodeClassLoader);
			if (ownClock > currentClock + absp){
				return;
			}
		}

//		else if (event instanceof AllWorkersDoneEvent) {
//			AllWorkersDoneEvent wde = (AllWorkersDoneEvent) event;
//			aggregatorNames = wde.getAggregatorNames();
//			aggregates = wde.getAggregates(userCodeClassLoader);
//		}
		else {
			throw new IllegalArgumentException("Unknown event type.");
		}

		latch.countDown();
		justBeenReleased = true;
	}

	public boolean terminationSignaled() {
		return terminationSignaled;
	}
}
