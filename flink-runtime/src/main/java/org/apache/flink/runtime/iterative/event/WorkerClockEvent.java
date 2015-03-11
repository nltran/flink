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


package org.apache.flink.runtime.iterative.event;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

public class WorkerClockEvent extends IterationEventWithAggregators {
	
	private int workerIndex;
	private int workerClock;
	
	public WorkerClockEvent() {
		super();
	}

	public WorkerClockEvent(int workerIndex, int workerClock, String aggregatorName, Value aggregate) {
		super(aggregatorName, aggregate);
		this.workerIndex = workerIndex;
		this.workerClock = workerClock;
	}
	
	public WorkerClockEvent(int workerIndex, int workerClock, Map<String, Aggregator<?>> aggregators) {
		super(aggregators);
		this.workerIndex = workerIndex;
		this.workerClock = workerClock;
	}
	
	public int getWorkerIndex() {
		return workerIndex;
	}
	
	public int getWorkerClock() {
		return workerClock;
	}
	
	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.workerIndex);
		out.writeLong(workerClock);
		super.write(out);
	}
	
	@Override
	public void read(DataInputView in) throws IOException {
		this.workerIndex = in.readInt();
		this.workerClock = in.readInt();
		super.read(in);
	}
}
