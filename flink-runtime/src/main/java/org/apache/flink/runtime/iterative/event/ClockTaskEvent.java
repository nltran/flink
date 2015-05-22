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

/**
 * This class provides a simple implementation of clock event as is used in A-BSP and SSP iterations.
 * 
 */
public class ClockTaskEvent extends IterationEventWithAggregators {

	/**
	 * The clock information encapsulated by this event.
	 */
	private int clock = -1;

	/**
	 * The default constructor implementation. It should only be used for deserialization.
	 */
	public ClockTaskEvent() {
		super();
	}
	
	public ClockTaskEvent(int clock, Map<String, Aggregator<?>> aggregators) {
		super(aggregators);
		this.clock = clock;
	}

	/**
	 * Constructs a new clock task event with the given clock.
	 * 
	 * @param clock
	 *        the clock value that shall be stored in this event
	 */
	public ClockTaskEvent(final int clock) {
		this.clock = clock;
	}

	/**
	 * Returns the stored clock.
	 * 
	 * @return the stored clock or <code>-1</code> if no clock is set
	 */
	public int getClock() {
		return this.clock;
	}


	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeLong(this.clock);
		super.write(out);
//		StringRecord.writeString(out, Long.toString(this.clock));

	}


	@Override
	public void read(final DataInputView in) throws IOException {
		this.clock = in.readInt();
		super.read(in);
//		this.clock = Integer.parseInt(StringRecord.readString(in));
	}


	@Override
	public int hashCode() {
		return ((int) (this.clock >>> 32)) ^ ((int) this.clock);
	}


	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof ClockTaskEvent)) {
			return false;
		}

		final ClockTaskEvent ste = (ClockTaskEvent) obj;

		return this.clock == ste.getClock();
	}
}