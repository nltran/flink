package org.apache.flink.ps.impl;

import org.apache.flink.ps.model.ParameterElement;

/**
 * Created by nltran on 07/05/15.
 */
public class ParameterElementImpl<T> implements ParameterElement<T>{

	private int clock;
	private T value;

	public ParameterElementImpl(int clock, T value) {
		this.clock = clock;
		this.value = value;
	}

	@Override
	public int getClock() {
		return clock;
	}

	@Override
	public T getValue() {
		return value;
	}
}
