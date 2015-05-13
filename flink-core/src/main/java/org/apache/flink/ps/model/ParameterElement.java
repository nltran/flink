package org.apache.flink.ps.model;

import java.io.Serializable;

/**
 * Created by nltran on 07/05/15.
 */
public interface ParameterElement<T> extends Serializable{
	int getClock();
	T getValue();
}
