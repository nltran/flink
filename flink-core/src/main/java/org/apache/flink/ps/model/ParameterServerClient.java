package org.apache.flink.ps.model;

/**
 * Created by Thomas Peel @ Eura Nova
 * on 3/07/15.
 */
public interface ParameterServerClient {
	public void update(String id, ParameterElement value, ParameterElement<Double> opt);

	public void updateShared(String id, ParameterElement value);

	public ParameterElement get(String id);

	public ParameterElement getShared(String id);
}
