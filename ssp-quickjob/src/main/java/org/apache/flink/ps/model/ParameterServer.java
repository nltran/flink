package org.apache.flink.ps.model;

/**
 * Created by nltran on 28/04/15.
 */
public interface ParameterServer {

	public void update(String id, ParameterElement value);
	public void clock(String wid);
	public void clock(int wid);
	public ParameterElement get(String id);


}
