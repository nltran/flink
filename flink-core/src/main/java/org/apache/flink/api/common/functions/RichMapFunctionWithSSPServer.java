package org.apache.flink.api.common.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.ps.impl.ParameterServerIgniteImpl;
import org.apache.flink.ps.model.ParameterElement;
import org.apache.flink.ps.model.ParameterServer;

/**
 * Created by nltran on 12/05/15.
 */
public abstract class RichMapFunctionWithSSPServer<IN, OUT> extends RichMapFunction<IN, OUT> {

	private ParameterServer psInstance = null;
	private int wid;
//	private int nc = 0;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		wid = getRuntimeContext().getIndexOfThisSubtask();
		System.out.println("This subtask ID is " + wid);
		if (psInstance == null) {
			psInstance = new ParameterServerIgniteImpl(Integer.toString(wid));
		}
//		System.out.println("nc :" + ++nc);
		System.out.println("taskName is:" + getRuntimeContext().getTaskName());

	}

	protected ParameterServer getParameterServer() {
		return psInstance;
	}

	protected void update(String id, ParameterElement el) {
		if( psInstance != null) {
			psInstance.update(id, el);
		}
		else {
			throw new NullPointerException("Parameter server should have been instantiated at this stage");
		}
	}

	protected ParameterElement get(String id) {
		if( psInstance != null) {
			return psInstance.get(id);
		}
		else {
			throw new NullPointerException("Parameter server should have been instantiated at this stage");
		}
	}

	protected void clock() {
		if( psInstance != null) {
			psInstance.clock(getRuntimeContext().getIndexOfThisSubtask());
		}
		else {
			throw new NullPointerException("Parameter server should have been instantiated at this stage");
		}

	}

	@Override
	public void close() throws Exception {
		super.close();
//		clock();
	}
}

