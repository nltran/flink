package org.apache.flink.api.common.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.ps.impl.ParameterServerIgniteImpl;
import org.apache.flink.ps.model.ParameterElement;
import org.apache.flink.ps.model.ParameterServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by nltran on 12/05/15.
 */
public abstract class RichMapFunctionWithSSPServer<IN, OUT> extends RichMapFunction<IN, OUT> {

//	private ParameterServer psInstance = null;

	private static final Logger log = LoggerFactory.getLogger(RichMapFunctionWithSSPServer.class);

	private int wid;

	private IgniteCache<String, ParameterElement> parameterCache = null;
//	private IgniteCache<String, Integer> clockCache = null;
//	private IgniteCache<String, Boolean> convergenceCache = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		wid = getRuntimeContext().getIndexOfThisSubtask();
//		System.out.println("This subtask ID is " + wid);
//		if (parameterCache == null   && clockCache == null) {
		if (parameterCache == null) {
			Ignite ignite = Ignition.ignite(ParameterServerIgniteImpl.GRID_NAME);
			parameterCache = ignite.getOrCreateCache(ParameterServerIgniteImpl.getParameterCacheConfiguration());
//			clockCache = ignite.getOrCreateCache(ParameterServerIgniteImpl.getClockCacheConfiguration());
//			convergenceCache = ignite.getOrCreateCache(ParameterServerIgniteImpl.getConvergenceCacheConfiguration());
		}


//		if (psInstance == null) {
////			psInstance = new ParameterServerIgniteImpl(getRuntimeContext().getTaskName(), false);
//
//			psInstance = new ParameterServerIgniteImpl(Integer.toString(wid), true);
//		}

//		System.out.println("nc :" + ++nc);

//		System.out.println("taskName is:" + getRuntimeContext().getTaskName());

	}

	protected ParameterServer getParameterServer() {
//		return psInstance;
		return null;
	}

//	protected void putConvergence(boolean b) {
//		convergenceCache.put(Integer.toString(getRuntimeContext().getIndexOfThisSubtask()), b);
//	}


	protected void update(String id, ParameterElement el) {
		parameterCache.put(id, el);

//		if( psInstance != null) {
//			psInstance.update(id, el);
//		}
//		else {
//			throw new NullPointerException("Parameter server should have been instantiated at this stage");
//		}
	}

	protected ParameterElement get(String id) {
		return parameterCache.get(id);
//		if( psInstance != null) {
//			return psInstance.get(id);
//		}
//		else {
//			throw new NullPointerException("Parameter server should have been instantiated at this stage");
//		}
//		return null;
	}

//	protected void clock() {
//
//		Integer oldClock = clockCache.get(Integer.toString(wid));
//		if (oldClock == null) {
//			clockCache.put(Integer.toString(wid), 1);
//		} else {
//			clockCache.replace(Integer.toString(wid), oldClock + 1);
//		}
//		if (log.isInfoEnabled()) {
//			log.info("Worker " + wid + " is at clock " + clockCache.get(Integer.toString(wid)));
//		}
////		if( psInstance != null) {
////			psInstance.clock(getRuntimeContext().getIndexOfThisSubtask());
////		}
////		else {
////			throw new NullPointerException("Parameter server should have been instantiated at this stage");
////		}
//
//	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}

