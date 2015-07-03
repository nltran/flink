package org.apache.flink.api.common.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.ps.impl.ParameterServerIgniteImpl;
import org.apache.flink.ps.model.ParameterElement;
import org.apache.flink.ps.model.ParameterServer;
import org.apache.flink.ps.model.ParameterServerClient;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by nltran on 12/05/15.
 */
public abstract class RichMapFunctionWithSSPServer<IN, OUT> extends RichMapFunction<IN, OUT>
implements ParameterServerClient {

	private static final Logger log = LoggerFactory.getLogger(RichMapFunctionWithSSPServer.class);

	private int wid;

	private IgniteCache<String, ParameterElement> parameterCache = null;
    private IgniteCache<String, ParameterElement> sharedCache = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		wid = getRuntimeContext().getIndexOfThisSubtask();

		if (parameterCache == null) {
			Ignite ignite = Ignition.ignite(ParameterServerIgniteImpl.GRID_NAME);
			parameterCache = ignite.getOrCreateCache(ParameterServerIgniteImpl.getParameterCacheConfiguration());
		}

        if (sharedCache == null) {
            Ignite ignite = Ignition.ignite(ParameterServerIgniteImpl.GRID_NAME);
            sharedCache = ignite.getOrCreateCache(ParameterServerIgniteImpl
                    .getSharedCacheConfiguration());
        }
	}

    @Override
	public void update(String id, ParameterElement el) {
		parameterCache.withAsync().put(id, el);
	}

    @Override
    public void updateShared(String id, ParameterElement el) {
        sharedCache.withAsync().put(id, el);
    }

    @Override
    public ParameterElement get(String id) {
        return parameterCache.get(id);
    }

    @Override
    public ParameterElement getShared(String id) {
        return sharedCache.get(id);
    }

	@Override
	public void close() throws Exception {
		super.close();
	}
}

