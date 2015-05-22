package org.apache.flink.ps.impl;

import org.apache.flink.ps.model.ParameterElement;
import org.apache.flink.ps.model.ParameterServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by nltran on 28/04/15.
 */
public class ParameterServerIgniteImpl implements ParameterServer {

	private static final Logger log = LoggerFactory.getLogger(ParameterServerIgniteImpl.class);

	public final static String CACHE_NAME = ParameterServerIgniteImpl.class.getSimpleName();

	private Ignite ignite = null;
	private IgniteCache<String, ParameterElement> parameterCache = null;
	private IgniteCache<String, Integer> clockCache = null;

	public ParameterServerIgniteImpl(String name) {
		try {
			CacheConfiguration<String, ParameterElement> parameterCacheCfg = new CacheConfiguration<String, ParameterElement>();
			parameterCacheCfg.setCacheMode(CacheMode.PARTITIONED);
			parameterCacheCfg.setName(CACHE_NAME + "_parameter");

			CacheConfiguration<String, Integer> clockCacheCfg = new CacheConfiguration<String, Integer>();
			clockCacheCfg.setCacheMode(CacheMode.PARTITIONED);
			clockCacheCfg.setName(CACHE_NAME + "_clock");

			IgniteConfiguration cfg1 = new IgniteConfiguration();
//			cfg1.setClientMode(true);
			cfg1.setGridName(name);
			cfg1.setPeerClassLoadingEnabled(true);
			cfg1.setCacheConfiguration(parameterCacheCfg, clockCacheCfg);
			this.ignite = Ignition.start(cfg1);

			parameterCache = ignite.getOrCreateCache(parameterCacheCfg);
			clockCache = ignite.getOrCreateCache(clockCacheCfg);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void update(String id, ParameterElement value) {
		parameterCache.put(id, value);
	}

	@Override
	public void clock(String wid) {
		Integer oldClock = clockCache.get(wid);
		if (oldClock == null) {
			clockCache.put(wid, 1);
		} else {
			clockCache.replace(wid, oldClock + 1);
		}
		if (log.isInfoEnabled()) {
			log.info("Worker " + wid + " is at clock " + clockCache.get(wid));
		}
	}

	public void clock(int wid) {
		clock(Integer.toString(wid));
	}

	@Override
	public void shutDown() {
		ignite.close();
	}

	@Override
	public ParameterElement get(String id) {
		return parameterCache.get(id);
	}

}