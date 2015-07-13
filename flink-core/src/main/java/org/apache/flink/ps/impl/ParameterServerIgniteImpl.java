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

	public final static String GRID_NAME = "FLINK_PARAMETER_SERVER";

	public static CacheConfiguration<String, ParameterElement> getParameterCacheConfiguration() {
		CacheConfiguration<String, ParameterElement> parameterCacheCfg = new CacheConfiguration<String, ParameterElement>();
		parameterCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		parameterCacheCfg.setName(CACHE_NAME + "_parameter");
//		parameterCacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		return parameterCacheCfg;
	}

	public static CacheConfiguration<String, ParameterElement> getSharedCacheConfiguration
			() {
		CacheConfiguration<String, ParameterElement> sharedCacheCfg = new
				CacheConfiguration<String, ParameterElement>();
		sharedCacheCfg.setCacheMode(CacheMode.REPLICATED);
		sharedCacheCfg.setName(CACHE_NAME + "_SHARED");
		return sharedCacheCfg;
	}

	private Ignite ignite = null;
	private IgniteCache<String, ParameterElement> parameterCache = null;
	private IgniteCache<String, ParameterElement> sharedCache = null;

	public ParameterServerIgniteImpl(String name, boolean client) {
		try {
			CacheConfiguration<String, ParameterElement> parameterCacheCfg = getParameterCacheConfiguration();
			CacheConfiguration<String, ParameterElement> sharedCacheCfg =
					getSharedCacheConfiguration();

//			CacheConfiguration<String, Integer> clockCacheCfg = getClockCacheConfiguration();

			IgniteConfiguration cfg1 = new IgniteConfiguration();
			cfg1.setGridName(name);
			cfg1.setPeerClassLoadingEnabled(true);
			cfg1.setCacheConfiguration(parameterCacheCfg, sharedCacheCfg);
//			cfg1.setCacheConfiguration(parameterCacheCfg);

			if (client) {
				cfg1.setClientMode(true);
				Ignition.setClientMode(true);
			}
			if (log.isInfoEnabled()) {
				String mode = client ? "client" : "server";
				log.info("Starting ps " + name + " in " + mode + " mode");
			}
			this.ignite = Ignition.start(cfg1);

			parameterCache = ignite.getOrCreateCache(parameterCacheCfg).withAsync();
			sharedCache = ignite.getOrCreateCache(sharedCacheCfg).withAsync();

			log.info("I hereby confirm that parameter cache is async enabled: " + parameterCache.isAsync());
			log.info("I hereby confirm that shared cache is async enabled: " + sharedCache
					.isAsync());


//			clockCache = ignite.getOrCreateCache(clockCacheCfg).withAsync();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

//	@Override
//	public void clock(String wid) {
//		Integer oldClock = clockCache.withAsync().get(wid);
//		if (oldClock == null) {
//			clockCache.withAsync().put(wid, 1);
//		} else {
//			clockCache.withAsync().replace(wid, oldClock + 1);
//		}
////		if (log.isInfoEnabled()) {
////			log.info("Worker " + wid + " is at clock " + clockCache.get(wid));
////		}
//	}

//	public void clock(int wid) {
//		clock(Integer.toString(wid));
//	}

	@Override
	public void shutDown() {
		log.info("Stopping parameter server ...");
		Ignition.stopAll(true);
		ignite.close();
		log.info("Parameter server successfully stopped.");
	}
}
