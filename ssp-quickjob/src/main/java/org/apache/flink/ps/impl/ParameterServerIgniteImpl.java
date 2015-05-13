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

	//	private static ParameterServerIgniteImpl INSTANCE = null;
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
			cfg1.setGridName(name);
			cfg1.setPeerClassLoadingEnabled(true);
			this.ignite = Ignition.start(cfg1);

//			IgniteConfiguration icfg = new IgniteConfiguration();
//			icfg.setPeerClassLoadingEnabled(true);
//			icfg.setGridName("ParameterServerGrid");
//			icfg.setCacheConfiguration(parameterCacheCfg, clockCacheCfg);


//			int ignites = Ignition.allGrids().size();
//			Ignition.setClientMode(true);
//			Ignition.setDaemon(true);
//			if (ignites == 0) {
//				this.ignite = Ignition.start("examples/config/example-cache.xml");
//				log.info("Starting Ignite");

//			} else {
//			this.ignite = Ignition.start(icfg);
//				log.info("Igniting Ignite");
//			}

//			ignite = Ignition.ignite();


			parameterCache = ignite.getOrCreateCache(parameterCacheCfg);
			clockCache = ignite.getOrCreateCache(clockCacheCfg);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

//	public static ParameterServerIgniteImpl getInstance() {
//		if (INSTANCE == null) {
//			INSTANCE = new ParameterServerIgniteImpl();
//			log.info("instantiating as no instance of PS created yet");
//		}
//		else {
//			log.info("retrieving PS instance");
//		}
//		return INSTANCE;
//	}

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

	public static void main(String[] args) {
//		try {
//			Ignite ignite = Ignition.start("examples/config/example-parameterCache.xml");
//			CacheConfiguration<Integer, String> cfg = new CacheConfiguration<Integer,String>();
//			cfg.setCacheMode(CacheMode.PARTITIONED);
//			cfg.setName(CACHE_NAME);
//			IgniteCache<Integer, String> parameterCache = ignite.createCache(cfg);
////			IgniteCache<Integer, String> parameterCache = ignite.parameterCache(CACHE_NAME);
//
//			// Store keys in parameterCache (values will end up on different parameterCache nodes).
//			for (int i = 0; i < 10; i++)
//				parameterCache.put(i, Integer.toString(i));
//
//			for (int i = 0; i < 10; i++)
//				System.out.println("Got [key=" + i + ", val=" + parameterCache.get(i) + ']');
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//		}

//		ParameterServerIgniteImpl.startParameterServer();
//		ParameterServerIgniteImpl ps = ParameterServerIgniteImpl.getInstance();

//		ParameterServerIgniteImpl ps = new ParameterServerIgniteImpl("test");
//		ps.update("vecline2", new ParameterElementImpl<String>(5, "yolo"));
//		ps.update("1", new ParameterElementImpl<String>(5, "yolo"));
//		ParameterElement<String> test = ps.get("vecline2");
//		System.out.println("test = " + test.getValue());
//
//		ParameterServerClientImpl psc = new ParameterServerClientImpl();
//
//		psc.parameterCache.put("test", new ParameterElementImpl<String>(8, "vecline"));
//		ParameterElement t = psc.parameterCache.get("test");
//		ParameterElement tt = psc.parameterCache.get("vecline2");
//		System.out.println(t.getValue());
//		System.out.println(tt.getValue());

		ParameterServerIgniteImpl ps1 = new ParameterServerIgniteImpl("1");

		ps1.update("worker1",new ParameterElementImpl<String>(1, "salut"));
		ParameterServerIgniteImpl ps2 = new ParameterServerIgniteImpl("2");
		ParameterElement<String> t = ps2.get("worker1");

		System.out.println("ps2 = " + t.getValue());

		ps1.shutDown();
		ps2.shutDown();





//		System.out.println("args = " + ParameterServerIgniteImpl.getInstance().get("vecline").getValue());
//
//		ps.shutDown();


	}
}
