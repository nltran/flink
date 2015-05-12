package org.apache.flink.ps.impl;

import org.apache.flink.ps.model.ParameterElement;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Created by nltran on 08/05/15.
 */
public class ParameterServerClientImpl {

	private Ignite ignite;
	public IgniteCache<String, ParameterElement> parameterCache;

	ParameterServerClientImpl()  {
		int ignites = Ignition.allGrids().size();
		System.out.println("Ignition.allGrids().size() = " + Ignition.allGrids().size());
		if (ignites == 0) {
//			Ignition.setClientMode(true);
			this.ignite = Ignition.start("examples/config/example-cache.xml");
		}
		else {
//			Ignition.setClientMode(true);
			this.ignite = Ignition.ignite();

		}

		CacheConfiguration<String, ParameterElement> parameterCacheCfg = new CacheConfiguration<String, ParameterElement>();
		parameterCacheCfg.setName(ParameterServerIgniteImpl.CACHE_NAME + "_parameter");
		parameterCache = ignite.getOrCreateCache(parameterCacheCfg);

	}



	public static void main(String[] args) {
//		Ignition.setClientMode(true);
//		Ignite ignite = Ignition.start("examples/config/example-cache.xml");
//		Ignite ignite = Ignition.ignite();



//		IgniteCache<String, ParameterElement> parameterCache = ignite.cache(ParameterServerIgniteImpl.CACHE_NAME);

//		ParameterServerClientImpl psc = new ParameterServerClientImpl();
//
//		psc.parameterCache.put("test", new ParameterElementImpl<String>(8, "vecline"));
//		ParameterElement t = psc.parameterCache.get("test");
//		ParameterElement tt = psc.parameterCache.get("vecline2");
//		System.out.println(t.getValue());
//		System.out.println(tt.getValue());


		String CACHE_NAME = "testCache";
		CacheConfiguration<String, ParameterElement> parameterCacheCfg = new CacheConfiguration<String, ParameterElement>();
		parameterCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		parameterCacheCfg.setName(CACHE_NAME + "_parameter");

		CacheConfiguration<String, Integer> clockCacheCfg = new CacheConfiguration<String, Integer>();
		clockCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		clockCacheCfg.setName(CACHE_NAME + "_clock");
		Ignite ignite = Ignition.start("examples/config/example-cache.xml");
		IgniteConfiguration cfg1 = new IgniteConfiguration();
		cfg1.setGridName("name1");
		cfg1.setPeerClassLoadingEnabled(true);
		Ignite ignite1 = Ignition.start(cfg1);
//		Ignite ignite2 = Ignition.start("examples/config/example-cache.xml");
//		Ignite ignite = Ignition.ignite();
		IgniteCache<String, ParameterElement> parameterCache = ignite.getOrCreateCache(parameterCacheCfg);
		IgniteCache<String, Integer> clockCache = ignite.getOrCreateCache(clockCacheCfg);


//		Ignite ignite = Ignition.ignite();
//		CacheConfiguration<String, ParameterElement> parameterCacheCfg = new CacheConfiguration<String, ParameterElement>();
//		parameterCacheCfg.setName("testCache" + "_parameter");


		parameterCache.put("yolo",new ParameterElementImpl<String>(5, "tucrains"));



	}

}
