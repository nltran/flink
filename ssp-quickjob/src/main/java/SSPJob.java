/**
 * Created by nltran on 28/04/15.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ps.impl.ParameterElementImpl;
import org.apache.flink.ps.impl.ParameterServerIgniteImpl;
import org.apache.flink.ps.model.ParameterElement;
import org.apache.flink.ps.model.ParameterServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SSPJob{



	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
//		String CACHE_NAME = "testCache";
//		CacheConfiguration<String, ParameterElement> parameterCacheCfg = new CacheConfiguration<String, ParameterElement>();
//		parameterCacheCfg.setCacheMode(CacheMode.PARTITIONED);
//		parameterCacheCfg.setName(CACHE_NAME + "_parameter");
//
//		CacheConfiguration<String, Integer> clockCacheCfg = new CacheConfiguration<String, Integer>();
//		clockCacheCfg.setCacheMode(CacheMode.PARTITIONED);
//		clockCacheCfg.setName(CACHE_NAME + "_clock");
//		Ignite ignite = Ignition.start("examples/config/example-cache.xml");
//		IgniteCache<String, ParameterElement> parameterCache = ignite.getOrCreateCache(parameterCacheCfg);
//		IgniteCache<String, Integer> clockCache = ignite.getOrCreateCache(clockCacheCfg);

//		ParameterServerIgniteImpl ps = new ParameterServerIgniteImpl();
		System.out.println("SSP mock job");
		List<Integer> mockValues = Arrays.asList(1,2,3,4,5);

		DataSet<Integer> set = env.fromCollection(mockValues);

		//initial set
		IterativeDataSet<Integer> loop = set.iterate(10);
		//step function
		DataSet<Integer> newMockValues =  loop.map(new incrementer());
		//
		DataSet<Integer> finalMockValues = loop.closeWith(newMockValues);

		finalMockValues.print();

		env.execute("yolo");

	}

	public static final class incrementer extends RichMapFunction<Integer, Integer> {
		public static Random random = new Random();
		private ParameterServer psInstance = null;
		private int wid;
		private boolean firstIter = true;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			wid = getRuntimeContext().getIndexOfThisSubtask();
			if(firstIter){
//				psInstance = new ParameterServerIgniteImpl();

//				IgniteConfiguration cfg1 = new IgniteConfiguration();
//				cfg1.setGridName(Integer.toString(wid));
//				cfg1.setPeerClassLoadingEnabled(true);
//				Ignite ignite1 = Ignition.start(cfg1);

				psInstance = new ParameterServerIgniteImpl(Integer.toString(wid));

				System.out.println("This should be executed only once");
				firstIter = false;
			}
			System.out.println("This should be executed at each iteration");
//			psInstance = ParameterServerIgniteImpl.getInstance();
//			if(psInstance ==null) {
//				psInstance = new ParameterServerIgniteImpl();
//			}
		}

		@Override
		public Integer map(Integer value) throws Exception {
			Thread.sleep(random.nextInt(50));
			int result = value + 1;


//			Ignite ignite = Ignition.ignite();
//			CacheConfiguration<String, ParameterElement> parameterCacheCfg = new CacheConfiguration<String, ParameterElement>();
//			parameterCacheCfg.setName("testCache" + "_parameter");
//
//			IgniteCache<String, ParameterElement> parameterCache = ignite.getOrCreateCache(parameterCacheCfg);
//			parameterCache.put("yolo",new ParameterElementImpl<String>(5, "tucrains"));

//			psInstance.update(Integer.toString(wid),new ParameterElementImpl<Integer>(5,value));

			return result;
		}

		@Override
		public void close() throws Exception {
			super.close();
//			psInstance.clock(wid);

		}
	}

}
