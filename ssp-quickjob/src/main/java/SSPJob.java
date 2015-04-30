/**
 * Created by nltran on 28/04/15.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

import java.util.Arrays;
import java.util.List;

public class SSPJob{



	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

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

	public static final class incrementer implements MapFunction<Integer, Integer> {

		@Override
		public Integer map(Integer value) throws Exception {
			return value + 1;
		}
	}

}
