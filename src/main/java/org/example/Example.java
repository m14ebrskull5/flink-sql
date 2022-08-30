package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Example {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setString("state.backend", "filesystem");
		conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
		conf.setString("execution.checkpointing.interval", "10s");
		conf.setString(
				"execution.checkpointing.externalized-checkpoint-retention",
				"RETAIN_ON_CANCELLATION");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf).setParallelism(1);

		DataStream<String> control = env
				.fromElements("DROP", "IGNORE","1", "2", "3","4")
				.keyBy(x -> x);

		DataStream<String> streamOfWords = env
				.fromElements("Apache", "DROP", "Flink", "IGNORE")
				.keyBy(x -> x);

		control
				.connect(streamOfWords)
				.flatMap(new ControlFunction())
				.print();

		env.execute();

	}
	public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
		private ValueState<Boolean> blocked;

		@Override
		public void open(Configuration config) {
			blocked = getRuntimeContext()
					.getState(new ValueStateDescriptor<>("blocked", Boolean.class));
		}

		@Override
		public void flatMap1(String control_value, Collector<String> out) throws Exception {
//			System.out.printf("blocked %s\n", control_value);
			blocked.update(Boolean.TRUE);
		}

		@Override
		public void flatMap2(String data_value, Collector<String> out) throws Exception {
			if (blocked.value() == null) {
//				System.out.println(data_value);
				out.collect(data_value);
			}
		}
	}
	public static class Person {
		public String name;
		public Integer age;
		public Person() {}

		public Person(String name, Integer age) {
			this.name = name;
			this.age = age;
		}

		public String toString() {
			return this.name.toString() + ": age " + this.age.toString();
		}
	}
}