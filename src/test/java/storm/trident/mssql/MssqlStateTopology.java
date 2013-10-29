package storm.trident.mssql;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.StateType;
import storm.trident.state.mssql.MssqlState;
import storm.trident.state.mssql.MssqlStateConfig;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import clojure.lang.Numbers;

import com.google.common.collect.Lists;

public class MssqlStateTopology {

	@SuppressWarnings("serial")
	static class RandomTupleSpout implements IBatchSpout {
		private transient Random random;
		private static final int BATCH = 500; // Keep in mind that the JTDS driver's maximum number of parameter markers is 2000
		
		@Override
		@SuppressWarnings("rawtypes")
		public void open(final Map conf, final TopologyContext context) {
			random = new Random();
		}

		@Override
		public void emitBatch(final long batchId, final TridentCollector collector) {
			// emit a 3 number tuple (a,b,c)
			for (int i = 0; i < BATCH; i++) {
				collector.emit(new Values(random.nextInt(1000) + 1, random.nextInt(100) + 1, random.nextInt(100) + 1));
			}
		}

		@Override
		public void ack(final long batchId) {}

		@Override
		public void close() {}

		@Override
		@SuppressWarnings("rawtypes")
		public Map getComponentConfiguration() {
			return null;
		}

		@Override
		public Fields getOutputFields() {
			return new Fields("a", "b", "c");
		}
	}

	@SuppressWarnings("serial")
	static class ThroughputLoggingFilter extends BaseFilter {

		private static final Logger logger = Logger.getLogger(ThroughputLoggingFilter.class);
		private long count = 0;
		private Long start = System.nanoTime();
		private Long last = System.nanoTime();

		public boolean isKeep(final TridentTuple tuple) {
			count += 1;
			final long now = System.nanoTime();
			if (now - last > 5000000000L) { // emit every 5 seconds
				logger.info("tuples per second = " + (count * 1000000000L) / (now - start));
				last = now;
			}
			return true;
		}
	}

	@SuppressWarnings("serial")
	static class CountSumSum implements CombinerAggregator<List<Number>> {

		@Override
		public List<Number> init(TridentTuple tuple) {
			return Lists.newArrayList(1L, (Number) tuple.getValue(0), (Number) tuple.getValue(1));
		}

		@Override
		public List<Number> combine(List<Number> val1, List<Number> val2) {
			return Lists.newArrayList(Numbers.add(val1.get(0), val2.get(0)), Numbers.add(val1.get(1), val2.get(1)), Numbers.add(val1.get(2), val2.get(2)));
		}

		@Override
		public List<Number> zero() {
			return Lists.newArrayList((Number) 0, (Number) 0, (Number) 0);
		}

	}

	/**
	 * storm local cluster executable
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		/*
		   Create a table like this:
		 
			CREATE TABLE TridentState (
			  a int NOT NULL,
			  count int DEFAULT NULL,
			  bsum int DEFAULT NULL,
			  csum int DEFAULT NULL,
			  PRIMARY KEY (a)
			)
		 */
		
		final String dburl = "jdbc:jtds:sqlserver://sqlserver1:1433/myDatabase1;user=username;password=password";
		
		final TridentTopology topology = new TridentTopology();
		
		final GroupedStream stream = topology.newStream("test", new RandomTupleSpout())
				.each(new Fields(), new ThroughputLoggingFilter())
				.groupBy(new Fields("a"));
		
		final MssqlStateConfig config = new MssqlStateConfig();
		{
			config.setUrl(dburl);
			config.setTable("state_default");
			config.setKeyColumns(new String[] { "a" });
			config.setValueColumns(new String[] { "count", "bsum", "csum" });
			config.setType(StateType.NON_TRANSACTIONAL);
			config.setCacheSize(1000);
		}
		stream.persistentAggregate(MssqlState.newFactory(config), new Fields("b", "c"), new CountSumSum(), new Fields("sum"));

		final MssqlStateConfig config1 = new MssqlStateConfig();
		{
			config1.setUrl(dburl);
			config1.setTable("state_transactional");
			config1.setKeyColumns(new String[] { "a" });
			config1.setValueColumns(new String[]{ "count", "bsum", "csum" });
			config1.setType(StateType.TRANSACTIONAL);
			config1.setCacheSize(1000);
		}
		stream.persistentAggregate(MssqlState.newFactory(config1), new Fields("b", "c"), new CountSumSum(), new Fields("sum"));
		
		final MssqlStateConfig config2 = new MssqlStateConfig();
		{
			config2.setUrl(dburl);
			config2.setTable("state_opaque");
			config2.setKeyColumns(new String[] { "a" });
			config2.setValueColumns(new String[] { "count", "bsum", "csum" });
			config2.setType(StateType.OPAQUE);
			config2.setCacheSize(1000);
		}
		stream.persistentAggregate(MssqlState.newFactory(config2), new Fields("b", "c"), new CountSumSum(), new Fields("sum"));

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", new Config(), topology.build());
		while (true) {

		}
	}
}
