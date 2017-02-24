package com.science.test;

import com.lmax.disruptor.*;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.science.util.ScienceUtil;
import org.apache.commons.lang.math.RandomUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BatchTest {
	private RingBuffer<List<String>> ringBuffer;

	public static void main(String[] args) throws SQLException {
		BatchTest test = new BatchTest();
		int threadnum = Integer.parseInt(args[0]);
		final int recordsPerThread = Integer.parseInt(args[1]);// 总记录数
		final int numPerCommit = Integer.parseInt(args[2]);// 每次提交记录数
		final EventFactory<List<String>> EVENT_FACTORY = new EventFactory<List<String>>() {
			public List<String> newInstance() {
				return new ArrayList<>();
			}
		};
		ExecutorService executor = Executors.newFixedThreadPool(threadnum, DaemonThreadFactory.INSTANCE);
		test.ringBuffer = RingBuffer.createSingleProducer(EVENT_FACTORY, 1024, new YieldingWaitStrategy());
		DisruptorHandler[] handlers = new DisruptorHandler[threadnum];
		for (int i = 0; i < threadnum; i++) {
			handlers[i] = new DisruptorHandler();
		}
		WorkerPool<List<String>> workerPool = new WorkerPool<List<String>>(test.ringBuffer,
				test.ringBuffer.newBarrier(), new FatalExceptionHandler(), handlers);

		workerPool.start(executor);
		test.ringBuffer.addGatingSequences(workerPool.getWorkerSequences());
		ScienceUtil.init();
		for (int i = 0; i < recordsPerThread / numPerCommit; i++) {
			test.productData(numPerCommit);
		}
	}

	private void productData(int numPerCommit) {
		long sequence = this.ringBuffer.next();
		List<String> list = this.ringBuffer.get(sequence);
		long batch = RandomUtils.nextInt();
		for (int i = 0; i < numPerCommit; i++) {
			String sql = "UPSERT INTO user(id, firstname,lastname) values('" + batch + ":" + i + "','zjh','zhou')";
			list.add(sql);
		}
		this.ringBuffer.publish(sequence);
	}

	public static class DisruptorHandler implements WorkHandler<List<String>> {
		final Connection conn;
		final Statement stmt;

		public DisruptorHandler() throws SQLException {
			conn = DriverManager.getConnection("jdbc:phoenix:10.253.11.207,10.139.113.47,10.253.12.4:2181");
			stmt = conn.createStatement();
		}

		@Override
		public void onEvent(List<String> event) throws Exception {
			for (String sql : event) {
				stmt.execute(sql);
			}
			conn.commit();
		}
	}

}
