package com.zendesk.maxwell.producer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

import com.timgroup.statsd.NonBlockingStatsDClient;

public class KinesisProducer extends AbstractProducer {

	static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
	protected final com.amazonaws.services.kinesis.producer.KinesisProducer kinesis;

	protected final int maxSize = (int) Math.pow(2, 14);
	protected final int maxTransactions = Math.floorDiv(maxSize, 4);
	protected final String streamName;

	protected final AtomicInteger keys = new AtomicInteger(0);

	protected final Semaphore queueSize;
	protected final ConcurrentHashMap<String, LinkedBlockingQueue<RowMap>> queue;
	protected final ConcurrentHashMap<String, RowMap> inFlight;

	protected final ConcurrentLinkedHashMap<BinlogPosition, RowMapContext> positions;
	protected final ConcurrentHashMap<RowMap, Integer> attempts;

	protected BinlogPosition mostRecentPosition;

	protected final NonBlockingStatsDClient statsd;

	private class RowMapContext {
		public AtomicInteger remainingRows;
		public boolean isTxCommit;
		public long Xid;

		public RowMapContext(RowMap r) {
			this.remainingRows = new AtomicInteger(r.getEffectedRows());
			this.isTxCommit = r.isTXCommit();
			
			if (r.getXid() != null)
				this.Xid = r.getXid();
		}
	}

	public KinesisProducer(
			MaxwellContext context,
			String kinesisAccessKeyId,
			String kinesisSecretKey,
			int kinesisMaxBufferedTime,
			int kinesisMaxConnections,
			int kinesisRequestTimeout,
			int kinesisConnectTimeout,
			String kinesisRegion,
			String kinesisStreamName
			) {
		super(context);

		this.statsd = new NonBlockingStatsDClient("com.kinesis", "127.0.0.1", 8125);

		this.streamName = kinesisStreamName;

		// Set up AWS system properties
		System.setProperty("aws.accessKeyId", kinesisAccessKeyId);
		System.setProperty("aws.secretKey", kinesisSecretKey);

		// Set up AWS producer
		KinesisProducerConfiguration config = new KinesisProducerConfiguration()
				.setRecordMaxBufferedTime(kinesisMaxBufferedTime)
				.setMaxConnections(kinesisMaxConnections)
				.setMinConnections(4)
				.setRequestTimeout(kinesisRequestTimeout)
				.setConnectTimeout(kinesisConnectTimeout)
				.setRecordTtl(kinesisRequestTimeout + kinesisConnectTimeout + 1000)
				.setCollectionMaxSize((int) Math.pow(2, 20))
				.setRegion(kinesisRegion)
				.setCredentialsProvider(new SystemPropertiesCredentialsProvider());

		this.kinesis = new com.amazonaws.services.kinesis.producer.KinesisProducer(config);

		// Set up message queue
		this.queueSize = new Semaphore(maxTransactions);
		this.queue = new ConcurrentHashMap<String, LinkedBlockingQueue<RowMap>>(maxSize);
		this.inFlight = new ConcurrentHashMap<String, RowMap>(maxSize);

		Builder<BinlogPosition, RowMapContext> builder = new Builder<BinlogPosition,RowMapContext>();
		this.positions = builder.maximumWeightedCapacity(maxSize).build();
		this.attempts = new ConcurrentHashMap<RowMap, Integer>(maxSize);

		metricsReporter();
	}

	@Override
	public void push(RowMap r) throws Exception {
		try {
			// Get partition key
			String key = DigestUtils.sha256Hex(r.getDatabase() + ":" + r.getTable() + ":" + r.pkAsConcatString());

			if (!r.isHeartbeat()) {

				// Don't allow re-processing of older events
				if (mostRecentPosition != null && mostRecentPosition.newerThan(r.getPosition()))
					return;

				// Keep track of most recent position we've seen
				mostRecentPosition = r.getPosition();
			}

			long startTime = System.currentTimeMillis();

			// index 0 will acquire room in the queue system
			if (r.getIndex() == 0) {
				queueSize.acquire();
				statsd.recordHistogramValue("producer.affected_rows", r.getEffectedRows(), getTags(r));
			}

			// Get an instance of a queue segmented by partition key
			LinkedBlockingQueue<RowMap> localQueue = queue.get(key);
			if (localQueue == null) 
				queue.putIfAbsent(key, localQueue = new LinkedBlockingQueue<RowMap>());

			int localSize;
			synchronized (localQueue) {
				localQueue.add(r);
				localSize = localQueue.size();
			}

			// Record amount of time it took to get the permit
			statsd.recordHistogramValue("producer.wait", System.currentTimeMillis() - startTime);

			if (localSize == 1) {
				statsd.increment("producer.inflight.queue", getTags(r, "type:immediate"));
				pushToKinesis(key, r);
				return;
			}

			statsd.increment("producer.inflight.queue", getTags(r, "type:delayed"));

		} catch (InterruptedException e) {
			// If this thread is interrupted we want to signal to stop
			System.exit(1);
		}
	}

	protected RowMap popAndGetNext(String key, RowMap r) {
		LinkedBlockingQueue<RowMap> localQueue = queue.get(key);
		RowMap next = null;

		try {
			synchronized (localQueue) {
				localQueue.take();
				next = localQueue.peek();
			}

			// Cleanup the queue if we've exhausted all elements
			if (next != null) {
				statsd.increment("producer.inflight.queue", getTags(r, "type:chained"));
				return next;
			}

			queue.remove(key);

		} catch (InterruptedException e) {
			LOGGER.error("Interrupted while removing element from the queue");
			System.exit(1);
		}

		return next;
	}

	protected void addToInFlight(String key, RowMap r) {
		statsd.increment("producer.push", getTags(r));
		attempts.putIfAbsent(r, 0);
		recordBinlogPosition(r);
		inFlight.putIfAbsent(key, r);
	}

	protected void removeFromInFlight(String key, RowMap r) {  
		statsd.increment("producer.pop", getTags(r));
		attempts.remove(r);
		updateBinlogPosition(r);
		inFlight.remove(key);
	}

	protected void recordBinlogPosition(RowMap r) {
		if (!r.isHeartbeat() && !positions.containsKey(r.getPosition()))
			positions.putIfAbsent(r.getPosition(), new RowMapContext(r));
	}

	protected void updateBinlogPosition(RowMap r) {
		boolean isHeartbeat = r.isHeartbeat();

		BinlogPosition minPosition = null;
		BinlogPosition position = r.getPosition();
		int remainingRows = !isHeartbeat? positions.get(position).remainingRows.decrementAndGet(): 0;

		// If remaining rows reaches 0, prune the outstanding records 
		if (remainingRows <= 0) {
			try {
				synchronized (positions) {
					Map<BinlogPosition, RowMapContext> map = positions.ascendingMap();
					for (Map.Entry<BinlogPosition, RowMapContext> entry : map.entrySet()) {
						// If entry is > 0, we're still waiting on elements and can't continue
						if (entry.getValue().remainingRows.get() > 0)
							break;

						// Only save positions for transactional commits so we don't resume mid-transaction 
						if (entry.getValue().isTxCommit)
							minPosition = entry.getKey();

						// Attempt to remove the key from the tracker & release a key if we're the first to remove it
						positions.remove(entry.getKey());
						queueSize.release();
					}

					context.setPosition(minPosition);
				}
			} catch (Exception e) {
				LOGGER.error("Failed to write out binlog position");
				e.printStackTrace();
			}
		}
	}

	protected void pushToKinesis(String key, RowMap r) {
		try {
			// Use JSON instead of AVRO as data format
			// ByteBuffer data = ByteBuffer.wrap(r.toAvro().toByteArray());
			ByteBuffer data = ByteBuffer.wrap(r.toJSON().getBytes("UTF-8"));
			
			statsd.histogram("producer.rowmap.size", data.remaining(), getTags(r));

			addToInFlight(key, r);

			FutureCallback<UserRecordResult> callBack = 
					(new FutureCallback<UserRecordResult>() {
						protected String key;
						protected RowMap r;
						protected long start;

						public FutureCallback<UserRecordResult> setUp(String key, RowMap r) {
							this.key = key;
							this.r = r;
							this.start = System.currentTimeMillis();
							return this;
						}

						public void reportStats(String outcome) {
							statsd.histogram("producer.kpl.latency", System.currentTimeMillis() - start, getTags(r, "status:" + outcome));
						}

						@Override public void onFailure(Throwable t) { 
							reportStats("error");

							int attemptCount = attempts.put(this.r, attempts.get(this.r) + 1);
							if (attemptCount < 3) {
								pushToKinesis(key, r);
								return;
							}

							LOGGER.error("Maximum retry count exceeded");
							System.exit(1);
						};

						@Override 
						public void onSuccess(UserRecordResult result) {
							reportStats("success");

							removeFromInFlight(key, r);
							RowMap next = popAndGetNext(key, r);

							// If there's another element in the queue, send it through
							if (next != null)
								pushToKinesis(key, next);
						};

					}).setUp(key, r);

			// Make sure all records from the same database end up on the same shard
			String partitionKey = r.getDatabase();

			ListenableFuture<UserRecordResult> response =
					this.kinesis.addUserRecord(streamName, partitionKey, data);

			Futures.addCallback(response, callBack);

		} catch (Exception e) {
			LOGGER.error("Failed to serialize to avro." + e.getStackTrace());
			e.printStackTrace();
			System.exit(1);
		}
	}

	public String[] getTags(RowMap r, String... args) {
		String[] tags = {
				"database:" + r.getDatabase(),
				"table:" + r.getTable()
		};

		for (String arg: args)
			tags = (String[]) ArrayUtils.add(tags, arg);

		return tags;
	}

	public void metricsReporter() {
		(new Thread() {
			public void run() {
				for (;;) {
					try {
						statsd.recordGaugeValue("producer.kpl.inflight", kinesis.getOutstandingRecordsCount());
						statsd.recordGaugeValue("producer.inflight", inFlight.size());
						statsd.recordGaugeValue("producer.positions", positions.size());
						statsd.recordGaugeValue("producer.permits.available", queueSize.availablePermits());
						statsd.recordGaugeValue("producer.permits.used", maxTransactions - queueSize.availablePermits());

						Thread.sleep(1000);
					} catch (InterruptedException e) {}
				}
			}
		}).start();
	}
}
