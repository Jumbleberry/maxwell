package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;
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
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

public class KinesisProducer extends AbstractProducer {

	static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
	protected final com.amazonaws.services.kinesis.producer.KinesisProducer kinesis;

	protected final int maxSize = (int) Math.pow(2, 16);
	protected final String streamName;

	protected final AtomicInteger keys = new AtomicInteger(0);

	protected final Semaphore queueSize;
	protected final ConcurrentHashMap<String, LinkedBlockingQueue<RowMap>> queue;
	protected final ConcurrentHashMap<String, RowMap> inFlight;

	protected final ConcurrentLinkedHashMap<BinlogPosition, AtomicInteger> positions;
	protected final ConcurrentHashMap<RowMap, Integer> attempts;

	public KinesisProducer(
			MaxwellContext context,
			String kinesisAccessKeyId,
			String kinesisSecretKey,
			int kinesisMaxBufferedTime,
			int kinesisMaxConnections,
			int kinesisRequestTimeout,
			String kinesisRegion,
			String kinesisStreamName
			) {
		super(context);

		this.streamName = kinesisStreamName;

		// Set up AWS system properties
		System.setProperty("aws.accessKeyId", kinesisAccessKeyId);
		System.setProperty("aws.secretKey", kinesisSecretKey);

		// Set up AWS producer
		KinesisProducerConfiguration config = new KinesisProducerConfiguration()
				.setRecordMaxBufferedTime(kinesisMaxBufferedTime)
				.setMaxConnections(kinesisMaxConnections)
				.setMinConnections(8)
				.setRequestTimeout(kinesisRequestTimeout)
				.setRecordTtl(kinesisRequestTimeout * 2 * 3)
				.setRegion(kinesisRegion)
				.setCredentialsProvider(new SystemPropertiesCredentialsProvider());

		this.kinesis = new com.amazonaws.services.kinesis.producer.KinesisProducer(config);

		// Set up message queue
		this.queueSize = new Semaphore((int) (maxSize / 4));
		this.queue = new ConcurrentHashMap<String, LinkedBlockingQueue<RowMap>>(maxSize);
		this.inFlight = new ConcurrentHashMap<String, RowMap>(maxSize);

		Builder<BinlogPosition, AtomicInteger> builder = new Builder<BinlogPosition,AtomicInteger>();
		this.positions = builder.maximumWeightedCapacity(maxSize).build();
		this.attempts = new ConcurrentHashMap<RowMap, Integer>(maxSize);
	}

	@Override
	public void push(RowMap r) throws Exception {
		try {
			// index 0 will acquire room in the queue system
			if (r.getIndex() == 0)
				queueSize.acquire();
			
			// Get partition key
			String key = DigestUtils.sha256Hex(r.getTable() + r.pkAsConcatString());

			// Get an instance of a queue segmented by partition key
			LinkedBlockingQueue<RowMap> localQueue = queue.get(key);
			if (localQueue == null) 
				localQueue = new LinkedBlockingQueue<RowMap>();

			synchronized (localQueue) {
				localQueue.add(r);
				queue.putIfAbsent(key, localQueue);

				if (localQueue.size() == 1)
					pushToKinesis(key, r);
			}
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

				// Cleanup the queue if we've exhausted all elements
				if (next == null)
					queue.remove(key);

			}
		} catch (InterruptedException e) {
			LOGGER.error("Interrupted while removing element from the queue");
			System.exit(1);
		}

		return next;
	}

	protected void addToInFlight(String key, RowMap r) {
		attempts.putIfAbsent(r, 0);
		recordBinlogPosition(r);
		inFlight.putIfAbsent(key, r);
	}

	protected void removeFromInFlight(String key, RowMap r) {    	
		attempts.remove(r);
		updateBinlogPosition(r);
		inFlight.remove(key);
	}

	protected void recordBinlogPosition(RowMap r) {
		if (!r.isHeartbeat())
			positions.putIfAbsent(r.getPosition(), new AtomicInteger(r.getEffectedRows()));
	}

	protected void updateBinlogPosition(RowMap r) {
		boolean isHeartbeat = r.isHeartbeat();

		BinlogPosition position = r.getPosition();
		int remainingRows = !isHeartbeat? positions.get(position).decrementAndGet(): 0;
		BinlogPosition minPosition = null;

		// If remaining rows reaches 0, prune the outstanding records 
		if (remainingRows <= 0) {
			try {
				synchronized (positions) {

					Map<BinlogPosition, AtomicInteger> map = positions.ascendingMap();
					for (Map.Entry<BinlogPosition, AtomicInteger> entry : map.entrySet()) {
						// If entry is > 0, we're still waiting on elements and can't continue
						if (entry.getValue().get() > 0)
							break;

						minPosition = entry.getKey();
						if (!isHeartbeat || position != minPosition) {
							positions.remove(minPosition);
							queueSize.release();
						}
					}

					if (minPosition != null)
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
			ByteBuffer data = ByteBuffer.wrap(r.toAvro().toByteArray());
			addToInFlight(key, r);

			FutureCallback<UserRecordResult> callBack = 
					(new FutureCallback<UserRecordResult>() {
						protected String key;
						protected RowMap r;

						public FutureCallback<UserRecordResult> setUp(String key, RowMap r) {
							this.key = key;
							this.r = r;
							return this;
						}

						@Override public void onFailure(Throwable t) { 
							int attemptCount = attempts.put(this.r, attempts.get(this.r) + 1);

							if (attemptCount < 3) {
								LOGGER.error("Failed to push to kinesis: retrying.");
								pushToKinesis(key, r);
								return;
							}

							LOGGER.error("Maximum retry count exceeded");
							System.exit(1);
						};

						@Override 
						public void onSuccess(UserRecordResult result) {
							removeFromInFlight(key, r);
							RowMap next = popAndGetNext(key, r);

							// If there's another element in the queue, send it through
							if (next != null) {
								pushToKinesis(key, next);
							}
						};

					}).setUp(key, r);

			ListenableFuture<UserRecordResult> response =
					this.kinesis.addUserRecord(streamName, key, data);

			Futures.addCallback(response, callBack);

			return;

		} catch (Exception e) {
			LOGGER.error("Failed to serialize to avro." + e.getStackTrace());
			e.printStackTrace();
			System.exit(1);
		}
	}
}
