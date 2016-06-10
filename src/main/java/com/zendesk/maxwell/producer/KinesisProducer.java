package com.zendesk.maxwell.producer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

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

public class KinesisProducer extends AbstractProducer {

    static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
    protected final com.amazonaws.services.kinesis.producer.KinesisProducer kinesis;

    protected final ConcurrentHashMap<String, LinkedBlockingQueue<RowMap>> queue;
    protected int queueSize;
    protected final int maxQueueSize = 10000;
    protected final Object lock;
    protected final ConcurrentHashMap<String, RowMap> inFlight;

    protected String streamName;
    protected ConcurrentLinkedHashMap<BinlogPosition, Rows> positions;
    protected final int maxPositionSize = 10000;
    protected ConcurrentHashMap<RowMap, Integer> attempts;

    public class Rows {
		public RowMap rowMap;
        public int count;
        
        public Rows(RowMap r, int i) {
			this.rowMap = r;
			this.count = i;
		}
    }

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

        // Set up AWS system properties
        System.setProperty("aws.accessKeyId", kinesisAccessKeyId);
        System.setProperty("aws.secretKey", kinesisSecretKey);

        // Set up AWS producer
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
           .setRecordMaxBufferedTime(kinesisMaxBufferedTime)
           .setMaxConnections(kinesisMaxConnections)
           .setRequestTimeout(kinesisRequestTimeout)
           .setRegion(kinesisRegion)
           .setCredentialsProvider(new SystemPropertiesCredentialsProvider());
        
        this.kinesis = new com.amazonaws.services.kinesis.producer.KinesisProducer(config);
        
        // Set up message queue
        this.queue = new ConcurrentHashMap<String, LinkedBlockingQueue<RowMap>>();
        this.queueSize = 0;
        this.inFlight = new ConcurrentHashMap<String, RowMap>();
        this.lock = new Object();

        this.streamName = kinesisStreamName;
        Builder<BinlogPosition, Rows> builder = new Builder<BinlogPosition,Rows>();
        this.positions = builder.maximumWeightedCapacity(this.maxPositionSize).build();
        this.attempts = new ConcurrentHashMap<RowMap, Integer>();
    }

    @Override
    public void push(RowMap r) throws Exception {
        // Get partition key
        String key = DigestUtils.sha256Hex(r.getTable() + r.pkAsConcatString());

        LinkedBlockingQueue<RowMap> localQueue = queue.get(key);
        if (localQueue == null) 
            localQueue = new LinkedBlockingQueue<RowMap>();
       
        synchronized (localQueue) {
            queue.putIfAbsent(key, localQueue);
            addToQueue(key, r);
            
            if (localQueue.size() == 1)
                addToInFlight(key, r);
        }
    }

    protected void addToQueue(String key, RowMap r) throws Exception {
    	if (queueSize > maxQueueSize) {
    		synchronized(lock) {
        		lock.wait();
    		}
    	}
        queue.get(key).add(r);
        ++queueSize;

        // Add to binlog position tracker if its new
        BinlogPosition newPosition = r.getPosition();
        if (! positions.containsKey(newPosition)) {
            positions.put(newPosition, new Rows(r, r.getAssociatedRows()));
        }
    }

    protected RowMap getNextInQueue(String key, RowMap r) throws InterruptedException {
        LinkedBlockingQueue<RowMap> list = queue.get(key);
        list.take();
        --queueSize;
        synchronized(lock) {
        	lock.notifyAll();
        }
        return list.peek();
    }

    protected void addToInFlight(String key, RowMap r) throws Exception {
        inFlight.put(key, r);
        pushToKinesis(key, r);
    }
    
    protected void removeFromInFlight(String key, RowMap r) throws Exception {
        inFlight.remove(key);
        
        synchronized(positions) {
            updateMinBinlogPosition(r);
        }
    }

    protected void updateMinBinlogPosition(RowMap r) throws Exception{

        BinlogPosition newPosition = r.getPosition();

        if (positions.containsKey(newPosition)) {
            --positions.get(newPosition).count;
        } else {
            LOGGER.error("Missing binlog position in Kinesis Producer.");
        }

        BinlogPosition minBinlogPosition = null;

        // Get the latest minimum binlog position
        int index = 0;
        int zeroCount = 0;
        for (Map.Entry<BinlogPosition,Rows> entry : positions.entrySet()) {
        	// Check if all row events of the current position has been successfully processed
            if (entry.getValue().count == 0) {
                if (index == zeroCount++) {
                    // Update new minimum position and remove it from positions
                    minBinlogPosition = entry.getKey();
                    positions.remove(entry.getKey());
                } else {
                    break;
                }
            }
            
            ++index;
            if (index > zeroCount){
            	break;
            }
        }

        if (minBinlogPosition != null && positions.containsKey(minBinlogPosition)) {
            context.setPosition(positions.get(minBinlogPosition).rowMap);
        }
    }

    protected void pushToKinesis(String key, RowMap r) throws Exception {
        
        ByteBuffer data = ByteBuffer.wrap(r.toAvro().toByteArray());

        FutureCallback<UserRecordResult> callBack = 
            (new FutureCallback<UserRecordResult>() {
                protected String key;
                protected RowMap r;
            
                @Override public void onFailure(Throwable t) { 
                    System.out.println("Failure: " + t.toString());
                     if (attempts.containsKey(this.r)) {
                    	 int attemptCount = attempts.get(this.r);
                    	 attempts.put(this.r, attemptCount+1);
                     } else {
                    	 attempts.put(this.r, 1);
                     }
                     
                     if (attempts.get(this.r) > 3) {
                    	 // Error after three tries
                    	 LOGGER.error("Exception during put", t);
                     } else {
                    	 // Re-try
                    	 try {
							addToQueue(this.key, this.r);
						} catch (Exception e1) {
                            LOGGER.error("Exception during re-try: failling add back to queue.");
						}
                     }
                };

                @Override 
                public void onSuccess(UserRecordResult result) {
                    System.out.println("Success: " + result.toString());
                    try {
                        removeFromInFlight(key, r);
                        LinkedBlockingQueue<RowMap> localQueue = queue.get(key);
                        
                        synchronized (localQueue) {
                            RowMap next = getNextInQueue(key, r);
                            
                            if (next != null) {
                                addToInFlight(key, next);
                            } else {
                                queue.remove(key);
                            }
                            
                            if (attempts.containsKey(this.r)) {
                            	attempts.remove(this.r);
                            }
                        }
                        
                    } catch (Exception e) {
                        LOGGER.error("Exception during Kinesis callback on success.");
                    }
                };
                
                public FutureCallback<UserRecordResult> setUp(String key, RowMap r) {
                    this.key = key;
                    this.r = r;
                    return this;
                }
            }).setUp(key, r);

        ListenableFuture<UserRecordResult> response =
                this.kinesis.addUserRecord(streamName, key, data);

        Futures.addCallback(response, callBack);
    }
}
