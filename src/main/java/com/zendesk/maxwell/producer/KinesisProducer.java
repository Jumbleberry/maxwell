package com.zendesk.maxwell.producer;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

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

public class KinesisProducer extends AbstractProducer {

    static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
    static final AtomicLong counter = new AtomicLong();
    protected final com.amazonaws.services.kinesis.producer.KinesisProducer kinesis;

    protected final ConcurrentHashMap<String, LinkedBlockingQueue<RowMap>> queue;
    protected int queueSize;
    protected final int maxQueueSize = 10000;
    protected Object lock;
    protected final ConcurrentHashMap<String, RowMap> inFlight;

    protected String streamName;
    protected ConcurrentHashMap<BinlogPosition, Rows> positions;

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

        this.streamName = kinesisStreamName;
        this.positions = new ConcurrentHashMap<BinlogPosition, Rows>();
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
    		lock.wait();
    	}
        queue.get(key).add(r);
        ++queueSize;
    }

    protected RowMap getNextInQueue(String key, RowMap r) throws InterruptedException {
        LinkedBlockingQueue<RowMap> list = queue.get(key);
        list.take();
        --queueSize;
        lock.notifyAll();
        return list.peek();
    }

    protected void addToInFlight(String key, RowMap r) throws Exception {
        inFlight.put(key, r);
        pushToKinesis(key, r);
    }
    
    protected void removeFromInFlight(String key, RowMap r) throws Exception {
        inFlight.remove(key);
        updateMinBinlogPosition(r);
    }

    protected void updateMinBinlogPosition(RowMap r) throws SQLException{

        BinlogPosition newPosition = r.getPosition();

        if (positions.containsKey(newPosition)) {
            --positions.get(newPosition).count;
        } else {
            positions.put(newPosition, new Rows(r, r.getAssociatedRows()-1));
        }

        BinlogPosition minBinlogPosition = null;

        // Get the min binlog position
        // In order check how many position has count of 0
        // The order is very important
        // Set new position and remove from positions hash map

        if (minBinlogPosition != null) {
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
                    // Upon failure, get minimum position of binlog and re-try
                    // TODO
                    System.out.println("Failed:" + t.toString());

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
                        }
                        
                    } catch (Exception e) {
                        e.printStackTrace();
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
