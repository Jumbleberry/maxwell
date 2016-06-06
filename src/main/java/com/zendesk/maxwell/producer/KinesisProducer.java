package com.zendesk.maxwell.producer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

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
    private final com.amazonaws.services.kinesis.producer.KinesisProducer kinesis;
    private final HashMap<String, LinkedBlockingQueue<RowMap>> queue;
    private final ConcurrentHashMap<String, RowMap> inFlight;
    private int queueSize;
    private String streamName;

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
        this.queue = new HashMap<String, LinkedBlockingQueue<RowMap>>();
        this.queueSize = 0;
        this.inFlight = new ConcurrentHashMap<String, RowMap>();

        this.streamName = kinesisStreamName;
    }

    @Override
    public void push(RowMap r) throws Exception {
        // Get partition key
        String key = r.getTable() + r.pkAsConcatString();

        // Initialize list if none exist
        // and add to in flight queue
        if (! queue.containsKey(key)) {
            queue.put(key, new LinkedBlockingQueue<RowMap>());
            addToInFlight(key, r);
        }
        
        addToQueue(key, r);
    }

    protected void addToQueue(String key, RowMap r) throws Exception {
        queue.get(key).add(r);
        ++queueSize;
    }

    protected void removeFromQueue(String key, RowMap r) throws Exception {
        queue.get(key).remove(r);
        --queueSize;
    }

    protected RowMap getNextInQueue(String key, RowMap r) throws InterruptedException {
        LinkedBlockingQueue<RowMap> list = queue.get(key);
        list.take();
        --queueSize;
        return list.peek();
    }

    protected void addToInFlight(String key, RowMap r) throws Exception {
        inFlight.put(key, r);
        pushToKinesis(key, r);
    }
    
    protected void removeFromInFlight(String key, RowMap r) throws Exception {
        inFlight.remove(key);
        context.setPosition(r);
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
                        RowMap next = getNextInQueue(key, r);
                        if (next != null) {
                    	   addToInFlight(key, next);
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
