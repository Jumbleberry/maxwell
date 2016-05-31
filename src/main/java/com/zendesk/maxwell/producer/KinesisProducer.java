package com.zendesk.maxwell.producer;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

public class KinesisProducer extends AbstractProducer {

    static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
    static final AtomicLong counter = new AtomicLong();
    private final com.amazonaws.services.kinesis.producer.KinesisProducer kinesis;
    private final HashMap<String, LinkedBlockingQueue<RowMap>> messageQueue;

    public KinesisProducer(
            MaxwellContext context,
            String kinesisAccessKeyId,
            String kinesisSecretKey,
            int kinesisMaxBufferedTime,
            int kinesisMaxConnections,
            int kinesisRequestTimeout,
            String kinesisRegion
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
           .setRegion(kinesisRegion);
        this.kinesis = new com.amazonaws.services.kinesis.producer.KinesisProducer(config);
        
        // Set up message queue
        this.messageQueue = new HashMap<String, LinkedBlockingQueue<RowMap>>();
    }

    @Override
    public void push(RowMap r) throws Exception {
        // Get partition key
        String key = r.getTable() + r.pkAsConcatString();
        // Initialize list if none exist
        if (! messageQueue.containsKey(key)) {
        	messageQueue.put(key, new LinkedBlockingQueue<RowMap>());
        }
    	// Add to it's own list in the message queue
        LinkedBlockingQueue<RowMap> list = messageQueue.get(key);
        list.add(r);
    }
    
    private void pushToKinesis() throws Exception {
    	// Convert RowMap to Avro here
    	// Use addUserRecord to push data to Kinesis
    	// Upon success, this.context.setPosition(r);
    	// Upon failure, get minimum position of binlog and re-try
    }
}
