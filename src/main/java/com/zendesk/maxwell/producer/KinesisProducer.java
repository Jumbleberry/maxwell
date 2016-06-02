package com.zendesk.maxwell.producer;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.google.common.io.Files;

public class KinesisProducer extends AbstractProducer {

    static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
    static final AtomicLong counter = new AtomicLong();
    private final com.amazonaws.services.kinesis.producer.KinesisProducer kinesis;
    private final HashMap<String, LinkedBlockingQueue<RowMap>> messageQueue;
    private int messageQueueSize;
    private String[] shards;

    public KinesisProducer(
            MaxwellContext context,
            String kinesisAccessKeyId,
            String kinesisSecretKey,
            int kinesisMaxBufferedTime,
            int kinesisMaxConnections,
            int kinesisRequestTimeout,
            String kinesisRegion,
            String kinesisShards
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
        this.messageQueueSize = 0;
        this.shards = kinesisShards.split(";");
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
        ++this.messageQueueSize;

        // If this is the only element in list
        if (list.size() == 1) {
            // pushToKinesis(getShard(r.getTable()), key, r);
            System.out.println("Shard name is " + getShard(r.getTable()));
        }
    }

    /**
     * Get shard by table name
     * The default shard is other
     * 
     * @param  String   table
     * @return String   shardName
     */
    private String getShard(String table)
    {
        if (shards == null) {
            return "other";
        }

        for ( String shard : shards ) {
            String shardName = shard.split(":")[0];
            String tableName = shard.split(":")[1];
            if (table == tableName) {
                return shardName;
            }
        }

        return "other";
    }
    
    private void pushToKinesis(String shard, String partitionKey, RowMap r) throws Exception {
    	// Convert RowMap to Avro here
		byte[] avro = r.toAvro();

    	// Use addUserRecord to push data to Kinesis
		// TODO: send it to Kinesis
		File output = new File("/tmp/test.avro");
		Files.write(avro, output);		
		
    	// Upon success, this.context.setPosition(r);
    	// Upon failure, get minimum position of binlog and re-try
    }
}
