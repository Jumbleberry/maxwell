package com.zendesk.maxwell.producer;

import java.util.Observable;
import java.util.Observer;

import com.jumbleberry.kinesis.ConsulLock;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.producer.AbstractProducer;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellContext context) {		
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {
//		System.out.println(r.toJSON());					
		
		int attempts = 0;
		System.out.println("Doing something... " + ++attempts);
		
    	// Convert RowMap to Avro here
//    	AvroData avroData = r.toAvro();
//
//    	// Use addUserRecord to push data to Kinesis
//		// TODO: send it to Kinesis
//		File output = new File("/tmp/test.avro");
//		Files.write(avroData.toByteArray(), output);			
		
//		this.context.setPosition(r);
	}
}
