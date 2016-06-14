package com.zendesk.maxwell.producer;

import java.util.Observable;
import java.util.Observer;

import com.jumbleberry.kinesis.ConsulLock;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.producer.AbstractProducer;

public class StdoutProducer extends AbstractProducer implements Observer  {
	public StdoutProducer(MaxwellContext context) {		
		super(context);
		ConsulLock.addObserver(this);
	}
	
	@Override
	public void update(Observable observable, Object arg) {
		System.out.println("Bye");
		System.exit(1);		
	}

	@Override
	public void push(RowMap r) throws Exception {
//		System.out.println(r.toJSON());					
		
		int attempts = 0;
		while(true) {
			System.out.println("Doing something... " + ++attempts);
			
			Thread.sleep(5000);
		}
		
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
