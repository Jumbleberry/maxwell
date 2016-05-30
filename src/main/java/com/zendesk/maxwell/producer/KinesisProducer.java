package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.producer.AbstractProducer;

public class KinesisProducer extends AbstractProducer {

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

        // TODO
        // Build kinesis producer
    }

    @Override
    public void push(RowMap r) throws Exception {
        System.out.println("This comes from Kinesis producer");
        System.out.println(r.toJSON());
        this.context.setPosition(r);
    }
}
