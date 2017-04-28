package org.sanju.kafka.connect.arangodb.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.sanju.kafka.connect.arangodb.ArangoDBWriter;
import org.sanju.kafka.connect.arangodb.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class ArangoDBSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBSinkTask.class);
    private int timeout;
    private Writer writer;
    private Map<String, String> config;

    @Override
    public void put(final Collection<SinkRecord> records) {

        if (records.isEmpty()) {
            logger.debug("Empty record collection to process");
            return;
        }

        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        logger.debug("Received {} records. kafka coordinates from record: Topic - {}, Partition - {}, Offset - {}",
                        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

        try {
            writer.write(records);
        } catch (final RetriableException e) {
            logger.warn("Setting the task timeout to {} ms upon RetriableException", timeout);
            this.writer = new ArangoDBWriter(config);
            context.timeout(timeout);
            throw e;
        }
    }

    @Override
    public void start(final Map<String, String> config) {

        logger.info("start called!");
        this.config = config;
        this.timeout = Integer.valueOf(config.get(ArangoDBSinkConfig.RETRY_BACKOFF_MS));
        this.writer = new ArangoDBWriter(config);
    }

    @Override
    public void stop() {

        logger.info("stop called!");
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        currentOffsets.forEach((k, v) -> logger.debug("Flush - Topic {}, Partition {}, Offset {}, Metadata {}",
                k.topic(), k.partition(), v.offset(), v.metadata()));
    }

    public String version() {

        return ArangoDBSinkConnector.ARANGODB_CONNECTOR_VERSION;
    }

}
