package org.sanju.kafka.connect.arangodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.sanju.kafka.connect.arangodb.sink.ArangoDBSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;


/**
 * 
 * @author Sanju Thomas
 *
 */
public class ArangoDBWriter implements Writer{
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBWriter.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private final ArangoDB arangoDB;
	private final int batchSize;
	private final String databaseName;
	private final String collectionName;
	
	public ArangoDBWriter(final Map<String, String> config){
	    
	    arangoDB = new ArangoDB.Builder().host(config.get(ArangoDBSinkConfig.ARANAGODB_HOST), 
	            Integer.valueOf(config.get(ArangoDBSinkConfig.ARANAGODB_PORT)))
	            .user(config.get(ArangoDBSinkConfig.CONNECTION_USER))
	            .password(config.get(ArangoDBSinkConfig.CONNECTION_PASSWORD)).build();
	    
	    databaseName = config.get(ArangoDBSinkConfig.DATABASE_NAME);
	    collectionName = config.get(ArangoDBSinkConfig.COLLECTION_NAME);
	    batchSize = Integer.valueOf(config.get(ArangoDBSinkConfig.BATCH_SIZE));
	    
	}
	

    @Override
    public void write(final Collection<SinkRecord> records) {
        
        final List<List<SinkRecord>> partitions = Lists.partition(new ArrayList<>(records), batchSize);
        partitions.forEach(partition ->{
            final List<String> rs = new ArrayList<>();
            partition.forEach(r -> {
                try {
                     rs.add(MAPPER.writeValueAsString(r.value()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            });
            try{
                arangoDB.db(databaseName).collection(collectionName).insertDocuments(rs);
            }catch(ArangoDBException e){
                logger.error("Exception occurred while saving the document to ArangoDB, {}", e);
                throw new RetriableException(e);
            }
        });
    }
}
