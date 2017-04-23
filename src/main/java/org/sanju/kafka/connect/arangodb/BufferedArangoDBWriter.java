package org.sanju.kafka.connect.arangodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.sanju.kafka.connect.arangodb.sink.ArangoDBSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class BufferedArangoDBWriter implements Writer{
	
	private static final Logger logger = LoggerFactory.getLogger(BufferedArangoDBWriter.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();
	
	private final int batchSize;
	private final BufferedRecords bufferedRecords = new BufferedRecords();
	
	public BufferedArangoDBWriter(final Map<String, String> config){
	    
	    batchSize = Integer.valueOf(config.get(ArangoDBSinkConfig.BATCH_SIZE));
	    
	}
	
	/**
	 * 
	 * Buffer the Records until the batch size reached.
	 *
	 */
	class BufferedRecords extends ArrayList<SinkRecord>{
	    
        private static final long serialVersionUID = 1L;
        
        void buffer(final SinkRecord r){
	        add(r);
	        if(batchSize <= size()){
	            flush();
	        }
	    }

        void flush() {
            clear();
        }
	}

    @Override
    public void write(final Collection<SinkRecord> recrods) {
       recrods.forEach(r -> bufferedRecords.buffer(r));
       bufferedRecords.flush();
    }
    
}
