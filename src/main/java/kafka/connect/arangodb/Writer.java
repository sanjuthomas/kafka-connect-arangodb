package kafka.connect.arangodb;

import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * 
 * @author Sanju Thomas
 *
 */
public interface Writer {
	
	List<String> write(final Collection<SinkRecord> recrods);    
}						
