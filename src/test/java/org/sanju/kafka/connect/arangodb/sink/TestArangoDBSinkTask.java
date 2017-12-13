package org.sanju.kafka.connect.arangodb.sink;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.sanju.kafka.connect.arangodb.Writer;
import org.sanju.kafka.connect.beans.Account;
import org.sanju.kafka.connect.beans.Client;
import org.sanju.kafka.connect.beans.QuoteRequest;

import com.fasterxml.jackson.databind.ObjectMapper;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class TestArangoDBSinkTask {
	
	@Tested
	private ArangoDBSinkTask arangoDBSinkTask;
	
	@Injectable
	private Writer writer;
	
	@Injectable
	private SinkTaskContext sinkTaskContext;
	
	private static final ObjectMapper MAPPER = new ObjectMapper();
	
	private final Map<String, String> conf = new HashMap<>();
	
	private final List<SinkRecord> documents = new ArrayList<SinkRecord>();
	
	@Before
    public void setup(){

        conf.put(ArangoDBSinkConfig.BATCH_SIZE, "100");
        conf.put(ArangoDBSinkConfig.RETRY_BACKOFF_MS, "100");
        conf.put(ArangoDBSinkConfig.MAX_RETRIES, "3");
        conf.put(ArangoDBSinkConfig.ARANAGODB_HOST, "localhost");
        conf.put(ArangoDBSinkConfig.ARANAGODB_PORT, "9800");
        conf.put(ArangoDBSinkConfig.CONNECTION_USER, "test");
        conf.put(ArangoDBSinkConfig.CONNECTION_PASSWORD, "test");
    }
	
	
	@Test(expected = ConnectException.class)
	public void shouldPutRecordsIntoSync() {
		
		arangoDBSinkTask.start(conf);
		
		new Expectations() {{
			writer.write(documents);
			times = 4;
			result = new RetriableException("A RetriableException Test Exception!");
		}};
		
		final Account account = new Account("A1");
		final Client client = new Client("C1", account);
		final QuoteRequest quoteRequest = new QuoteRequest("Q1", "APPL", 100, client, new Date());
		documents.add(new SinkRecord("trades", 1, null, null, null,  MAPPER.convertValue(quoteRequest, Map.class), 0));
		try {
	        initDependencies();
			arangoDBSinkTask.put(documents);
		} catch (RetriableException e) {
			assertEquals(RetriableException.class.getName(), e.getClass().getName());
	        initDependencies();
			try {
				arangoDBSinkTask.put(documents);
			} catch (Exception e1) {
				assertEquals(RetriableException.class.getName(), e.getClass().getName());
		        initDependencies();
		        try {
					arangoDBSinkTask.put(documents);
				} catch (Exception e2) {
					assertEquals(RetriableException.class.getName(), e.getClass().getName());
			        initDependencies();
			        arangoDBSinkTask.put(documents);
				}
			}
		}
	}


	private void initDependencies() {
        Deencapsulation.setField(arangoDBSinkTask, writer);
        Deencapsulation.setField(arangoDBSinkTask, sinkTaskContext);
	}

}
