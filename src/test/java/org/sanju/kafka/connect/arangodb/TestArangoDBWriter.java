package org.sanju.kafka.connect.arangodb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.sanju.kafka.connect.arangodb.sink.ArangoDBSinkConfig;
import org.sanju.kafka.connect.beans.Account;
import org.sanju.kafka.connect.beans.Client;
import org.sanju.kafka.connect.beans.QuoteRequest;

import com.arangodb.ArangoDB;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class TestArangoDBWriter {
	
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private final Map<String, String> conf = new HashMap<String, String>();
	private Writer writer;
	private ArangoDB arangoDB;
	
	@Before
	public void setup() {
		conf.put(ArangoDBSinkConfig.ARANAGODB_HOST, "127.0.0.1");
		conf.put(ArangoDBSinkConfig.ARANAGODB_PORT, "32769");
		conf.put(ArangoDBSinkConfig.CONNECTION_USER, "kafka-connect-arangodb-user");
		conf.put(ArangoDBSinkConfig.CONNECTION_PASSWORD, "kafka-connect-arangodb-user");
		conf.put(ArangoDBSinkConfig.DATABASE_NAME, "kafka-connect-arangodb");
		conf.put(ArangoDBSinkConfig.COLLECTION_NAME, "trades");
		conf.put(ArangoDBSinkConfig.BATCH_SIZE, "100");
		writer = new ArangoDBWriter(conf);
		arangoDB =new ArangoDB.Builder().host(conf.get(ArangoDBSinkConfig.ARANAGODB_HOST), 
	            Integer.valueOf(conf.get(ArangoDBSinkConfig.ARANAGODB_PORT)))
	            .user(conf.get(ArangoDBSinkConfig.CONNECTION_USER))
	            .password(conf.get(ArangoDBSinkConfig.CONNECTION_PASSWORD)).build();
	}
	
	
	@Test
	public void shouldWrite() {
		
		final List<SinkRecord> documents = new ArrayList<SinkRecord>();
		final Account account = new Account("A1");
		final Client client = new Client("C1", account);
		final QuoteRequest quoteRequest = new QuoteRequest("Q1", "APPL", 100, client, new Date());
		
		documents.add(new SinkRecord("topic", 1, null, null, null, MAPPER.convertValue(quoteRequest, Map.class), 0));
	//	writer.write(documents);

		QuoteRequest document = arangoDB.db(ArangoDBSinkConfig.DATABASE_NAME)
				.collection(ArangoDBSinkConfig.COLLECTION_NAME).getDocument(quoteRequest.getKey(), QuoteRequest.class);
		assertEquals(document.getKey(), "Q1");
		assertEquals(document.getSymbol(), "APPL");
		assertEquals(document.getQuantity(), 100);
		assertEquals(document.getClient().getId(), "C1");
		assertEquals(document.getClient().getAccount().getId(), "A1");
	}
	
}
