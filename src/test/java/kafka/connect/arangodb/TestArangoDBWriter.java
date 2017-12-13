package kafka.connect.arangodb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoDB;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.connect.arangodb.ArangoDBWriter;
import kafka.connect.arangodb.Writer;
import kafka.connect.arangodb.sink.ArangoDBSinkConfig;
import kafka.connect.beans.Account;
import kafka.connect.beans.Client;
import kafka.connect.beans.QuoteRequest;

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
		conf.put(ArangoDBSinkConfig.ARANAGODB_PORT, "8529");
		conf.put(ArangoDBSinkConfig.CONNECTION_USER, "root");
		conf.put(ArangoDBSinkConfig.CONNECTION_PASSWORD, "");
		conf.put(ArangoDBSinkConfig.DATABASE_NAME, "kafka-connect-arangodb");
		conf.put(ArangoDBSinkConfig.COLLECTION_NAME, "trades");
		conf.put(ArangoDBSinkConfig.BATCH_SIZE, "100");
		writer = new ArangoDBWriter(conf);
		arangoDB =new ArangoDB.Builder().host(conf.get(ArangoDBSinkConfig.ARANAGODB_HOST), 
	            Integer.valueOf(conf.get(ArangoDBSinkConfig.ARANAGODB_PORT)))
	            .user(conf.get(ArangoDBSinkConfig.CONNECTION_USER))
	            .password(conf.get(ArangoDBSinkConfig.CONNECTION_PASSWORD)).build();
	}
	
	@After
	public void tearDown() {
		arangoDB.db("kafka-connect-arangodb").collection("trades").truncate();
	}
	
	
	@Test
	public void shouldWrite() {
		
		final List<SinkRecord> documents = new ArrayList<SinkRecord>();
		final Account account = new Account("A1");
		final Client client = new Client("C1", account);
		final QuoteRequest quoteRequest = new QuoteRequest("Q1", "APPL", 100, client, new Date());
		
		documents.add(new SinkRecord("topic", 1, null, null, null, MAPPER.convertValue(quoteRequest, Map.class), 0));
		final List<String> keys = writer.write(documents);
		
		assertEquals(1, keys.size());

		final Map<?, ?> document = arangoDB.db("kafka-connect-arangodb")
				.collection("trades").getDocument(keys.get(0), Map.class);
		
		assertEquals("APPL", document.get("symbol"));
		assertEquals(100L, document.get("quantity"));
		
	}
	
}
