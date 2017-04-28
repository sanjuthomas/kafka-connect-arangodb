package org.sanju.kafka.connect.arangodb.sink;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class ArangoDBSinkConfig extends AbstractConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(ArangoDBSinkConfig.class);
	
	public static final String ARANAGODB_HOST = "arangodb.host";
	private static final String ARANAGODB_HOST_DOC = "arangodb host ip";
	
	public static final String ARANAGODB_PORT = "arangodb.port";
    private static final String ARANAGODB_PORT_DOC = "arangodb port";
    
	public static final String CONNECTION_USER = "arangodb.user";
	private static final String CONNECTION_USER_DOC = "arangodb connection user.";

	public static final String CONNECTION_PASSWORD = "arangodb.password";
	private static final String CONNECTION_PASSWORD_DOC = "arangodb connection password";
	
	public static final String DATABASE_NAME = "arangodb.database.name";
	private static final String DATABASE_NAME_DOC = "arangodb database name";
	
	public static final String COLLECTION_NAME = "arangodb.collection.name";
	private static final String  COLLECTION_NAME_DOC = "arangodb collection name";

	public static final String BATCH_SIZE = "arangodb.batch.size";
	private static final String BATCH_SIZE_DOC = "arangodb batch size";
	
	public static final String MAX_RETRIES = "arangodb.max.retries";
	private static final String MAX_RETRIES_DOC =  "The maximum number of times to retry on errors/exception before failing the task.";
	
	public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 10000;
	private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error/exception before a retry attempt is made.";
	
	public static ConfigDef CONFIG_DEF = new ConfigDef()
			.define(ARANAGODB_HOST, Type.STRING, Importance.HIGH, ARANAGODB_HOST_DOC)
			.define(ARANAGODB_PORT, Type.INT, Importance.HIGH, ARANAGODB_PORT_DOC)
			.define(CONNECTION_USER, Type.STRING, Importance.HIGH, CONNECTION_USER_DOC)
			.define(CONNECTION_PASSWORD, Type.STRING, Importance.LOW, CONNECTION_PASSWORD_DOC)
			.define(BATCH_SIZE, Type.INT, Importance.MEDIUM, BATCH_SIZE_DOC)
			.define(MAX_RETRIES, Type.INT, Importance.MEDIUM, MAX_RETRIES_DOC)
			.define(DATABASE_NAME, Type.STRING, Importance.MEDIUM, DATABASE_NAME_DOC)
			.define(COLLECTION_NAME, Type.STRING, Importance.MEDIUM, COLLECTION_NAME_DOC)
			.define(RETRY_BACKOFF_MS, Type.INT, RETRY_BACKOFF_MS_DEFAULT, Importance.MEDIUM, RETRY_BACKOFF_MS_DOC);

	public ArangoDBSinkConfig(final Map<?, ?> originals) {
		
		super(CONFIG_DEF, originals, false);
		logger.info("Original Configs {}", originals);
	}

}
