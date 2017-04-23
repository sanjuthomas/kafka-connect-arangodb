package org.sanju.kafka.connect.arangodb.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class ArangoDBSinkConnector extends SinkConnector{
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBSinkConnector.class);
	public static final String ARANGODB_CONNECTOR_VERSION = "1.0";
	
	private Map<String, String> config;

	@Override
	public ConfigDef config() {
		
		return ArangoDBSinkConfig.CONFIG_DEF;
	}

	@Override
	public void start(final Map<String, String> arg0) {
		config = arg0;
	}

	@Override
	public void stop() {
		logger.info("stop called");
	}

	@Override
	public Class<? extends Task> taskClass() {
		
		return ArangoDBSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(final int taskCunt) {
	
	    final List<Map<String, String>> configs = new ArrayList<>(taskCunt);
	    for (int i = 0; i < taskCunt; ++i) {
	      configs.add(config);
	    }
	    return configs;
	}

	@Override
	public String version() {
		
		return ARANGODB_CONNECTOR_VERSION;
	}
	

}
