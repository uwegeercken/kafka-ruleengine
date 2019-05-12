package com.datamelt.kafka.util;

import java.util.HashMap;
import java.util.Map;

import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;

public class SchemaRegistryUtility 
{
	private static final long CLASSLOADER_CACHE_SIZE = 10L;
	private static final long CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS = 5000L;
	private static final long SCHEMA_VERSION_CACHE_SIZE = 1000L;
	private static final long SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60 * 1000L;
    
	private Map<String, Object> config = new HashMap<String, Object>();
	private SchemaRegistryClient schemaRegistryClient;
	
	public SchemaRegistryUtility(String registryUrl)
	{
		 config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryUrl);
	     config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), CLASSLOADER_CACHE_SIZE);
	     config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS);
	     config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), SCHEMA_VERSION_CACHE_SIZE);
	     config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS);
	     
	     schemaRegistryClient = new SchemaRegistryClient(config);
	}

	public String getLatestSchema(String topic) throws Exception
	{
		
		SchemaVersionInfo versionInfo = schemaRegistryClient.getLatestSchemaVersionInfo(topic);
		String latestSchema = versionInfo.getSchemaText();
		return latestSchema;
	}
	
	public void closeRegistryClient()
	{
		schemaRegistryClient.close();
	}
}
