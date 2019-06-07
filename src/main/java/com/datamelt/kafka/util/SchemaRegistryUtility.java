/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
