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

import java.math.BigDecimal;

/**
 * Defines constants for the KafkaRuleEngine service/program.
 * 
 * 
 * @author uwe.geercken@web.de - 2019-06-04
 *
 */
public class Constants
{
	// properties of the ruleengine properties file
	public static final String PROPERTY_KAFKA_BROKERS								= "kafka.brokers";
	public static final String PROPERTY_KAFKA_GROUP_ID								= "kafka.group.id";
	public static final String PROPERTY_KAFKA_CONSUMER_POLL							= "kafka.consumer.poll";
	public static final String PROPERTY_KAFKA_TOPIC_SOURCE							= "kafka.topic.source";
	public static final String PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT					= "kafka.topic.source.format";
	public static final String PROPERTY_KAFKA_TOPIC_OUTPUT_FORMAT					= "kafka.topic.output.format";
	public static final String PROPERTY_KAFKA_TOPIC_TARGET							= "kafka.topic.target";
	public static final String PROPERTY_KAFKA_TOPIC_DROP_FAILED						= "kafka.topic.dropfailed";
	public static final String PROPERTY_KAFKA_TOPIC_TARGET_FAILED					= "kafka.topic.target.failed";
	public static final String PROPERTY_KAFKA_TOPIC_TARGET_LOGGING					= "kafka.topic.target.logging";
	public static final String PROPERTY_RULEENGINE_FAILED_MODE						= "ruleengine.failed.mode";
	public static final String PROPERTY_RULEENGINE_MINIMUM_FAILED_NUMBER_OF_GROUPS	= "ruleengine.failed.minimum_number_of_groups";
	public static final String PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL			= "ruleengine.check.modified.file.interval";
	public static final String PROPERTY_KAFKA_MESSAGE_FORMAT						= "kafka.topic.source.format";
	public static final String PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS		= "kafka.topic.source.format.csv.fields";
	public static final String PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_SEPARATOR		= "kafka.topic.source.format.csv.value.separator";
	public static final String PROPERTY_KAFKA_TOPIC_EXCLUDE_FIELDS					= "kafka.topic.exclude.fields";
	public static final String PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_FIELDNAME		= "kafka.topic.message.counter.fieldname";
	public static final String PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_INTERVAL= "kafka.topic.message.counter.logging.interval";
	public static final String PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_IDLETIME= "kafka.topic.message.counter.logging.idletime";
	
	// properties for the SchemaRegistry
	public static final String PROPERTY_SCHEMA_REGISTRY_URL							= "schema.registry.url";
	
	//Default values 
	public static final int DEFAULT_PROPERTY_RULEENGINE_FAILED_MODE							= 0;
	public static final long DEFAULT_PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_INTERVAL 	= 10000;
	public static final long DEFAULT_PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_IDLETIME	= 60;
	public static final int DEFAULT_PROPERTY_RULEENGINE_MINIMUM_FAILED_NUMBER_OF_GROUPS		= 0;
	public static final long DEFAULT_PROPERTY_KAFKA_CONSUMER_POLL							= 1000;	
	
	public static final boolean DEFAULT_PRESERVE_RULE_EXECUTION_RESULTS						= true;
	public static final String DEFAULT_KAFKA_TOPIC_SOURCE_FORMAT							= "json";
	public static final String DEFAULT_KAFKA_TOPIC_OUTPUT_FORMAT							= "json";
	public static final long DEFAULT_RULEENGINE_ZIPFILE_CHECK_MODIFIED_INTERVAL				= 600;
	
	
	
	// separator used in the properties file when multiple values are specified
	public static final String PROPERTY_VALUES_SEPARATOR							= ",";
	
	public static final String PROPERTY_KAFKA_BOOTSTRAP_SERVERS						= "bootstrap.servers";
	public static final String PROPERTY_KAFKA_CONSUMER_GROUP_ID						= "group.id";
	
	// ruleengine fields that will be added to the output
	public static final String RULEENGINE_FIELD_GROUP_ID							= "ruleengine_group";
	public static final String RULEENGINE_FIELD_GROUP_FAILED						= "ruleengine_group_failed";
	public static final String RULEENGINE_FIELD_SUBGROUP_ID							= "ruleengine_subgroup";
	public static final String RULEENGINE_FIELD_SUBGROUP_FAILED						= "ruleengine_subgroup_failed";
	public static final String RULEENGINE_FIELD_SUBGROUP_OPERATOR					= "ruleengine_subgroup_operator";
	public static final String RULEENGINE_FIELD_RULES_OPERATOR						= "ruleengine_rules_operator";
	public static final String RULEENGINE_FIELD_RULE_ID								= "ruleengine_rule";
	public static final String RULEENGINE_FIELD_RULE_FAILED							= "ruleengine_rule_failed";
	public static final String RULEENGINE_FIELD_RULE_MESSAGE						= "ruleengine_rule_message";
	
	// standard datetime format for logging
	public static final String DATETIME_FORMAT										= "yyyy-MM-dd HH:mm:ss";

	// default values for RowField/ReferenceField that are additionally created by the ruleengine
	public static String		ROWFIELD_TYPE_STRING_DEFAULT_VALUE					= "";
	public static int			ROWFIELD_TYPE_INTEGER_DEFAULT_VALUE					= 0;
	public static long			ROWFIELD_TYPE_LONG_DEFAULT_VALUE					= 0;
	public static float			ROWFIELD_TYPE_FLOAT_DEFAULT_VALUE					= 0.0f;
	public static double		ROWFIELD_TYPE_DOUBLE_DEFAULT_VALUE					= 0.0d;
	public static BigDecimal	ROWFIELD_TYPE_BIGDECIMAL_DEFAULT_VALUE				= new BigDecimal(0);
	public static boolean		ROWFIELD_TYPE_BOOLEAN_DEFAULT_VALUE					= false;
	public static long	 		ROWFIELD_TYPE_DATE_DEFAULT_VALUE					= 0;
	
	// log level subtypes
	public static final String LOG_LEVEL_SUBTYPE_GENERAL							= "[GENERAL] ";
	public static final String LOG_LEVEL_SUBTYPE_METRICS							= "[METRICS] ";
	public static final String LOG_LEVEL_SUBTYPE_RULEENGINE							= "[RULEENGINE] ";
	public static final String LOG_LEVEL_SUBTYPE_KAFKA								= "[KAFKA] ";
		
	// format of the kafka messages
	public static final String MESSAGE_FORMAT_JSON									= "json";
	public static final String MESSAGE_FORMAT_AVRO									= "avro";
	public static final String MESSAGE_FORMAT_CSV									= "csv";
	
	// ruleengine output mode
    public static final int RULEENGINE_OUTPUT_MODE_ALL_GROUPS						= 0;
    public static final int RULEENGINE_OUTPUT_MODE_PASSED_GROUPS					= 1;
    public static final int RULEENGINE_OUTPUT_MODE_FAILED_GROUPS					= 2;
    
    public static final String VERSION_NUMBER								 		= "0.8.7";
    public static final String LAST_UPDATE									 		= "2019-06-25";
	


}
