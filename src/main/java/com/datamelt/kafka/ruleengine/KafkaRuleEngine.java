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

package com.datamelt.kafka.ruleengine;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema; 
import org.apache.avro.Schema.Parser;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.datamelt.rules.core.ReferenceField;
import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.kafka.util.Constants;
import com.datamelt.kafka.util.SchemaRegistryUtility;
import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;

/**
 * Reads JSON or CSV formatted data from a Kafka topic, runs business rules (logic) on the data and
 * outputs the resulting data to a Kafka target topic.
 * 
 * Optionally the detailed results of the execution of the ruleengine may be ouput to a defined
 * topic for logging purposes. In this case the topic will contain one output message for each
 * input message and rule. E.g. if there are 10 rules in the ruleengine project file, then for any
 * received input message, 10 output messages are generated.
 * 
 * The source topic data is expected to be in Avro, JSON or CSV format. Output will be in JSON or Avro format.
 * This can be configured in the kafka_ruleengine.properties file and by specifying the appropriate value
 * deserializer/serializer in the configuration for the kafka consumer and producer. 
 * 
 * 
 * @author uwe geercken - 2019-05-30
 *
 */
public class KafkaRuleEngine
{
	private static HashSet<String> referenceFieldsAvailableInReferenceFileOnly;
	private static ArrayList<ReferenceField> referenceFieldsAvailableInReferenceFileOnlyList;
	private static HashSet<String> referenceFieldsAvailableInMessage 				= new HashSet<>();
	private static Properties properties 									 		= new Properties();
	private static String propertiesFilename;
	private static String propertiesFilenameConsumer;
	private static String propertiesFilenameProducer;
	private static String ruleEngineZipFilename;
	private static Properties kafkaConsumerProperties						 		= new Properties();
	private static Properties kafkaProducerProperties						 		= new Properties();
	private static boolean outputToFailedTopic								 		= false;
	private static boolean dropFailedMessages							 	 		= false;	

	private static int failedMode										 	 		= Constants.DEFAULT_PROPERTY_RULEENGINE_FAILED_MODE;
	private static long fieldNameToCountOnLoggingInterval				 	 		= Constants.DEFAULT_PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_INTERVAL;
	private static long fieldNameToCountOnLoggingIdleTime				 	 		= Constants.DEFAULT_PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_IDLETIME;
	private static int minimumFailedNumberOfGroups									= Constants.DEFAULT_PROPERTY_RULEENGINE_MINIMUM_FAILED_NUMBER_OF_GROUPS;
	private static long kafkaConsumerPoll									 		= Constants.DEFAULT_PROPERTY_KAFKA_CONSUMER_POLL;
	private static long ruleEngineZipFileCheckModifiedInterval						= Constants.DEFAULT_RULEENGINE_ZIPFILE_CHECK_MODIFIED_INTERVAL;
	private static ArrayList<String> excludedFields;
	private static Map<String, Schema> topicSchemas;
	
	final static Logger logger = Logger.getLogger(KafkaRuleEngine.class);
	
	public static void main(String[] args) throws Exception
	{
		if(args.length!=4)
    	{
    		help();
    	}
		else 
		{
			// properties files
			propertiesFilename = args[0];
			// properties for kafka consumer and producer
			propertiesFilenameConsumer = args[1];
			propertiesFilenameProducer = args[2];
			
			// ruleengine project zip file
			ruleEngineZipFilename = args[3]; 

			logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "Start of KafkaRuleEngine program...");
			logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "Program version: " + getVersionAndLastUpdate());
			logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "JaRE RuleEngine version: " + getRuleEngineVersionAndLastUpdate());
			
			boolean zipFileAccessible = checkFileAccessible(ruleEngineZipFilename);
			boolean propertiesFileAccessible = checkFileAccessible(propertiesFilename);
			boolean propertiesFileConsumerAccessible = checkFileAccessible(propertiesFilenameConsumer);
			boolean propertiesFileProducerAccessible = checkFileAccessible(propertiesFilenameProducer);
			
			if(propertiesFileAccessible && propertiesFileConsumerAccessible && propertiesFileProducerAccessible && zipFileAccessible)
			{
				// load ruleengine properties file
				properties = loadProperties(propertiesFilename);

				// process ruleengine properties into variables.
				processRuleengineProperties();
				
				// check the ruleengine properties file for availability of variables
				boolean propertiesFileOk = checkRuleengineProperties();
				
				// load kafka consumer properties file
				kafkaConsumerProperties = loadProperties(propertiesFilenameConsumer);
				
				// load kafka producer properties file
				kafkaProducerProperties = loadProperties(propertiesFilenameProducer);

				logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "properties for the ruleengine => " + properties.toString());
				logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "properties for kafka consumer => " + kafkaConsumerProperties.toString());
				logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "properties for kafka producer => " + kafkaProducerProperties.toString());

				// if ruleengine properties file is ok: all mandatory properties are defined in the properties file and some other checks
				if(propertiesFileOk)
				{
					// add properties from the kafka ruleengine properties file
					kafkaProducerProperties.put(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS, getProperty(Constants.PROPERTY_KAFKA_BROKERS));
					kafkaConsumerProperties.put(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS, getProperty(Constants.PROPERTY_KAFKA_BROKERS));
					kafkaConsumerProperties.put(Constants.PROPERTY_KAFKA_CONSUMER_GROUP_ID, getProperty(Constants.PROPERTY_KAFKA_GROUP_ID));
		
					String failedTopic = "undefined";
					if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim().equals(""))
					{
						failedTopic = getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED);
					}
					
					String loggingTopic = "undefined";
					if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim().equals(""))
					{
						loggingTopic = getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING);
					}
					
					logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "ruleengine project file: [" + ruleEngineZipFilename  + "]");
					logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "ruleengine project file check interval (seconds): [" + getProperty(Constants.PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL) + "]");
					logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "kafka brokers: [" + getProperty(Constants.PROPERTY_KAFKA_BROKERS) + "]");
					logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "kafka source topic: [" + getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE) + "]");
					logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "kafka target topic: [" + getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET) + "]");
					logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "kafka failed topic: [" + failedTopic + "]");
					logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "kafka detailed logging topic: [" + loggingTopic + "]");
				
					// check if we can get a list of topics from the brokers. if not, then the brokers are probably not available
					boolean consumerCanListTopics = consumerCanListTopics(kafkaConsumerProperties);
					if(consumerCanListTopics)
					{
						// create a RuleEngineConsumerProducer instance and run it
						try
						{
							RuleEngineConsumerProducer ruleEngineConsumerProducer = new RuleEngineConsumerProducer(ruleEngineZipFilename,kafkaConsumerProperties,kafkaProducerProperties);
							
							// mandatory properties - we already checked that they are defined in the properties file
							ruleEngineConsumerProducer.setKafkaTopicSource(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE));
							ruleEngineConsumerProducer.setKafkaTopicTarget(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET));
							ruleEngineConsumerProducer.setKafkaTopicSourceFormat(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT));
							
							
							// optional properties
							ruleEngineConsumerProducer.setKafkaTopicOutputFormat(getProperty(Constants.PROPERTY_KAFKA_TOPIC_OUTPUT_FORMAT));
							ruleEngineConsumerProducer.setKafkaTopicFailed(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED));
							ruleEngineConsumerProducer.setKafkaTopicLogging(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING));
							ruleEngineConsumerProducer.setRuleEngineZipFileCheckModifiedInterval(ruleEngineZipFileCheckModifiedInterval);
							ruleEngineConsumerProducer.setFailedMode(failedMode);
							ruleEngineConsumerProducer.setDropFailedMessages(dropFailedMessages);
							ruleEngineConsumerProducer.setMinimumFailedNumberOfGroups(minimumFailedNumberOfGroups);
							ruleEngineConsumerProducer.setKafkaConsumerPoll(kafkaConsumerPoll);
							ruleEngineConsumerProducer.setOutputToFailedTopic(outputToFailedTopic);
							
							// count the number of messages based on the value of one of the fields in the message
							// useful for counting messages when multiple messages belong together based on e.g. a batch-id
							if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_FIELDNAME)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_FIELDNAME).trim().equals(""))
							{
								ruleEngineConsumerProducer.setFieldNameToCountOn(getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_FIELDNAME));
								ruleEngineConsumerProducer.setFieldNameToCountOnLoggingInterval(fieldNameToCountOnLoggingInterval);
								ruleEngineConsumerProducer.setFieldNameToCountOnLoggingIdleTime(fieldNameToCountOnLoggingIdleTime);
							}
							
							// we do not want to preserve the detailed results of the ruleengine execution
							// if we are not logging the detailed results to a topic
							if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim().equals(""))
							{
								ruleEngineConsumerProducer.setPreserveRuleExecutionResults(false);
							}
							
							if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS).trim().equals(""))
							{
								ruleEngineConsumerProducer.setKafkaTopicSourceFormatCsvFields(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS));
							}
							
							if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_SEPARATOR)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_SEPARATOR).trim().equals(""))
							{
								ruleEngineConsumerProducer.setKafkaTopicSourceFormatCsvSeparator(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_SEPARATOR));
							}

							// if the source format is avro
							if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT).equals(Constants.MESSAGE_FORMAT_AVRO))
							{
								if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_AVRO_SCHEMA_NAME)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_AVRO_SCHEMA_NAME).trim().equals(""))
								{
									ruleEngineConsumerProducer.setAvroSchema(parseAvroSchema(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_AVRO_SCHEMA_NAME)));
									ruleEngineConsumerProducer.setKafkaTopicSourceFormatAvroSchemaName(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_AVRO_SCHEMA_NAME));
								}
							}
							
							// if the output format is avro
							if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_OUTPUT_FORMAT)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_OUTPUT_FORMAT).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_OUTPUT_FORMAT).equals(Constants.MESSAGE_FORMAT_AVRO))
							{
								getRegistrySchemas();
							}

							// run the service which creates consumer and producer(s)
							ruleEngineConsumerProducer.run();
						}
						catch(Exception ex)
						{
							logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error running the RuleEngineConsumerProducer instance");
							System.out.println(ex.getMessage());
						}
					}
					else
					{
						String kafkaBrokers = kafkaConsumerProperties.getProperty(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS);
						logger.error(Constants.LOG_LEVEL_SUBTYPE_KAFKA + "could not retrieve a list of topics from Kafka broker(s): [" + kafkaBrokers + "]");
						logger.error(Constants.LOG_LEVEL_SUBTYPE_KAFKA + "end of program");
					}
				}
				else
				{
					logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "there was an error with the ruleengine properties file: [" + ruleEngineZipFilename + "]");
					logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "end of program");
				}
			}
			else
			{
				if(!zipFileAccessible)
				{
					logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "there was an error accessing the ruleengine project zip file: [" + ruleEngineZipFilename + "]");
				}
				if(!propertiesFileAccessible)
				{
					logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "there was an error accessing the ruleengine properties file: [" + propertiesFilename + "]");
				}
				if(!propertiesFileConsumerAccessible)
				{
					logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "there was an error accessing the kafka consumer properties file: [" + propertiesFilenameConsumer + "]");
				}
				if(!propertiesFileProducerAccessible)
				{
					logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "there was an error accessing the kafka producer properties file: [" + propertiesFilenameProducer + "]");
				}
				
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "end of program");
			}
		}
	}

	private static void help()
	{
		System.out.println("KafkaRuleEngine. Program to process data from an Apache Kafka source topic,");
    	System.out.println("run the business rules from a ruleengine project file against the data and output the");
    	System.out.println("results to an Apache Kafka target topic. Failed rows of data may be output to a separate topic.");
    	System.out.println();
    	System.out.println("Additionally an optional topic for logging may be specified which will contain the detailed");
    	System.out.println("results of the execution of each of the individual rules and the message itself.");
    	System.out.println();
    	System.out.println("The Apache Kafka source topic messages must be in Avro, JSON or CSV format. Output will always be in JSON format");
    	System.out.println();
    	System.out.println("Four files must be specified, defining various properties for the program, Kafka and the Ruleengine project zip file.");
    	System.out.println();
    	System.out.println("KafkaRuleEngine [ruleengine properties file] [kafka consumer properties file] [kafka producer properties file]");
    	System.out.println("where [ruleengine properties file]     : required. path and name of the ruleengine properties file");
    	System.out.println("      [kafka consumer properties file] : required. path and name of the kafka consumer properties file");
    	System.out.println("      [kafka producer properties file] : required. path and name of the kafka producer properties file");
    	System.out.println("      [rule engine project file]       : required. path and name of the rule engine project file");
    	System.out.println();
    	System.out.println("example: KafkaRuleEngine /home/test/kafka_ruleengine.properties /home/test/kafka_consumer.properties /home/test/kafka_producer.properties /home/test/my_project_file.zip");
    	System.out.println();
    	System.out.println("published as open source under the Apache License. read the licence notice.");
    	System.out.println("utilizes the Java Rule Engine - JaRE - to apply business rules and actions to the data.");
    	System.out.println("check https://github.com/uwegeercken for source code, documentation and samples.");
    	System.out.println("all code by uwe geercken, 2017-2019. uwe.geercken@web.de");
    	System.out.println();
	}
	
	private static String getVersionAndLastUpdate()
	{
		return "[" + Constants.VERSION_NUMBER + "] - last update: [" + Constants.LAST_UPDATE + "]";
	}
	
	private static String getRuleEngineVersionAndLastUpdate()
	{
		return "[" + BusinessRulesEngine.getVersion() + "] - last update: [" + BusinessRulesEngine.getLastUpdateDate() + "]";
	}

	/**
	 * Loads the properties from the given file
	 * 
	 * @param propertiesFilename		the path and name of the properties file
	 * @return							Properties object
	 */
	private static Properties loadProperties(String propertiesFilename) 
    {
    	Properties properties = new Properties();
    	logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "loading properties from file: [" + propertiesFilename + "]");
		File propertiesFile = new File(propertiesFilename);
		try(FileInputStream inputStream = new FileInputStream(propertiesFile);)
		{
			properties.load(inputStream);
			inputStream.close();
		}
		catch(Exception ex)
		{
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "properties file could not processed: [" + propertiesFilename + "]");
		}
    	return properties;
    }
	
	private static boolean checkFileAccessible(String filename)
	{
		boolean accessible=false;
		logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "checking if file is accessible: [" + filename + "]");
		File file = new File(filename);
    	if(!file.exists())
    	{
    		logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "file not found: [" + filename + "]");
    	}
    	else if(!file.canRead())
    	{
    		logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "file can not be read: [" + filename + "]");
    	}
    	else if(!file.isFile())
    	{
    		logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "is not a file: [" + filename + "]");
    	}
    	else
    	{
    		accessible=true;
    	}
    	return accessible;
	}
	
	/**
	 * checks if the consumer is able to list topics.
	 * 
	 * An attempt is made to retrieve a list of topics. If this fails
	 * then it is assumed that the broker(s) is (are) not available
	 * 
	 * @param kafkaConsumerProperties	the path and name of the properties file
	 * @return		boolean 			indicator if the consumer can list topics
	 */
	private static boolean consumerCanListTopics(Properties kafkaConsumerProperties)
	{
		try(KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);)
		{
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "retrieving list of topics from kafka broker(s)");
			kafkaConsumer.listTopics();
			System.out.println();
			return true;
		}
		catch(Exception ex)
		{
			return false;
		}
	}
	
	/**
	 * in the ruleengine project file there may be additional fields defined that are used
	 * by the ruleengine - which are not available in the input message. For example fields
	 * that are used by actions to update the data.
	 * 
	 * these fields will be added to the rowfield collection and subsequently also to the output
	 * to the target topic.
	 * 
	 * note: java type BigDecimal is not supported by avro schemas. 
	 * 
	 * these additional fields are initialized to the following default values:
	 *  - String to an empty string
	 *  - Date to null
	 *  - integer to zero
	 *  - long to zero
	 *  - float to zero
	 *  - double to zero
	 *  - boolean to false
	 *  - BigDecimal to zero
	 *  
	 * @param referenceFields	ArrayList of reference fields
	 * @param collection		collection of row fields
	 * @return				    indicator if all fields have been processed without errors	 
	 */
	public static boolean addReferenceFields(ArrayList <ReferenceField>referenceFields, RowFieldCollection collection)
	{
		// ruleEngineProjectFileReferenceFields contains a list of fields, that are defined in the rules project file.
		// these are fields that are created using one or more rulegroup action(s). This list is the same for all data rows.
		// but we check here which fields are additional to the fields of the message (collection).
		//
		// if we have not initialized the list of ruleEngineProjectFileReferenceFields then initialize it and determine which
		// fields have to be added to the data row.
		//
		// this is done on the first message only. subsequent messages will use the already processed list of fields - for better
		// performance.
	
		// indicator if all fields have been processed without errors
		int numberOfErrors = 0;
		
		// only on the first message
		if(referenceFieldsAvailableInReferenceFileOnly==null)
		{
			// initialize the list
			referenceFieldsAvailableInReferenceFileOnly = new HashSet<String>();
			
			// initialize the list which is appended to all of the messages
			referenceFieldsAvailableInReferenceFileOnlyList = new ArrayList<ReferenceField>();
			
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "determining reference fields from project zip file not present in the message");
			
			// loop over all reference fields as defined in the project zip file and check if
			// any of the fields are part of the message
			for(int i=0;i<referenceFields.size();i++)
			{
				ReferenceField referenceField = referenceFields.get(i);
				
				// Avro does not support the java BigDecimal data type
				if(referenceField.getJavaTypeId() == ReferenceField.FIELD_TYPE_ID_BIGDECIMAL)
				{
					logger.warn(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "fields of java type BigDecimal are not supported in avro: [" + referenceField.getName() + "]");
				}
				
				// check if the field already exists in the data row
				boolean existField = collection.existField(referenceField.getName());
				// if not, add the field to the list
				if(!existField)
				{
					referenceFieldsAvailableInReferenceFileOnly.add(referenceField.getName());
					referenceFieldsAvailableInReferenceFileOnlyList.add(referenceField);
				}
				else
				{
					referenceFieldsAvailableInMessage.add(referenceField.getName());
				}
			}
			
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "number of message fields: [" + collection.getNumberOfFields() + "]");
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "number of fields in reference file: [" + referenceFields.size() + "]");
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "number of fields in reference file from message: [" + referenceFieldsAvailableInMessage.size() + "]");
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "number of fields in reference file only: [" + referenceFieldsAvailableInReferenceFileOnly.size() + "]");
		}

		// only if debugging - for performance reasons
		if(logger.isDebugEnabled())
		{
			// we check on each message if the fields required by the ruleengine as determined
			// from the reference fields and on the first message, are present in the current message
			for(String fieldName : referenceFieldsAvailableInMessage)
			{
				if(!collection.existField(fieldName))
				{
					logger.error(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "field missing that was present in the first message that was processed: [" + fieldName + "]");
					numberOfErrors++;
				}
			}
			
			// check if the message - the number of fields - have changed. this could lead to
			// missing fields that the ruleengine requires
			for(String fieldName : referenceFieldsAvailableInReferenceFileOnly)
			{
				if(collection.existField(fieldName))
				{
					logger.error(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "field present that was not present in the first message that was processed: [" + fieldName + "]");
					numberOfErrors++;
				}
			}

		}
		
		// only we we found no errors. debug mode must be enables to check this!
		if(numberOfErrors==0)
		{
			// add the fields that are NOT part of the message as rowfields to the collection
			for(int i=0;i<referenceFieldsAvailableInReferenceFileOnlyList.size();i++)
			{
				ReferenceField referenceField = referenceFieldsAvailableInReferenceFileOnlyList.get(i);
				
				// set the default value according to the data type of the reference field.
				// if an exception occurs, then the data type is invalid. in this case we do not
				// set the value of the row field at all
				try
				{
					Object value = setDefaultValue(referenceField.getJavaTypeId());
					collection.addField(new RowField(referenceField.getName(),value));
				}
				// in this case create a rowfield without a value
				catch(Exception ex)
				{
					logger.warn(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "invalid data type for field: [" + referenceField.getName() + "] - type: [" + referenceField.getJavaTypeId() + "]");
					collection.addField(new RowField(referenceField.getName()));
				}
			}
		}
		return numberOfErrors==0;
	}

	/**
	 * returns an object of the appropriate type according to the type
	 * of the reference field
	 * 
	 * @param fieldType		the data type of the reference field 
	 * @return				the default value for the relevant data type
	 * @throws Exception	exception if the field taype is invalid
	 */	
	private static Object setDefaultValue(long fieldType) throws Exception
	{
		if(fieldType == ReferenceField.FIELD_TYPE_ID_STRING)
		{
			return Constants.ROWFIELD_TYPE_STRING_DEFAULT_VALUE;
		}
		else if(fieldType == ReferenceField.FIELD_TYPE_ID_INTEGER)
		{
			return Constants.ROWFIELD_TYPE_INTEGER_DEFAULT_VALUE;
		}
		else if(fieldType == ReferenceField.FIELD_TYPE_ID_LONG)
		{
			return Constants.ROWFIELD_TYPE_LONG_DEFAULT_VALUE;
		}
		else if(fieldType == ReferenceField.FIELD_TYPE_ID_DOUBLE)
		{
			return Constants.ROWFIELD_TYPE_DOUBLE_DEFAULT_VALUE;
		}
		else if(fieldType == ReferenceField.FIELD_TYPE_ID_FLOAT)
		{
			return Constants.ROWFIELD_TYPE_FLOAT_DEFAULT_VALUE;
		}
		else if(fieldType == ReferenceField.FIELD_TYPE_ID_BOOLEAN)
		{
			return Constants.ROWFIELD_TYPE_BOOLEAN_DEFAULT_VALUE;
		}
		else if(fieldType == ReferenceField.FIELD_TYPE_ID_BIGDECIMAL)
		{
			return Constants.ROWFIELD_TYPE_BIGDECIMAL_DEFAULT_VALUE;
		}
		else if(fieldType == ReferenceField.FIELD_TYPE_ID_DATE)
		{
			return Constants.ROWFIELD_TYPE_DATE_DEFAULT_VALUE;
		}
		else
		{
			// exception will be caught in the calling method
			throw new Exception();
		}
	}
	
	/**
	 * returns a property by specifying the key of the property
	 * 
	 * @param key		the key of a property
	 * @return			the value of the property for the given key
	 */
	private static String getProperty(String key)
	{
		return properties.getProperty(key);
	}
	
	/**
	 * processes some of the properties from the properties file into the correct types
	 * or splits values which contain multiple definitions into an array.
	 * 
	 */
	private static void processRuleengineProperties()
	{
		// get the poll interval of the kafka consumer 
		if(getProperty(Constants.PROPERTY_KAFKA_CONSUMER_POLL)!=null && !getProperty(Constants.PROPERTY_KAFKA_CONSUMER_POLL).equals(""))
		{
			try
			{
				kafkaConsumerPoll = Long.parseLong(getProperty(Constants.PROPERTY_KAFKA_CONSUMER_POLL));
			}
			catch(Exception ex)
			{
				logger.warn(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error converting property to long value [ " + Constants.PROPERTY_KAFKA_CONSUMER_POLL + "] from properties file [" + propertiesFilename + "]");
			}
		}
		
		// get the interval how often to check for a modified ruleengine project zip file 
		if(getProperty(Constants.PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL)!=null && !getProperty(Constants.PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL).equals(""))
		{
			try
			{
				ruleEngineZipFileCheckModifiedInterval = Long.parseLong(getProperty(Constants.PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL));
			}
			catch(Exception ex)
			{
				logger.warn(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error converting property to long value [ " + Constants.PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL + "] from properties file [" + propertiesFilename + "]");
			}
		}
		
		// determine the failed mode. can be "at least one" or "all" rulegroups failed 
		// has to be according the RULEGROUP_STATUS_MODE... of the ruleengine
		if(getProperty(Constants.PROPERTY_RULEENGINE_FAILED_MODE)!=null && !getProperty(Constants.PROPERTY_RULEENGINE_FAILED_MODE).equals(""))
		{
			try
			{
				failedMode = Integer.parseInt(getProperty(Constants.PROPERTY_RULEENGINE_FAILED_MODE));
			}
			catch(Exception ex)
			{
				logger.warn(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error converting property to integer value [ " + Constants.PROPERTY_RULEENGINE_FAILED_MODE + "] from properties file [" + propertiesFilename + "]");
			}
		}
		
		// determine the field name logging interval counter 
		// used for counting messages based on the value of a defined field in the message, e.g. batch-id
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_INTERVAL)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_INTERVAL).equals(""))
		{
			try
			{
				fieldNameToCountOnLoggingInterval = Long.parseLong(getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_INTERVAL));
			}
			catch(Exception ex)
			{
				logger.warn(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error converting property to long value [ " + Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_INTERVAL + "] from properties file [" + propertiesFilename + "]");
			}
		}
		
		// determine the field name logging interval idle time 
		// used for counting messages based on the value of a defined field in the message, e.g. batch-id
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_IDLETIME)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_IDLETIME).equals(""))
		{
			try
			{
				fieldNameToCountOnLoggingIdleTime = Long.parseLong(getProperty(Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_IDLETIME));
			}
			catch(Exception ex)
			{
				logger.warn(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error converting property to long value [ " + Constants.PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_IDLETIME + "] from properties file [" + propertiesFilename + "]");
			}
		}
		
		// determine the number of groups that must have failed so that
		// the data is regarded as failed
		if(failedMode==0)
		{
			if(getProperty(Constants.PROPERTY_RULEENGINE_MINIMUM_FAILED_NUMBER_OF_GROUPS)!=null && !getProperty(Constants.PROPERTY_RULEENGINE_MINIMUM_FAILED_NUMBER_OF_GROUPS).equals(""))
			{
				try
				{
					minimumFailedNumberOfGroups = Integer.parseInt(getProperty(Constants.PROPERTY_RULEENGINE_MINIMUM_FAILED_NUMBER_OF_GROUPS));
				}
				catch(Exception ex)
				{
					logger.warn(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error converting property to integer value [ " + Constants.PROPERTY_RULEENGINE_MINIMUM_FAILED_NUMBER_OF_GROUPS + "] from properties file [" + propertiesFilename + "]");
				}
			}
		}

		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_EXCLUDE_FIELDS)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_EXCLUDE_FIELDS).equals(""))
		{
			try
			{
				String[] fields = getProperty(Constants.PROPERTY_KAFKA_TOPIC_EXCLUDE_FIELDS).split(Constants.PROPERTY_VALUES_SEPARATOR);
				excludedFields = new ArrayList<String>(Arrays.asList(fields));
			}
			catch(Exception ex)
			{
				logger.warn(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error parsing exclude fields [ " + Constants.PROPERTY_KAFKA_TOPIC_EXCLUDE_FIELDS + "] from properties file [" + propertiesFilename + "]");
			}
		}
		
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).equals(""))
		{
			outputToFailedTopic = true;
		}
		
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_DROP_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_DROP_FAILED).equals(""))
		{
			try
			{
				dropFailedMessages = Boolean.parseBoolean(getProperty(Constants.PROPERTY_KAFKA_TOPIC_DROP_FAILED));
			}
			catch(Exception ex)
			{
				logger.warn(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error converting property to boolean value [ " + Constants.PROPERTY_KAFKA_TOPIC_DROP_FAILED + "] from properties file [" + propertiesFilename + "]");
			}
		}
	}
	
	/**
	 * checks some of the defined variables if they are correct. E.g. if the variable exists and that
	 * the different topics do not have the same names.
	 * 
	 * @return		indicator if the variables are correct
	 */
	private static boolean checkRuleengineProperties()
	{
		boolean propertiesOk = true;
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals(""))
		{
			propertiesOk = false;
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + Constants.PROPERTY_KAFKA_TOPIC_SOURCE + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_OUTPUT_FORMAT)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_OUTPUT_FORMAT).trim().equals(""))
		{
			propertiesOk = false;
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + Constants.PROPERTY_KAFKA_TOPIC_OUTPUT_FORMAT + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT).trim().equals(""))
		{
			propertiesOk = false;
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET).trim().equals(""))
		{
			propertiesOk = false;
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + Constants.PROPERTY_KAFKA_TOPIC_TARGET + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_BROKERS)==null || getProperty(Constants.PROPERTY_KAFKA_BROKERS).trim().equals(""))
		{
			propertiesOk = false;
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + Constants.PROPERTY_KAFKA_BROKERS + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET).trim().equals(""))
		{
			if (getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET).trim()))
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "source topic can not be the same as the target topic");
				propertiesOk = false;
			}
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim().equals(""))
		{
			if (getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim()))
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "source topic can not be the same as the logging topic");
				propertiesOk = false;
			}
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim().equals(""))
		{
			if (getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim()))
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "source topic can not be the same as the failed topic");
				propertiesOk = false;
			}
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim().equals(""))
		{
			if (getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim().equals(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim()))
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "failed topic can not be the same as the logging topic");
				propertiesOk = false;
			}
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT).equals(""))
		{
			if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT).equals(Constants.MESSAGE_FORMAT_CSV) && (getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS).trim().equals("")))
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS + " is undefined in properties file [" + propertiesFilename + "]");
				propertiesOk = false;
			}
			/*if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT).equals(Constants.MESSAGE_FORMAT_AVRO) && (getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_AVRO_SCHEMA_NAME)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_AVRO_SCHEMA_NAME).trim().equals("")))
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_AVRO_SCHEMA_NAME + " is undefined in properties file [" + propertiesFilename + "]");
				propertiesOk = false;
			}
			*/
		}
		return propertiesOk;
	}
	
	/**
	 * gets an avro schema
	 * 
	 * @param topic		the name of the topic corresponding to the schema
	 * @return			avro schema
	 */
	public static Schema getSchema(String topic)
	{
		return topicSchemas.get(topic);
	}
	
	/**
	 * connects to the schema registry and retrieves the schema text for the configured
	 * output topics
	 * 
	 * the schema is then parsed into an avro schema and put into a map
	 */
	private static void getRegistrySchemas()
	{
		String registryUrl = kafkaConsumerProperties.getProperty(Constants.PROPERTY_SCHEMA_REGISTRY_URL);
		SchemaRegistryUtility registryUtility = new SchemaRegistryUtility(registryUrl);
		
		topicSchemas = new HashMap<>();
		
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET).trim().equals(""))
		{
			String targetTopic = getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET); 
			try
			{
				String targetTopicSchema = registryUtility.getLatestSchema(targetTopic);
				topicSchemas.put(targetTopic, parseRegistryAvroSchema(targetTopic,targetTopicSchema));
			}
			catch(Exception ex)
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "could not retrieve schema for target topic [" + targetTopic + "] from registry at: [" + registryUrl + "]");
			}
			
		}

		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim().equals(""))
		{
			String targetTopicLogging = getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING); 
			try
			{
				String targetTopicSchema = registryUtility.getLatestSchema(targetTopicLogging);
				topicSchemas.put(targetTopicLogging,  parseRegistryAvroSchema(targetTopicLogging,targetTopicSchema));
			}
			catch(Exception ex)
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "could not retrieve schema for target logging topic [" + targetTopicLogging + "] from registry at: [" + registryUrl + "]");
			}
			
		}
		
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim().equals(""))
		{
			String targetTopicFailed = getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED); 
			try
			{
				String targetTopicSchema = registryUtility.getLatestSchema(targetTopicFailed);
				topicSchemas.put(targetTopicFailed,  parseRegistryAvroSchema(targetTopicFailed,targetTopicSchema));
			}
			catch(Exception ex)
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "could not retrieve schema for target logging topic [" + targetTopicFailed + "] from registry at: [" + registryUrl + "]");
			}
			
		}
		
		registryUtility.closeRegistryClient();
	}
	
	/**
	 * parses the given avro schema file
	 * 
	 * @param schemaFilename	the path and name of the avro schema file
	 * @return					avro schema
	 */
	public static Schema parseAvroSchema(String schemaFilename)
	{
		Schema schema = null;
		boolean schemaFileAccessible = checkFileAccessible(schemaFilename);
		if(schemaFileAccessible)
		{
			try
			{
				logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL +  " parsing avro schema [" + schemaFilename + "]");
				File schemaFile = new File(schemaFilename);
				Parser parser =  new Schema.Parser();
				schema = parser.parse(schemaFile);
			}
			catch(Exception ex)
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL +  " could not parse avro schema [" + schemaFilename + "]");
			}
		}
		return schema;
	}
	
	/**
	 * parses the given avro schema file retrieved from the schema registry
	 * 
	 * it is assumed that the name of the topic in the schema registry is the same as the topic name
	 * 
	 * @param topic				the path and name of the avro schema file
	 * @param schemaText		avro schema text
	 * @return					avro schema
	 */
	private static Schema parseRegistryAvroSchema(String topic, String schemaText)
	{
		Schema schema = null;
		try
		{
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL +  " parsing registry avro schema [" + topic + "]");
			Parser parser =  new Schema.Parser();
			schema = parser.parse(schemaText);
		}
		catch(Exception ex)
		{
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL +  " could not parse registry avro schema [" + topic + "]");
		}
		return schema;
	}
	
	/**
	 * Returns the label used by the ruleengine for each record which
	 * corresponds to the specified recordKey (if defined) or otherwise
	 * the value "record_" plus the counter value.
	 * 
	 * @param recordKey		key of the kafka message
	 * @param counter		the current counter for the number of messages retrieved
	 * @return				the key for the message
	 */
	public static String getLabel(String recordKey, long counter)
	{
		// if we have no key in the message, we use the running number of the counter instead
		if(recordKey!=null)
		{
			return recordKey;
		}
		else
		{
			return "record_" + counter;
		}
	}
	
	/**
	 * A list of fields which should not be transfered to the output topics
	 * 
	 * @return 	arraylist of fields to be excluded from the output
	 */
	public static ArrayList<String> getExcludedFields()
	{
		return excludedFields;
	}
}
