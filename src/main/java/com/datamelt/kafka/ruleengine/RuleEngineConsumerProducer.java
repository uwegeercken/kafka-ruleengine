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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.zip.ZipFile;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.datamelt.rules.core.RuleExecutionResult;
import com.datamelt.rules.core.RuleGroup;
import com.datamelt.rules.core.RuleSubGroup;
import com.datamelt.rules.core.XmlRule;
import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.kafka.util.Constants;
import com.datamelt.kafka.util.FormatConverter;
import com.datamelt.util.RowFieldCollection;

public class RuleEngineConsumerProducer implements Runnable 
{
	private Properties kafkaConsumerProperties						 = new Properties();
	private Properties kafkaProducerProperties						 = new Properties();
	private boolean outputToFailedTopic								 = false; 			//default
	private boolean dropFailedMessages								 = false; 			//default
	private int failedMode										 	 = Constants.DEFAULT_PROPERTY_RULEENGINE_FAILED_MODE;
	private int minimumFailedNumberOfGroups							 = Constants.DEFAULT_PROPERTY_RULEENGINE_MINIMUM_FAILED_NUMBER_OF_GROUPS;
	private long kafkaConsumerPoll									 = Constants.DEFAULT_PROPERTY_KAFKA_CONSUMER_POLL;
	private boolean preserveRuleExecutionResults					 = Constants.DEFAULT_PRESERVE_RULE_EXECUTION_RESULTS;
	private String kafkaTopicSourceFormat							 = Constants.DEFAULT_KAFKA_TOPIC_SOURCE_FORMAT;
	private String kafkaTopicOutputFormat							 = Constants.DEFAULT_KAFKA_TOPIC_OUTPUT_FORMAT;
	private String kafkaTopicSourceFormatCsvFields;
	private String kafkaTopicSourceFormatCsvSeparator;
	private String kafkaTopicSourceFormatAvroSchemaName;
	private Schema avroSchema;
	private String kafkaTopicSource;
	private String kafkaTopicTarget;
	private String kafkaTopicLogging;
	private String kafkaTopicFailed;
	private String ruleEngineZipFile;
	private String ruleEngineZipFileWithoutPath;
	private long ruleEngineZipFileCheckModifiedInterval				 = Constants.DEFAULT_RULEENGINE_ZIPFILE_CHECK_MODIFIED_INTERVAL;
	private String fieldNameToCountOn;
	private long fieldNameToCountOnLoggingInterval					 = Constants.DEFAULT_PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_INTERVAL;
	private long fieldNameToCountOnLoggingIdleTime					 = Constants.DEFAULT_PROPERTY_KAFKA_TOPIC_MESSAGE_COUNTER_LOGGING_IDLETIME;
	private boolean fieldNameCounterWrittenToLog					 = false;
	private String counterFieldnameValue							 = "undefined"; 												
	private String counterFieldnameCurrentValue						 = "undefined"; 										
	private long counterFieldname									 = 0;
	
	private WatchService watcher 									 = FileSystems.getDefault().newWatchService();
	private WatchKey key;
	
	private static volatile boolean keepRunning 					 = true;
	
	final static Logger logger = Logger.getLogger(RuleEngineConsumerProducer.class);
	
	public RuleEngineConsumerProducer (String ruleEngineZipFile, Properties kafkaConsumerProperties, Properties kafkaProducerProperties) throws Exception
	{
		this.kafkaConsumerProperties = kafkaConsumerProperties;
		this.kafkaProducerProperties = kafkaProducerProperties;
		this.ruleEngineZipFile = ruleEngineZipFile;
		
		Path p = Paths.get(ruleEngineZipFile);
		Path ruleEngineZipFilePath = p.getParent();
		this.ruleEngineZipFileWithoutPath = p.getFileName().toString();
		
		// register watcher for changed or deleted project zip file
		key = ruleEngineZipFilePath.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY,StandardWatchEventKinds.ENTRY_DELETE);
		logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "registered watcher for changed or deleted project zip file");
		
		logger.debug(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "adding shutdown hook");
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
		    public void run()
		    {
	        	if(counterFieldname>0)
	        	{
	        		logger.info(Constants.LOG_LEVEL_SUBTYPE_METRICS + "source_topic=" + kafkaTopicSource + " target_topic=" + kafkaTopicTarget +" field=" + fieldNameToCountOn + " field_value=" + counterFieldnameValue + " counter=" + counterFieldname);
	        	}

	        	logger.info(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "program shutdown...");

	        	try
		        {
		        	keepRunning = false;
		        }
		        catch(Exception ex)
		        {
		        	ex.printStackTrace();
		        }
		    }
		});
	}
	
	/**
	 * runs the RuleEngineConsumerProducer program. Messages are read from an input topic, processed using the ruleengine
	 * and are output to the target topic(s).
	 * 
	 * There is always a minimum of one input topic and one output topic. there can be up to three output topics:
	 * - output of messages of the input topic after the business logic was applied
	 * - output of those messages that failed the business logic to a different topic
	 * - output of the detailed results of the business logic to a different topic
	 */
	public synchronized void run()
	{
		// used for counting the number of messages/records.
		// if no key is defined in the kafka message, then this value is used
		// as the label for each row during the ruleengine execution 
		long counter = 0;
		
		// used for calculating the elapsedtime which is used for reloading 
		// the ruleengine project zip file if it has changed
		long startTime = System.currentTimeMillis();

		File ruleEngineProjectFile = new File(ruleEngineZipFile);
		
		BusinessRulesEngine ruleEngine = null;
		try
		{
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "initializing ruleengine with project zip file: [" + ruleEngineProjectFile + "]");
			// instantiate the ruleengine with the project zip file
			ruleEngine = new BusinessRulesEngine(new ZipFile(ruleEngineProjectFile));

			logger.debug(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "preserve ruleengine execution results set to: [" + preserveRuleExecutionResults + "]");
			// preserve rule execution results or not
			ruleEngine.setPreserveRuleExcecutionResults(preserveRuleExecutionResults);
		}
		catch(Exception ex)
		{
			logger.error(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "error processing the ruleengine project file: [" + ruleEngineProjectFile + "]");
			logger.error(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "no data will be consumed from the kafka topic until a valid project file was processed");
			// we do not want to start reading data, if the ruleengine produced an exception
			keepRunning=false;
			
		}
		
		// create consumer and producers for the topics
		logger.debug(Constants.LOG_LEVEL_SUBTYPE_KAFKA + "creating KafkaConsumer and KafkaProducer instances");
		try(
				KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
				KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
				KafkaProducer<String, Object> kafkaProducerLogging = new KafkaProducer<>(kafkaProducerProperties);
				KafkaProducer<String, Object> kafkaProducerFailed = new KafkaProducer<>(kafkaProducerProperties)
			)
		{
			
			// subscribe consumer to the given kafka topic
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_KAFKA + "subscribing to Kafka topic [" + kafkaTopicSource +"]");
			kafkaConsumer.subscribe(Arrays.asList(kafkaTopicSource));
			
			// indicator if loading the ruleengine project zip file produced an error
			boolean errorReloadingProjectZipFile = false;

			// when we received the last record and it's offset - used for logging
			long lastRecordTime = 0;
			long lastOffset = 0;
			
			// process while we receive messages 
			while (true && keepRunning) 
	        {
				long currentTime = System.currentTimeMillis();
				long elapsedTime = (currentTime - startTime)/1000;
				long elapsedTimeSinceLastRecord = 0;

				// calculate the seconds since the last record was received
				if(lastRecordTime!=0)
				{
					elapsedTimeSinceLastRecord = (System.currentTimeMillis() - lastRecordTime)/1000;
				}
				ConsumerRecords<String, Object> records = null;
				
				// if the check modified interval has passed, check if the ruleengine project zip
				// file has changed and if so, reload the file
				if(elapsedTime >= ruleEngineZipFileCheckModifiedInterval)
				{
					startTime = currentTime;
					if(errorReloadingProjectZipFile)
					{
						logger.error(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "no data will be consumed from the kafka because of an error loading the project zip file: [" + ruleEngineZipFile + "]");
					}
					logger.debug(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "checking if project zip file has changed: [" + ruleEngineZipFile + "]");
					boolean reload = checkFileChanges();
					if(reload)
					{
						logger.info(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "project zip file has changed - reloading: [" + ruleEngineZipFile + "]");
			        	synchronized(ruleEngine)
			        	{
							try
				        	{
				        		// reload the ruleengine project zip file
								ruleEngine.reloadZipFile(new ZipFile(new File(ruleEngineZipFile)));
								errorReloadingProjectZipFile = false;
								logger.info(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "reloaded ruleengine project file: [" + ruleEngineZipFile + "]");
				        	}
				        	catch(Exception ex)
				        	{
				        		errorReloadingProjectZipFile = true;
				        		logger.error(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "could not process changed ruleengine project file: [" + ruleEngineZipFile + "]");
				        		logger.error(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "no further data will be processed, until a valid ruleengine project zip file is available");
				        	}
			        	}
					}
				}
				
				// only process data if the ruleengine project zip file was correctly processed
				if(!errorReloadingProjectZipFile)
				{
					// poll records from kafka
					Duration timeout = Duration.ofMillis(kafkaConsumerPoll);
					records = kafkaConsumer.poll(timeout);
					
					// loop over the records/messages
					for (ConsumerRecord<String, Object> record : records) 
					{
						// if there was an error in the process, don't continue to process the records
						if(!keepRunning)
						{
							break;
						}
						
						// when we received the record
						lastRecordTime = System.currentTimeMillis();
						lastOffset = record.offset();
						
						// count all messages
						counter++;
						try
						{
							// create a collection of fields from the incoming record
							RowFieldCollection collection = null;
							if(kafkaTopicSourceFormat.equals(Constants.MESSAGE_FORMAT_JSON))
							{
								collection = FormatConverter.convertFromJson(new String(record.value().toString()));
							}
							else if(kafkaTopicSourceFormat.equals(Constants.MESSAGE_FORMAT_CSV))
							{
								collection = FormatConverter.convertFromCsv(new String(record.value().toString()),kafkaTopicSourceFormatCsvFields,kafkaTopicSourceFormatCsvSeparator);
							}
							else if(kafkaTopicSourceFormat.equals(Constants.MESSAGE_FORMAT_AVRO))
							{
								//collection = FormatConverter.convertFromAvroByteArray(avroSchema,(byte[])record.value());
								collection = FormatConverter.convertFromAvroGenericRecord((GenericRecord)record.value());
							}
							
							// add fields that have been additionally defined in the ruleengine project file
							// these are fields that are not present in the message but used by the rule engine
							KafkaRuleEngine.addReferenceFields(ruleEngine.getReferenceFields(), collection);
						
							// label for the ruleengine. used for assigning a unique label to each record
							// this is useful when outputting the detailed results.
							String label = KafkaRuleEngine.getLabel(record.key(), counter);
							
							// run the ruleengine to apply logic and execute actions
							ruleEngine.run(label, collection);

							// if a field to count on is defined increase counter if the field value has not changed
							// or otherwise reset counter
							if(fieldNameToCountOn!=null)
							{
								counterFieldnameCurrentValue = collection.getFieldValue(fieldNameToCountOn).toString();
								increaseCounterFieldname(counterFieldnameCurrentValue);
							}
							
							// write value of the field to count on, the counter and the kafka offset to the log
							if(fieldNameToCountOn!=null && counter % fieldNameToCountOnLoggingInterval == 0)
							{
								logger.info(Constants.LOG_LEVEL_SUBTYPE_METRICS + "source_topic=" + kafkaTopicSource + " target_topic=" + kafkaTopicTarget + " field=" + fieldNameToCountOn + " field_value=" + counterFieldnameValue + " counter=" + counterFieldname + " offset=" + lastOffset);
								fieldNameCounterWrittenToLog = true;
							}
							else
							{
								// indicator that the counter value was not written to the log yet
								fieldNameCounterWrittenToLog = false;
							}
							
							// check if according to the settings the message has a ruleengine 
							// status of failed
							boolean failedMessage = false;
							
							// depending on the selected mode (PROPERTY_RULEENGINE_FAILED_MODE), the ruleengine returns if the message failed
							if(failedMode != 0 && ruleEngine.getRuleGroupsStatus(failedMode))
							{
								failedMessage = true;
							}
							// if failedMode is equal to 0, then the user specifies the minimum number
							// of groups that must have failed to regard the data as failed
							else if(failedMode == 0 && ruleEngine.getRuleGroupsMinimumNumberFailed(minimumFailedNumberOfGroups)) 
							{
								failedMessage = true;
							}
							
							// failed messages
							if(failedMessage)
							{
								// if an output topic for failed messages is defined and we don't drop failed messages
								// send message to failed topic
								if(!dropFailedMessages && outputToFailedTopic)
								{
									// send the message to the target topic for failed messages
									Object outputMessage = getOutputMessage(collection, kafkaTopicFailed);
									sendFailedTargetTopicMessage(kafkaProducerFailed, record.key(), outputMessage);
								}
								// if output topic for failed messages is undefined and we don't drop failed messages
								// send message to the target topic
								else if(!dropFailedMessages && !outputToFailedTopic)
								{
									// send the message to the target topic
									Object outputMessage = getOutputMessage(collection, kafkaTopicTarget);
									sendTargetTopicMessage(kafkaProducer, record.key(), outputMessage);
								}
							}
							// passed messages
							else
							{
								// all passed messages go to the target topic
								Object outputMessage = getOutputMessage(collection, kafkaTopicTarget);
								sendTargetTopicMessage(kafkaProducer, record.key(), outputMessage);
							}
							
							// send the rule execution details as message(s) to the logging topic, if one is defined.
							// if no logging topic is defined, then no execution details will be generated 
							// (when ruleEngine.setPreserveRuleExcecutionResults() is set to "false")
							if(ruleEngine.getPreserveRuleExcecutionResults())
							{
								Object outputMessage = getOutputMessage(collection, kafkaTopicLogging);
								sendLoggingTargetTopicMessage(kafkaProducerLogging, record.key(), outputMessage, ruleEngine.getGroups());
							}
							
							// clear the collection of details/results otherwise results accumulate
							// this also clears the counters of passed and failed groups and others
							ruleEngine.getRuleExecutionCollection().clear();

						}
						// if we have a parsing problem with the JSON message, we log this and continue processing
						catch(JSONException jex)
						{
							logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error parsing message: key = [" + record.key() + "], value = [" + record.value() + "]");
							logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + jex.getMessage());
						}
						// if we have a problem with SSL we log this and STOP processing
						catch (javax.net.ssl.SSLProtocolException sslex)
						{
							logger.error(Constants.LOG_LEVEL_SUBTYPE_KAFKA + "server refused certificate or other SSL protocol exception");
							logger.error(Constants.LOG_LEVEL_SUBTYPE_KAFKA + sslex.getMessage());
							logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "end of programe");
							keepRunning=false;
						}
						// if we have any other exception we log this and STOP processing
						catch(Exception ex)
						{
							logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + ex.getMessage());
							logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "end of program");
							keepRunning=false;
						}
					}
					
					// write field counter to log (once), if time passed and no record was received
					if(elapsedTimeSinceLastRecord > fieldNameToCountOnLoggingIdleTime && fieldNameCounterWrittenToLog==false)
					{
						logger.info(Constants.LOG_LEVEL_SUBTYPE_METRICS + "source_topic=" + kafkaTopicSource + " target_topic=" + kafkaTopicTarget + " field=" + fieldNameToCountOn + " field_value=" + counterFieldnameValue + " counter=" + counterFieldname + " offset=" + lastOffset);
						fieldNameCounterWrittenToLog = true;					
					}
				}
			}
		}
	}

	/**
	 * returns an object/the message to be sent to an output kafka topic.
	 * 
	 * the format is dependent on the configuration and can be json
	 * or avro format
	 * 
	 * @param collection	a rowfield collection
	 * @param topic			the name of the topic to write to
	 * @return				object representing a message to be sent to kafka
	 */
	private Object getOutputMessage(RowFieldCollection collection, String topic)
	{
		Object outputMessage = null;
		// if output is to json format we convert the rowfield collection to json
		if(kafkaTopicOutputFormat.equals(Constants.MESSAGE_FORMAT_JSON))
		{
			// format the values from the rowfield collection as json for output
			outputMessage = FormatConverter.convertToJson(collection, KafkaRuleEngine.getExcludedFields());
		}
		// if output is to avro format we convert the rowfield collection to GenericRecord
		else if(kafkaTopicOutputFormat.equals(Constants.MESSAGE_FORMAT_AVRO))
		{
			// get the schema corresponding to the topic
			Schema schema = KafkaRuleEngine.getSchema(topic);
			// format the values from the rowfield collection as json for output
			outputMessage = FormatConverter.convertToAvro(collection, KafkaRuleEngine.getExcludedFields(),schema);
		}
		return outputMessage;
	}

	/**
	 * method sends a message to the relevant kafka target topic.
	 * 
	 * the message will contain the original fields from the source topic, plus the refercence fields 
	 * that are defined in the ruleengine project file that are not already defined in the message itself.
	 * 
	 * For example if the project file contains a reference field "country" which is set by an action
	 * in one of the rulegroups and that field is not already existing in the source topic message, then this
	 * field will be added to the message of the target topic.
	 * 
	 * @param kafkaProducer		the kafka producer to use to send the message
	 * @param recordKey			the key of the kafka message
	 * @param message			the message as received from the source topic
	 */
	private void sendTargetTopicMessage(KafkaProducer<String, Object> kafkaProducer, String recordKey, Object message)
	{
		// send the resulting data to the target topic
		if(message instanceof JSONObject)
		{
			kafkaProducer.send(new ProducerRecord<String, Object>(kafkaTopicTarget,recordKey, message.toString()));
		}
		else
		{
			try
			{
				kafkaProducer.send(new ProducerRecord<String, Object>(kafkaTopicTarget,recordKey, message));
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}

	/**
	 * method sends a message for those records that failed the ruleengine logic to the relevant kafka target topic.
	 * 
	 * the message will contain the original fields from the source topic, plus the refercence fields 
	 * that are defined in the ruleengine project file that are not already defined in the message itself.
	 * 
	 * For example if the project file contains a reference field "country" which is set by an action
	 * in one of the rulegroups and that field is not already existing in the source topic message, then this
	 * field will be added to the message of the target topic.
	 * 
	 * @param kafkaProducerFailed		the kafka producer to use to send the message
	 * @param recordKey					the key of the kafka message
	 * @param message					the message as received from the source topic
	 */
	private void sendFailedTargetTopicMessage(KafkaProducer<String, Object> kafkaProducerFailed, String recordKey, Object message)
	{
		// send the resulting data to the target topic
		if(message instanceof JSONObject)
		{
			kafkaProducerFailed.send(new ProducerRecord<String, Object>(kafkaTopicFailed,recordKey, message.toString()));
		}
		else
		{
			kafkaProducerFailed.send(new ProducerRecord<String, Object>(kafkaTopicFailed,recordKey, message));
		}
	}

	/**
	 * method adds the relevant results from the ruleengine execution to the JSON formatted object (message) and
	 * submits the message to the kafka topic for logging
	 * 
	 * for each input message from the source topic and for each rule defined in the ruleengine project file
	 * one message is created. So for each incoming message, if there are 10 rules defined, then this method generates 10 messages
	 * - one for each rule. so this potentially creates a lot of data.
	 * 
	 * there are several fields added to the output message which are details of the execution of the ruleengine and rules.
	 * E.g. if the group, subgroup or rule failed, the rule message and the logical operators of the subgroups and rules.
	 * 
	 * @param kafkaProducer		producer used for the logging topic
	 * @param recordKey			key of the kafka source message
	 * @param message			the kafka source message
	 * @param ruleEngine		reference to the ruleengine instance
	 */
	private void sendLoggingTargetTopicMessage(KafkaProducer<String, Object> kafkaProducerLogging, String recordKey, Object message, ArrayList<RuleGroup> ruleGroups)
	{
		// loop over all rule groups
		for(int f=0;f<ruleGroups.size();f++)
        {
        	RuleGroup group = ruleGroups.get(f);
        	// loop over all subgroups
    		for(int g=0;g<group.getSubGroups().size();g++)
            {
        		RuleSubGroup subgroup = group.getSubGroups().get(g);
        		// get the ruleengine execution results of the subgroup
        		ArrayList <RuleExecutionResult> results = subgroup.getExecutionCollection().getResults();
        		// loop over all results of each subgroup
        		for (int h= 0;h< results.size();h++)
                {
        			RuleExecutionResult result = results.get(h);
        			XmlRule rule = result.getRule();
        			
    				String groupId  = group.getId();
    				long groupFailed = (long)group.getFailed();
    				String subgroupId = subgroup.getId();
    				long subgroupFailed = (long)subgroup.getFailed();
    				String subGroupOperator = subgroup.getLogicalOperatorSubGroupAsString();
    				String rulesOperator = subgroup.getLogicalOperatorRulesAsString();
    				String ruleId = rule.getId();
    				long ruleFailed = (long)rule.getFailed();
    				String ruleMessage = result.getMessage();
    				
    				// check if json or avro
    				if(message instanceof JSONObject)
    				{
    					JSONObject jsonMessage = (JSONObject) message;
    				
    					// add ruleengine fields to the message
    					jsonMessage.put(Constants.RULEENGINE_FIELD_GROUP_ID, groupId);
    					jsonMessage.put(Constants.RULEENGINE_FIELD_GROUP_FAILED, groupFailed);
    					jsonMessage.put(Constants.RULEENGINE_FIELD_SUBGROUP_ID, subgroupId);
    					jsonMessage.put(Constants.RULEENGINE_FIELD_SUBGROUP_FAILED, subgroupFailed);
    					jsonMessage.put(Constants.RULEENGINE_FIELD_SUBGROUP_OPERATOR, subGroupOperator);
    					jsonMessage.put(Constants.RULEENGINE_FIELD_RULES_OPERATOR, rulesOperator);
    					jsonMessage.put(Constants.RULEENGINE_FIELD_RULE_ID, ruleId);
    					jsonMessage.put(Constants.RULEENGINE_FIELD_RULE_FAILED, ruleFailed);
    					jsonMessage.put(Constants.RULEENGINE_FIELD_RULE_MESSAGE, ruleMessage);
    					
    					kafkaProducerLogging.send(new ProducerRecord<String, Object>(kafkaTopicLogging,recordKey, jsonMessage.toString()));
    				}
    				else if(message instanceof GenericRecord)
    				{
    					GenericRecord avroMessage = (GenericRecord) message;
    					
    					// add ruleengine fields to the message
    					avroMessage.put(Constants.RULEENGINE_FIELD_GROUP_ID, groupId);
    					avroMessage.put(Constants.RULEENGINE_FIELD_GROUP_FAILED, groupFailed);
    					avroMessage.put(Constants.RULEENGINE_FIELD_SUBGROUP_ID, subgroupId);
    					avroMessage.put(Constants.RULEENGINE_FIELD_SUBGROUP_FAILED, subgroupFailed);
    					avroMessage.put(Constants.RULEENGINE_FIELD_SUBGROUP_OPERATOR, subGroupOperator);
    					avroMessage.put(Constants.RULEENGINE_FIELD_RULES_OPERATOR, rulesOperator);
    					avroMessage.put(Constants.RULEENGINE_FIELD_RULE_ID, ruleId);
    					avroMessage.put(Constants.RULEENGINE_FIELD_RULE_FAILED, ruleFailed);
    					avroMessage.put(Constants.RULEENGINE_FIELD_RULE_MESSAGE, ruleMessage);
    					
    					kafkaProducerLogging.send(new ProducerRecord<String, Object>(kafkaTopicLogging,recordKey, avroMessage));
    				}
                }
            }
        }
	}

	/**
	 * tracking the number of messages based on the value of a field in the data row.
	 * if the value of the field to count on changes then the counter is reset and counting
	 * starts from zero. If it does not change compared to the previous message then the
	 * counter is increased by one.
	 * 
	 * The assumption is, that the messages that belong together and arrive one after the other,
	 * all have the same value for one specific field (like e.g. a batch-id or a run-id)
	 * 
	 * helpful for logging number of messages processed, specifically when messages arrive in batches
	 * and one wants to know, how many messages have been processed for that specific batch.
	 * 
	 * periodically the value of the counter is written to the log and can be processed from there.
	 * 
	 * 
	 * @param counterFieldnameCurrentValue		the value of the field that is used for counting messages
	 */
	private void increaseCounterFieldname(String counterFieldnameCurrentValue)
	{
		if(counterFieldnameCurrentValue.equals(counterFieldnameValue))
		{
			counterFieldname++;
		}
		else
		{
			logger.debug(Constants.LOG_LEVEL_SUBTYPE_KAFKA + "value for field: [" + fieldNameToCountOn + "] changed from [" + counterFieldnameValue +"] to [" + counterFieldnameCurrentValue +"]");
			resetCounterFieldname();
			counterFieldnameValue = counterFieldnameCurrentValue;
			counterFieldname++;
		}
	}
	
	/**
	 * reset the counter based on a certain field value to zero 
	 */
	private void resetCounterFieldname()
	{
		counterFieldname = 0;
	}
	
	public boolean isOutputToFailedTopic()
	{
		return outputToFailedTopic;
	}

	public void setOutputToFailedTopic(boolean outputToFailedTopic)
	{
		this.outputToFailedTopic = outputToFailedTopic;
	}

	public int getFailedMode()
	{
		return failedMode;
	}

	public void setFailedMode(int failedMode)
	{
		this.failedMode = failedMode;
	}

	public int getMinimumFailedNumberOfGroups()
	{
		return minimumFailedNumberOfGroups;
	}

	public void setMinimumFailedNumberOfGroups(int minimumFailedNumberOfGroups)
	{
		this.minimumFailedNumberOfGroups = minimumFailedNumberOfGroups;
	}

	public long getKafkaConsumerPoll()
	{
		return kafkaConsumerPoll;
	}

	public void setKafkaConsumerPoll(long kafkaConsumerPoll)
	{
		this.kafkaConsumerPoll = kafkaConsumerPoll;
	}

	public boolean isPreserveRuleExecutionResults()
	{
		return preserveRuleExecutionResults;
	}

	public void setPreserveRuleExecutionResults(boolean preserveRuleExecutionResults)
	{
		this.preserveRuleExecutionResults = preserveRuleExecutionResults;
	}

	public String getKafkaTopicSource()
	{
		return kafkaTopicSource;
	}

	public void setKafkaTopicSource(String kafkaTopicSource)
	{
		this.kafkaTopicSource = kafkaTopicSource;
	}
	
	public String getKafkaTopicTarget()
	{
		return kafkaTopicTarget;
	}

	public void setKafkaTopicTarget(String kafkaTopicTarget)
	{
		this.kafkaTopicTarget = kafkaTopicTarget;
	}

	public String getKafkaTopicLogging()
	{
		return kafkaTopicLogging;
	}

	public void setKafkaTopicLogging(String kafkaTopicLogging)
	{
		this.kafkaTopicLogging = kafkaTopicLogging;
	}

	public String getKafkaTopicFailed()
	{
		return kafkaTopicFailed;
	}

	public void setKafkaTopicFailed(String kafkaTopicFailed)
	{
		this.kafkaTopicFailed = kafkaTopicFailed;
	}

	public String getKafkaTopicSourceFormat()
	{
		return kafkaTopicSourceFormat;
	}

	public void setKafkaTopicSourceFormat(String kafkaTopicSourceFormat)
	{
		this.kafkaTopicSourceFormat = kafkaTopicSourceFormat;
	}

	public String getKafkaTopicSourceFormatCsvFields()
	{
		return kafkaTopicSourceFormatCsvFields;
	}

	public void setKafkaTopicSourceFormatCsvFields(String kafkaTopicSourceFormatCsvFields)
	{
		this.kafkaTopicSourceFormatCsvFields = kafkaTopicSourceFormatCsvFields;
	}

	public String getKafkaTopicSourceFormatCsvSeparator()
	{
		return kafkaTopicSourceFormatCsvSeparator;
	}

	public void setKafkaTopicSourceFormatCsvSeparator(String kafkaTopicSourceFormatCsvSeparator)
	{
		this.kafkaTopicSourceFormatCsvSeparator = kafkaTopicSourceFormatCsvSeparator;
	}

	/**
	 * method checks if the ruleengine project file has been changed or was deleted
	 * using WatchService.
	 * 
	 * @return	indicator if file changed
	 */
	
	private synchronized boolean checkFileChanges()
	{
		boolean fileChanged = false;
		
		// loop over watch events for the given key
		for (WatchEvent<?> event: key.pollEvents())
		{
	        WatchEvent.Kind<?> kind = event.kind();

	        // ignore overflow events
	        if (kind == StandardWatchEventKinds.OVERFLOW) 
	        {
	            continue;
	        }

	        @SuppressWarnings("unchecked")
			WatchEvent<Path> ev = (WatchEvent<Path>)event;

	        // get the filename of the event
	        Path filename = ev.context();
	        String eventFilename = filename.getFileName().toString();

	        if(eventFilename !=null && eventFilename.equals(ruleEngineZipFileWithoutPath))
	        {
		        if(event.kind().equals(StandardWatchEventKinds.ENTRY_DELETE))
		        {
		        	logger.warn(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "the ruleengine project zip file: [" + filename.getFileName() + "] has been deleted");	        
		        }
		        else
		        {
		        	logger.info(Constants.LOG_LEVEL_SUBTYPE_RULEENGINE + "detected changed ruleengine project file: [" + filename.getFileName() + "]");
		        	fileChanged = true;
		        }
	        }
	    }

	    // Reset the key -- this step is critical if you want to receive further watch events.
		// If the key is no longer valid, the directory is inaccessible so exit the loop.
	    try
	    {
	    	key.reset();
	    }
	    catch(Exception ex)
	    {
	    	logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error resetting the watch file key on folder: [" + ruleEngineZipFileWithoutPath + "]");
	    }
	    return fileChanged;
	}

	public long getRuleEngineZipFileCheckModifiedInterval()
	{
		return ruleEngineZipFileCheckModifiedInterval;
	}

	public void setRuleEngineZipFileCheckModifiedInterval(long ruleEngineZipFileCheckModifiedInterval)
	{
		this.ruleEngineZipFileCheckModifiedInterval = ruleEngineZipFileCheckModifiedInterval;
	}

	public boolean getDropFailedMessages() 
	{
		return dropFailedMessages;
	}

	public void setDropFailedMessages(boolean dropFailedMessages)
	{
		this.dropFailedMessages = dropFailedMessages;
	}

	public String getFieldNameToCountOn() 
	{
		return fieldNameToCountOn;
	}

	public void setFieldNameToCountOn(String fieldNameToCountOn) 
	{
		this.fieldNameToCountOn = fieldNameToCountOn;
	}
	
	public long getFieldNameToCountOnLoggingInterval() 
	{
		return fieldNameToCountOnLoggingInterval;
	}

	public void setFieldNameToCountOnLoggingInterval(long fieldNameToCountOnLoggingInterval) 
	{
		this.fieldNameToCountOnLoggingInterval = fieldNameToCountOnLoggingInterval;
	}
	
	public long getFieldNameToCountOnLoggingIdleTime() 
	{
		return fieldNameToCountOnLoggingIdleTime;
	}

	public void setFieldNameToCountOnLoggingIdleTime(long fieldNameToCountOnLoggingIdleTime) 
	{
		this.fieldNameToCountOnLoggingIdleTime = fieldNameToCountOnLoggingIdleTime;
	}

	public String getKafkaTopicSourceFormatAvroSchemaName()
	{
		return kafkaTopicSourceFormatAvroSchemaName;
	}

	public void setKafkaTopicSourceFormatAvroSchemaName(String kafkaTopicSourceFormatAvroSchemaName) 
	{
		this.kafkaTopicSourceFormatAvroSchemaName = kafkaTopicSourceFormatAvroSchemaName;
	}

	public Schema getAvroSchema() 
	{
		return avroSchema;
	}

	public void setAvroSchema(Schema avroSchema) 
	{
		this.avroSchema = avroSchema;
	}

	public String getKafkaTopicOutputFormat() 
	{
		return kafkaTopicOutputFormat;
	}

	public void setKafkaTopicOutputFormat(String kafkaTopicOutputFormat) 
	{
		this.kafkaTopicOutputFormat = kafkaTopicOutputFormat;
	}
}
